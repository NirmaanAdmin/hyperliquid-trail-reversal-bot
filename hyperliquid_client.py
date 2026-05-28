"""
hyperliquid_client.py
─────────────────────
Thin adapter around the official `hyperliquid-python-sdk` (Exchange + Info)
that exposes the small surface server2.py needs, normalised so the rest of
the bot ("the brain": profit-lock, daily cap, baseline, SL lockout, target)
never has to know it's talking to Hyperliquid.

Key differences from the old CoinDCX client this replaces:
  • No API-key/secret + HMAC. Hyperliquid signs L1 actions with an Ethereum
    key (EIP-712), handled entirely by the SDK. You authorise an *API wallet*
    (a.k.a. agent wallet) at https://app.hyperliquid.xyz/API. The agent key
    can place/cancel orders but CANNOT withdraw funds — so a leak is bounded.
        - HL_SECRET_KEY      = the API/agent wallet PRIVATE key (0x...)
        - HL_ACCOUNT_ADDRESS = your MAIN wallet PUBLIC address (0x...)
  • USDC-margined perps. Sizes are in coin units (e.g. 0.01 BTC); prices in
    USD. Symbols are bare coin names ("BTC", "ETH"), not "B-BTC_USDT".
  • Account state (positions, equity, unrealized PnL, margin used) comes from
    one `user_state` call — no per-symbol mark-price polling needed.

This module performs NO network I/O at import time. The SDK objects are built
lazily on first use so the process boots even in PAPER_MODE with no key set.
"""

import math
import time
import logging
import threading

import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

log = logging.getLogger("bot.hyperliquid")


class HyperliquidFutures:
    def __init__(self, account_address, secret_key, network="mainnet",
                 cross_margin=True, slippage=0.01, state_ttl=2.0):
        self.account_address = (account_address or "").strip()
        self.secret_key = (secret_key or "").strip()
        self.network = (network or "mainnet").strip().lower()
        self.cross_margin = bool(cross_margin)
        self.slippage = float(slippage)
        self.state_ttl = float(state_ttl)

        self._lock = threading.Lock()
        self._info = None
        self._exchange = None

        # meta caches
        self._meta_loaded = False
        self._sz_dec = {}     # coin -> szDecimals (int)
        self._tick = {}       # coin -> tick size (float)
        self._max_lev = {}    # coin -> maxLeverage (int)

        # user_state cache (live mode only)
        self._state = None
        self._state_ts = 0.0

    # ─── URLs / lazy SDK construction ──────────────────────────────────
    def _base_url(self):
        return (constants.TESTNET_API_URL if self.network in ("testnet", "test")
                else constants.MAINNET_API_URL)

    def ready(self):
        """True if a private key is configured (i.e. live trading possible)."""
        return bool(self.secret_key)

    def info(self):
        """Lazily build the read-only Info client (no key required)."""
        if self._info is None:
            with self._lock:
                if self._info is None:
                    self._info = Info(self._base_url(), skip_ws=True)
                    log.info(f"🌐 Hyperliquid Info ready ({self.network})")
        return self._info

    def exchange(self):
        """Lazily build the signing Exchange client (requires HL_SECRET_KEY)."""
        if self._exchange is None:
            if not self.secret_key:
                raise RuntimeError("HL_SECRET_KEY not set — cannot place live orders")
            if not self.account_address:
                raise RuntimeError("HL_ACCOUNT_ADDRESS (main wallet) not set")
            with self._lock:
                if self._exchange is None:
                    wallet = eth_account.Account.from_key(self.secret_key)
                    self._exchange = Exchange(
                        wallet, self._base_url(),
                        account_address=self.account_address,
                    )
                    log.info(f"🔑 Hyperliquid Exchange ready — agent={wallet.address} "
                             f"acting for main={self.account_address} ({self.network})")
        return self._exchange

    # ─── Meta (szDecimals / tick sizes) ────────────────────────────────
    def load_meta(self):
        try:
            meta = self.info().meta()
            universe = meta.get("universe", []) if isinstance(meta, dict) else []
            for a in universe:
                name = a.get("name")
                if not name:
                    continue
                sd = a.get("szDecimals")
                if sd is not None:
                    self._sz_dec[name] = int(sd)
                if a.get("maxLeverage") is not None:
                    self._max_lev[name] = int(a["maxLeverage"])
                tick = a.get("tickSz") or a.get("tickSize")
                if tick is not None:
                    try:
                        self._tick[name] = float(tick)
                    except (TypeError, ValueError):
                        pass
            self._meta_loaded = True
            log.info(f"📊 Loaded meta for {len(self._sz_dec)} perp coins")
        except Exception as e:
            log.error(f"❌ Failed to load Hyperliquid meta: {e}")

    def _ensure_meta(self):
        if not self._meta_loaded:
            self.load_meta()

    def sz_decimals(self, coin):
        self._ensure_meta()
        return self._sz_dec.get(coin, 4)  # 4 is a safe default for most perps

    def max_leverage(self, coin):
        self._ensure_meta()
        return self._max_lev.get(coin)

    def round_sz(self, coin, sz):
        """Floor a size to the coin's szDecimals (never round up past budget)."""
        dec = self.sz_decimals(coin)
        if dec <= 0:
            return float(math.floor(sz))
        factor = 10 ** dec
        return math.floor(float(sz) * factor) / factor

    def round_px(self, coin, px):
        """Round a price to the coin's tick (used only for native trigger SL)."""
        self._ensure_meta()
        tick = self._tick.get(coin)
        if not tick or tick <= 0:
            return float(px)
        decimals = max(0, -math.floor(math.log10(tick)))
        return round(round(float(px) / tick) * tick, decimals)

    # ─── Market data ───────────────────────────────────────────────────
    def all_mids(self):
        """{coin: price(float)} mid-prices for every actively traded perp.
        Public endpoint — works without a private key (used by PAPER_MODE)."""
        try:
            raw = self.info().all_mids()
            out = {}
            if isinstance(raw, dict):
                for k, v in raw.items():
                    try:
                        out[k] = float(v)
                    except (TypeError, ValueError):
                        continue
            return out
        except Exception as e:
            log.warning(f"all_mids failed: {e}")
            return {}

    def mid(self, coin):
        return self.all_mids().get(coin)

    # ─── Account state (live mode) ─────────────────────────────────────
    def clearinghouse(self, force=False):
        """Cached user_state (clearinghouseState) for the main account."""
        now = time.time()
        if (not force) and self._state is not None and (now - self._state_ts) < self.state_ttl:
            return self._state
        if not self.account_address:
            return None
        try:
            st = self.info().user_state(self.account_address)
            self._state = st
            self._state_ts = now
            return st
        except Exception as e:
            log.warning(f"user_state failed: {e}")
            return None

    def account_value(self):
        """Total account equity in USDC (includes unrealized PnL).
        This is the Hyperliquid analogue of CoinDCX's 'Current value'.
        Returns float or None on failure."""
        st = self.clearinghouse()
        if not isinstance(st, dict):
            return None
        ms = st.get("marginSummary") or {}
        try:
            return float(ms.get("accountValue"))
        except (TypeError, ValueError):
            return None

    def withdrawable(self):
        st = self.clearinghouse()
        if not isinstance(st, dict):
            return None
        try:
            return float(st.get("withdrawable"))
        except (TypeError, ValueError):
            return None

    def positions_live(self):
        """Normalised open positions from user_state. Each item:
        {coin, side, qty, entry, unrealized, margin_used, position_value, leverage}.
        Returns [] when flat, or None if the state fetch failed."""
        st = self.clearinghouse()
        if not isinstance(st, dict):
            return None
        out = []
        for ap in st.get("assetPositions", []) or []:
            p = ap.get("position", {}) if isinstance(ap, dict) else {}
            try:
                szi = float(p.get("szi", 0) or 0)
            except (TypeError, ValueError):
                continue
            if szi == 0:
                continue
            def _f(key):
                try:
                    return float(p.get(key))
                except (TypeError, ValueError):
                    return None
            lev = (p.get("leverage") or {}).get("value")
            out.append({
                "coin": p.get("coin"),
                "side": "buy" if szi > 0 else "sell",
                "qty": abs(szi),
                "entry": _f("entryPx"),
                "unrealized": _f("unrealizedPnl"),
                "margin_used": _f("marginUsed"),
                "position_value": _f("positionValue"),
                "leverage": int(lev) if lev is not None else None,
            })
        return out

    # ─── Trading ───────────────────────────────────────────────────────
    def set_leverage(self, coin, leverage):
        try:
            return self.exchange().update_leverage(int(leverage), coin, self.cross_margin)
        except Exception as e:
            log.warning(f"set_leverage {coin} {leverage}x failed (continuing): {e}")
            return {"status": "error", "message": str(e)}

    @staticmethod
    def _normalize_order_result(res):
        """Map an SDK order/close response to the bot's flat contract:
        success -> {"id", "total_quantity", "avg_price"}
        failure -> {"status": "error", "message": "..."}"""
        try:
            if not isinstance(res, dict):
                return {"status": "error", "message": f"unexpected response: {res!r}"}
            if res.get("status") != "ok":
                return {"status": "error", "message": str(res)}
            data = (res.get("response") or {}).get("data") or {}
            statuses = data.get("statuses") or []
            if not statuses:
                return {"status": "error", "message": "no statuses in response"}
            s0 = statuses[0]
            if "error" in s0:
                return {"status": "error", "message": s0["error"]}
            fill = s0.get("filled")
            if fill:
                return {
                    "id": fill.get("oid"),
                    "total_quantity": float(fill.get("totalSz", 0) or 0),
                    "avg_price": float(fill.get("avgPx", 0) or 0),
                }
            resting = s0.get("resting")
            if resting:  # shouldn't happen for IOC market orders, but handle it
                return {"id": resting.get("oid"), "total_quantity": 0.0, "avg_price": 0.0,
                        "note": "resting"}
            return {"status": "error", "message": f"unrecognised status: {s0!r}"}
        except Exception as e:
            return {"status": "error", "message": f"parse error: {e}"}

    def open_market(self, coin, is_buy, sz, leverage):
        """Set leverage then send an IOC market order to OPEN/increase a position."""
        self.set_leverage(coin, leverage)
        res = self.exchange().market_open(coin, is_buy, float(sz), None, self.slippage)
        return self._normalize_order_result(res)

    def close_market(self, coin, sz=None):
        """Reduce-only market close. sz=None closes the whole position;
        otherwise closes `sz` coin units. Direction is inferred by the SDK."""
        res = self.exchange().market_close(coin, sz=(None if sz is None else float(sz)),
                                           slippage=self.slippage)
        return self._normalize_order_result(res)

    # ─── Native stop-loss (real HL trigger order) — OFF by default ──────
    def place_stop_market(self, coin, is_buy_to_close, sz, trigger_px):
        """Place a reduce-only stop-market (trigger) order on Hyperliquid.
        Only used if NATIVE_SL_ENABLED is turned on in server2 (default off,
        matching the old behaviour where Pine drives SL via reverse/close).
        `is_buy_to_close` is the side that CLOSES the position (opposite of entry)."""
        order_type = {"trigger": {"triggerPx": self.round_px(coin, trigger_px),
                                  "isMarket": True, "tpsl": "sl"}}
        res = self.exchange().order(
            coin, is_buy_to_close, float(sz),
            self.round_px(coin, trigger_px), order_type, reduce_only=True,
        )
        return self._normalize_order_result(res)

    def cancel_order(self, coin, oid):
        try:
            return self.exchange().cancel(coin, int(oid))
        except Exception as e:
            log.warning(f"cancel_order {coin}/{oid} failed: {e}")
            return {"status": "error", "message": str(e)}
