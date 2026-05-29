import os
import json
import math
import time
import queue
import logging
import threading
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify
from hyperliquid_client import HyperliquidFutures

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")

# ═══════════════════════════════════════════════════════════
#  CONFIG  (Hyperliquid + USDC-native)
# ═══════════════════════════════════════════════════════════
# AUTH — Hyperliquid signs with an Ethereum key, NOT an API key/secret.
#   1. Go to https://app.hyperliquid.xyz/API and "Authorize API Wallet".
#   2. HL_SECRET_KEY      = the generated API/agent wallet PRIVATE key (0x...)
#   3. HL_ACCOUNT_ADDRESS = your MAIN wallet PUBLIC address (0x...)
#   The agent key can trade but CANNOT withdraw — keep it only in Railway env.
HL_ACCOUNT_ADDRESS = os.environ.get("HL_ACCOUNT_ADDRESS", "")
HL_SECRET_KEY      = os.environ.get("HL_SECRET_KEY", "")
HL_NETWORK         = os.environ.get("HL_NETWORK", "mainnet")   # mainnet | testnet
HL_CROSS_MARGIN    = os.environ.get("HL_CROSS_MARGIN", "true").lower() == "true"
HL_SLIPPAGE        = float(os.environ.get("HL_SLIPPAGE", "0.01"))  # market order cap

WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET", "")
DEFAULT_LEVERAGE = int(os.environ.get("DEFAULT_LEVERAGE", "5"))
DEFAULT_MARGIN   = os.environ.get("DEFAULT_MARGIN", "USDC")

# PAPER MODE — default ON. No orders hit Hyperliquid; the full brain
# (tracking, profit-lock, daily cap, baseline, SL lockout, target) runs against
# live mid-prices so you can validate for weeks before flipping to live.
# Paper needs NO private key (it only reads public mids). Flip to live by
# setting PAPER_MODE=false AND providing HL_SECRET_KEY + HL_ACCOUNT_ADDRESS.
PAPER_MODE       = os.environ.get("PAPER_MODE", "true").lower() == "true"
PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "1000"))

# Position sizing (USDC margin per trade)
FIXED_MARGIN_USDT = float(os.environ.get("FIXED_MARGIN_USDT", "0"))
WALLET_USAGE_PCT  = float(os.environ.get("WALLET_USAGE_PCT", "100")) / 100
MIN_NOTIONAL_USDT = float(os.environ.get("MIN_NOTIONAL_USDT", "10"))  # HL min order value

# Symbol normalisation overrides (TradingView ticker -> HL coin), JSON, optional.
# e.g. {"1000PEPEUSDT":"kPEPE","1000BONKUSDT":"kBONK"}
try:
    SYMBOL_OVERRIDES = {k.upper(): v for k, v in
                        json.loads(os.environ.get("SYMBOL_OVERRIDES", "{}")).items()}
except Exception:
    SYMBOL_OVERRIDES = {}

# Native SL (real HL trigger order). OFF by default — Pine drives SL via
# reverse/close webhooks, same as the original deployment.
NATIVE_SL_ENABLED = os.environ.get("NATIVE_SL_ENABLED", "false").lower() == "true"

# Async webhook processing — the /webhook route returns 200 instantly and
# enqueues the signal; a single background worker drains the queue serially.
# This stops TradingView from timing out during bursts (its webhooks are not
# retried on timeout) and keeps the close→reopen of a reverse from being cut
# off mid-sequence. Serial (one worker) is deliberate: active_trades is shared
# mutable state, so processing in order avoids races and keeps one source of
# truth — same reason the gunicorn worker count is pinned to 1.
ASYNC_QUEUE_ENABLED = os.environ.get("ASYNC_QUEUE_ENABLED", "true").lower() == "true"
# Skip ENTRY jobs that have waited longer than this in the queue (price has
# moved too far to act on the original signal). Books/reverses/closes are
# always processed — you want exposure-reducing actions done even if late.
# 0 disables the staleness guard.
MAX_JOB_AGE_SEC = float(os.environ.get("MAX_JOB_AGE_SEC", "15"))
_signal_queue = queue.Queue()

app = Flask(__name__)
client = HyperliquidFutures(
    account_address=HL_ACCOUNT_ADDRESS,
    secret_key=HL_SECRET_KEY,
    network=HL_NETWORK,
    cross_margin=HL_CROSS_MARGIN,
    slippage=HL_SLIPPAGE,
)

# Paper-mode realized P&L accumulator (USDC)
_paper_realized = 0.0


def coin_from_symbol(symbol):
    """Normalise a TradingView/CoinDCX-style symbol to a Hyperliquid coin name.
        B-BTC_USDT -> BTC | BTCUSDT.P -> BTC | BTCUSD -> BTC | ETH -> ETH
    Honours SYMBOL_OVERRIDES first for special cases (e.g. 1000PEPE -> kPEPE)."""
    if not symbol:
        return symbol
    s = symbol.strip().upper()
    if s in SYMBOL_OVERRIDES:
        return SYMBOL_OVERRIDES[s]
    if s.startswith("B-") and s.endswith("_USDT"):
        s = s[2:-5]
    s = s.replace(".P", "")
    for suf in ("USDT", "USDC", "PERP", "USD"):
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
            break
    return s


def clamp_leverage(coin, leverage):
    """Clamp requested leverage to the coin's exchange max where known."""
    mx = client.max_leverage(coin)
    if mx and leverage > mx:
        log.info(f"⚙️ Leverage {leverage}x > max {mx}x for {coin} — clamping to {mx}x")
        return mx
    return leverage


def round_down_quantity(coin, qty):
    """Floor a size to the coin's szDecimals (Hyperliquid size precision)."""
    return client.round_sz(coin, qty)


# ═══════════════════════════════════════════════════════════
#  POSITION TRACKING
# ═══════════════════════════════════════════════════════════
# {coin: {pair, side, qty, original_qty, entry_price, entry_time,
#         order_id, tp_price, sl_price, books_done, leverage, margin_ccy}}
active_trades = {}

ACTIVE_TRADES_FILE = os.environ.get("ACTIVE_TRADES_FILE", "/app/data/active_trades.json")

def _save_active_trades():
    try:
        os.makedirs(os.path.dirname(ACTIVE_TRADES_FILE), exist_ok=True)
        with open(ACTIVE_TRADES_FILE, "w") as f:
            json.dump(active_trades, f)
    except Exception as e:
        log.warning(f"⚠️ Failed to persist active_trades: {e}")

def _load_active_trades():
    try:
        with open(ACTIVE_TRADES_FILE) as f:
            data = json.load(f)
        active_trades.update(data)
        if data:
            log.info(f"📂 Restored {len(data)} active trade(s) from disk: {list(data.keys())}")
        else:
            log.info("📂 active_trades file empty — starting fresh")
    except FileNotFoundError:
        log.info(f"📂 No active_trades file at {ACTIVE_TRADES_FILE} — starting fresh")
    except Exception as e:
        log.warning(f"⚠️ Failed to load active_trades ({e}) — starting fresh")


# ═══════════════════════════════════════════════════════════
#  AUTO PROFIT-LOCK — close all positions when net ROE ≥ threshold
# ═══════════════════════════════════════════════════════════
PROFIT_LOCK_ENABLED        = os.environ.get("PROFIT_LOCK_ENABLED", "true").lower() == "true"
PROFIT_LOCK_PCT            = float(os.environ.get("PROFIT_LOCK_PCT", "13.0"))
POLL_INTERVAL_SEC          = int(os.environ.get("PROFIT_LOCK_POLL_SEC", "10"))
COOLDOWN_AFTER_LOCK_SEC    = int(os.environ.get("PROFIT_LOCK_COOLDOWN_SEC", "300"))

_profit_lock_until = 0.0   # epoch timestamp; webhooks restricted until this

# ═══════════════════════════════════════════════════════════
#  AUTO LOSS-LOCK — close all positions when net ROE ≤ −threshold
# ═══════════════════════════════════════════════════════════
# Mirror image of profit-lock: a basket-level drawdown circuit breaker. When
# the combined net ROE across all open positions falls to −LOSS_LOCK_PCT, the
# monitor market-closes everything and enters the SAME cooldown profit-lock
# uses (COOLDOWN_AFTER_LOCK_SEC). It does NOT touch the daily-cap counter —
# that tracks cumulative *profit* locked, so a loss must not subtract from it.
# Off by default; enable via LOSS_LOCK_ENABLED=true and set LOSS_LOCK_PCT.
LOSS_LOCK_ENABLED          = os.environ.get("LOSS_LOCK_ENABLED", "false").lower() == "true"
LOSS_LOCK_PCT              = float(os.environ.get("LOSS_LOCK_PCT", "10.0"))

# ═══════════════════════════════════════════════════════════
#  POSITION RECONCILER — keep active_trades in sync with the exchange
# ═══════════════════════════════════════════════════════════
# Every RECONCILE_POLL_SEC, diff tracked active_trades against live exchange
# positions and repair drift so orphans can't silently accumulate:
#   • ORPHAN  (on exchange, not tracked)  -> ADOPT into tracking (best-effort:
#       side/qty/entry from the exchange; tp/sl left None for Pine to re-drive)
#   • GHOST   (tracked, not on exchange)  -> CLEAR stale tracking
#   • QTY DRIFT (both present, differ)    -> logged for visibility (not auto-fixed)
# A grace window (RECONCILE_GRACE_SEC) avoids acting on positions mid-open or
# mid-close. Skips entirely during profit-lock cooldown (post-lock sweep is
# still settling) and in paper mode. LIVE only.
RECONCILER_ENABLED     = os.environ.get("RECONCILER_ENABLED", "true").lower() == "true"
RECONCILE_POLL_SEC     = int(os.environ.get("RECONCILE_POLL_SEC", "30"))
RECONCILE_GRACE_SEC    = int(os.environ.get("RECONCILE_GRACE_SEC", "20"))
RECONCILE_ADOPT        = os.environ.get("RECONCILE_ADOPT", "true").lower() == "true"
RECONCILE_CLEAR_GHOSTS = os.environ.get("RECONCILE_CLEAR_GHOSTS", "true").lower() == "true"

_orphan_first_seen = {}   # coin -> epoch first observed as an orphan (grace timer)
_reconcile_last = {"at": None, "adopted": [], "cleared": [], "qty_drift": [], "checked": 0}

# ═══════════════════════════════════════════════════════════
#  WINNING STREAK PAUSE
# ═══════════════════════════════════════════════════════════
# Counts consecutive profit-locks. Each profit-lock increments; a loss-lock
# resets to 0. When count reaches STREAK_THRESHOLD, the bot pauses (rejects
# webhooks) for STREAK_PAUSE_SEC, then resumes — counter clears on resume.
# Off by default; enable via STREAK_ENABLED=true. State persists across
# restarts in STREAK_FILE (use a Railway volume for durability).
STREAK_ENABLED   = os.environ.get("STREAK_ENABLED", "false").lower() == "true"
STREAK_THRESHOLD = int(os.environ.get("STREAK_THRESHOLD", "3"))
STREAK_PAUSE_SEC = int(os.environ.get("STREAK_PAUSE_SEC", "1800"))   # 30 min default
STREAK_FILE      = os.environ.get("STREAK_FILE", "/app/data/streak_state.json")

_streak_count        = 0
_streak_pause_until  = 0.0
_streak_last_lock_at = None
_streak_max_ever     = 0

# ═══════════════════════════════════════════════════════════
#  DAILY PROFIT CAP — hard stop after N% cumulative locked
# ═══════════════════════════════════════════════════════════
DAILY_CAP_ENABLED          = os.environ.get("DAILY_CAP_ENABLED", "true").lower() == "true"
DAILY_CAP_PCT              = float(os.environ.get("DAILY_CAP_PCT", "26.0"))

_daily_locked_pct_sum      = 0.0
_daily_lock_count          = 0
_daily_paused              = False
_daily_counter_date        = None

IST_TZ = timezone(timedelta(hours=5, minutes=30))

# ═══════════════════════════════════════════════════════════
#  BASELINE EQUITY TARGET — auto-rollover profit lock (USDC)
# ═══════════════════════════════════════════════════════════
BASELINE_ENABLED        = os.environ.get("BASELINE_ENABLED", "false").lower() == "true"
BASELINE_TRIGGER_PCT    = float(os.environ.get("BASELINE_TRIGGER_PCT", "6.0"))
BASELINE_ROLLOVER_PCT   = float(os.environ.get("BASELINE_ROLLOVER_PCT", "5.0"))
BASELINE_COOLDOWN_SEC   = int(os.environ.get("BASELINE_COOLDOWN_SEC", "60"))
BASELINE_FILE           = os.environ.get("BASELINE_FILE", "/app/data/baseline_state.json")

_baseline_usdt          = None
_baseline_realized_pnl  = 0.0
_baseline_lock_count    = 0
_baseline_last_lock_at  = None
_baseline_history       = []
_baseline_cooldown_until = 0.0

# ─── SL LOCKOUT ──────────────────────────────────────────────
SL_LOCKOUT_ENABLED   = os.environ.get("SL_LOCKOUT_ENABLED", "true").lower() == "true"
SL_LOCKOUT_SEC       = int(os.environ.get("SL_LOCKOUT_SEC", "600"))
SL_LOCKOUT_CHECK_SEC = int(os.environ.get("SL_LOCKOUT_CHECK_SEC", "30"))
_sl_lockout = {}  # {coin: unlock_epoch_time}

def mark_sl_lockout(symbol, reason=""):
    if not SL_LOCKOUT_ENABLED:
        return
    until = time.time() + SL_LOCKOUT_SEC
    _sl_lockout[symbol] = until
    mins = SL_LOCKOUT_SEC // 60
    log.info(f"🚫 SL LOCKOUT armed: {symbol} blocked for {mins}m ({reason})")

def in_sl_lockout(symbol):
    if not SL_LOCKOUT_ENABLED:
        return False, 0
    until = _sl_lockout.get(symbol, 0)
    if until == 0:
        return False, 0
    remaining = int(until - time.time())
    if remaining <= 0:
        _sl_lockout.pop(symbol, None)
        return False, 0
    return True, remaining

def sl_lockout_worker():
    log.info(f"🚫 SL lockout monitor started — enabled={SL_LOCKOUT_ENABLED}, "
             f"duration={SL_LOCKOUT_SEC}s, check_every={SL_LOCKOUT_CHECK_SEC}s")
    while True:
        try:
            time.sleep(SL_LOCKOUT_CHECK_SEC)
            if not SL_LOCKOUT_ENABLED:
                continue
            now = time.time()
            expired = [s for s, until in list(_sl_lockout.items()) if until <= now]
            for s in expired:
                _sl_lockout.pop(s, None)
                log.info(f"✅ SL LOCKOUT cleared: {s} — symbol free to re-enter")
            if _sl_lockout:
                pretty = ", ".join(f"{s}({int(u-now)}s left)" for s, u in _sl_lockout.items())
                log.info(f"🚫 SL lockout active: {pretty}")
        except Exception as e:
            log.error(f"sl_lockout_worker error: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════
#  TARGET CURRENT VALUE — equity-based hard stop (USDC)
#
#  Polls Hyperliquid wallet equity every TARGET_POLL_SEC. Uses the realized
#  balance (account equity minus floating unrealized P&L) — the same
#  "banked money" intent as the old CoinDCX wallet-only trigger, so it fires
#  on actually-banked profit rather than paper gains.
# ═══════════════════════════════════════════════════════════
TARGET_ENABLED        = os.environ.get("TARGET_ENABLED", "true").lower() == "true"
TARGET_CURRENT_VALUE  = float(os.environ.get("TARGET_CURRENT_VALUE", "0"))  # 0 = disabled
TARGET_POLL_SEC       = int(os.environ.get("TARGET_POLL_SEC", "5"))
TARGET_FILE           = os.environ.get("TARGET_FILE", "/app/data/target_state.json")

_target_value         = TARGET_CURRENT_VALUE
_target_hit           = False
_target_hit_at        = None
_target_last_value    = None
_target_last_check_at = None
_target_last_error    = None


# ─── USDC equity helpers (paper vs live) ───────────────────────────
def get_unrealized_usdt():
    """Net unrealized P&L across the book in USDC, or None if unavailable."""
    pnl, _, _ = compute_net_roe()
    return pnl

def get_wallet_usdt():
    """Banked balance excluding floating P&L (USDC), or None."""
    if PAPER_MODE:
        return PAPER_START_USDT + _paper_realized
    av = client.account_value()
    if av is None:
        return None
    u = get_unrealized_usdt()
    return av - (u or 0.0)

def get_current_value_usdt():
    """Total account equity in USDC (wallet + unrealized), or None."""
    if PAPER_MODE:
        u = get_unrealized_usdt()
        if u is None:
            return None
        return PAPER_START_USDT + _paper_realized + u
    return client.account_value()


def load_target_state():
    global _target_value, _target_hit, _target_hit_at
    try:
        with open(TARGET_FILE) as f:
            state = json.load(f)
        _target_value  = float(state.get("target_value", TARGET_CURRENT_VALUE))
        _target_hit    = bool(state.get("hit", False))
        _target_hit_at = state.get("hit_at")
        log.info(f"🎯 Target state loaded: target={_target_value} USDC, "
                 f"hit={_target_hit}, hit_at={_target_hit_at}")
    except FileNotFoundError:
        log.info(f"🎯 No target file at {TARGET_FILE} — using env default {TARGET_CURRENT_VALUE} USDC")
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        log.warning(f"⚠️ Target file corrupt ({e}) — using env default {TARGET_CURRENT_VALUE} USDC")


def save_target_state():
    try:
        os.makedirs(os.path.dirname(TARGET_FILE), exist_ok=True)
        with open(TARGET_FILE, "w") as f:
            json.dump({"target_value": _target_value, "hit": _target_hit,
                       "hit_at": _target_hit_at}, f, indent=2)
    except Exception as e:
        log.error(f"❌ Failed to save target state: {e}")


def target_worker():
    """Polls banked wallet equity (USDC) and flips _target_hit when it reaches
    the target. Books/closes still pass; new entries/reverses get rejected."""
    global _target_hit, _target_hit_at, _target_last_value, _target_last_check_at, _target_last_error
    log.info(f"🎯 Target monitor started — enabled={TARGET_ENABLED}, "
             f"target={_target_value} USDC, poll={TARGET_POLL_SEC}s, "
             f"mode={'paper' if PAPER_MODE else 'wallet-only'}, file={TARGET_FILE}")
    while True:
        try:
            time.sleep(TARGET_POLL_SEC)
            if not TARGET_ENABLED:
                continue
            if _target_value is None or _target_value <= 0:
                continue
            wallet = get_wallet_usdt()
            _target_last_check_at = datetime.now(timezone.utc).isoformat()
            if wallet is None:
                _target_last_error = "wallet fetch failed"
                continue
            _target_last_value = wallet
            _target_last_error = None
            if not _target_hit and wallet >= _target_value:
                _target_hit = True
                _target_hit_at = datetime.now(timezone.utc).isoformat()
                save_target_state()
                log.info(f"🎯 TARGET HIT: wallet={wallet:.2f} >= target={_target_value:.2f} USDC "
                         f"— new entries/reverses will be rejected")
        except Exception as e:
            _target_last_error = str(e)
            log.error(f"target_worker error: {e}", exc_info=True)


# ─── Event Log ───
trade_log = []
MAX_LOG = 50

# ═══════════════════════════════════════════════════════════
#  NET ROE  (paper: mids+tracking | live: user_state)
# ═══════════════════════════════════════════════════════════
def compute_net_roe():
    """Return (net_pnl_usdc, total_margin_usdc, net_pct) across the book.
    Returns (None, None, None) if state/prices are unavailable this cycle."""
    if not active_trades:
        return 0.0, 0.0, 0.0

    if PAPER_MODE:
        mids = client.all_mids()
        total_pnl = 0.0
        total_margin = 0.0
        for coin, t in list(active_trades.items()):
            mark = mids.get(coin)
            if mark is None or mark <= 0:
                return None, None, None
            entry = float(t.get("entry_price", 0) or 0)
            qty   = float(t.get("qty", 0) or 0)
            side  = t.get("side", "")
            lev   = int(t.get("leverage", DEFAULT_LEVERAGE) or DEFAULT_LEVERAGE)
            if entry <= 0 or qty <= 0:
                continue
            direction = 1 if side == "buy" else -1
            total_pnl += (mark - entry) * qty * direction
            total_margin += (entry * qty / lev)
        if total_margin <= 0:
            return total_pnl, 0.0, 0.0
        return total_pnl, total_margin, (total_pnl / total_margin) * 100

    # LIVE — pull straight from Hyperliquid's clearinghouse snapshot
    poss = client.positions_live()
    if poss is None:
        return None, None, None
    pos_map = {p["coin"]: p for p in poss if p.get("coin")}
    total_pnl = 0.0
    total_margin = 0.0
    for coin in list(active_trades.keys()):
        p = pos_map.get(coin)
        if not p:
            continue  # tracked but already flat on the exchange
        u = p.get("unrealized")
        m = p.get("margin_used")
        if u is None or m is None:
            continue
        total_pnl += u
        total_margin += m
    if total_margin <= 0:
        return total_pnl, 0.0, 0.0
    return total_pnl, total_margin, (total_pnl / total_margin) * 100


# ─── DAILY CAP HELPERS ─────────────────────────────────────
def _current_ist_date():
    return datetime.now(IST_TZ).date()

def _check_and_reset_daily_counter():
    global _daily_locked_pct_sum, _daily_lock_count, _daily_paused, _daily_counter_date
    today = _current_ist_date()
    if _daily_counter_date != today:
        if _daily_counter_date is not None:
            log.info(f"🌅 IST midnight crossed — resetting daily counter "
                     f"(previous day: locks={_daily_lock_count}, "
                     f"sum={_daily_locked_pct_sum:.2f}%, paused={_daily_paused})")
        _daily_locked_pct_sum = 0.0
        _daily_lock_count = 0
        _daily_paused = False
        _daily_counter_date = today

def _bump_daily_counter(lock_pct):
    global _daily_locked_pct_sum, _daily_lock_count, _daily_paused
    _check_and_reset_daily_counter()
    _daily_locked_pct_sum += lock_pct
    _daily_lock_count += 1
    log.info(f"📊 Daily progress: lock #{_daily_lock_count} added {lock_pct:.2f}% "
             f"→ cumulative {_daily_locked_pct_sum:.2f}% / cap {DAILY_CAP_PCT}%")
    if DAILY_CAP_ENABLED and _daily_locked_pct_sum >= DAILY_CAP_PCT and not _daily_paused:
        _daily_paused = True
        log.warning(f"🛑 DAILY CAP REACHED — cumulative {_daily_locked_pct_sum:.2f}% "
                    f"≥ {DAILY_CAP_PCT}% — ALL webhooks paused until IST midnight "
                    f"or manual /daily-cap/reset")

def daily_cap_active():
    _check_and_reset_daily_counter()
    return DAILY_CAP_ENABLED and _daily_paused


def _sweep_residual_positions(max_passes=2):
    """LIVE-only: ask Hyperliquid for ANY open position and market-close it,
    whether or not the bot was tracking it. This catches two failure modes a
    tracked-only close misses: (1) positions orphaned by earlier state drift,
    and (2) tracked closes that silently errored. Returns the residual count
    after sweeping: 0 = confirmed flat, None = couldn't verify (fetch failed)."""
    if PAPER_MODE:
        return 0
    last_seen = None
    for _ in range(max_passes):
        poss = client.positions_live(force=True)  # bypass TTL cache — must be fresh
        if poss is None:
            log.warning("⚠️ Post-lock sweep: position fetch failed — cannot verify flat")
            return None
        residual = [p for p in poss if p.get("coin") and float(p.get("qty") or 0) > 0]
        if not residual:
            log.info("✅ Post-lock sweep: no residual positions found")
            return 0
        last_seen = len(residual)
        names = ", ".join(p["coin"] for p in residual)
        log.warning(f"🧹 Post-lock sweep: {len(residual)} residual position(s) — force-closing: {names}")
        for p in residual:
            try:
                res = client.close_market(p["coin"], sz=None)  # full close, direction inferred
                if isinstance(res, dict) and res.get("status") == "error":
                    log.warning(f"⚠️ Sweep close failed for {p['coin']}: {res.get('message','')}")
                else:
                    log.info(f"🔻 Swept residual: {p['coin']} ({p['qty']})")
            except Exception as e:
                log.error(f"❌ Sweep close exception for {p['coin']}: {e}", exc_info=True)
        time.sleep(1)  # let fills settle before re-checking
    return last_seen


# ═══════════════════════════════════════════════════════════
#  STREAK HELPERS
# ═══════════════════════════════════════════════════════════
def load_streak_state():
    global _streak_count, _streak_pause_until, _streak_last_lock_at, _streak_max_ever
    try:
        with open(STREAK_FILE) as f:
            s = json.load(f)
        _streak_count        = int(s.get("count", 0))
        _streak_pause_until  = float(s.get("pause_until", 0.0))
        _streak_last_lock_at = s.get("last_lock_at")
        _streak_max_ever     = int(s.get("max_ever", 0))
        log.info(f"📈 Streak loaded: count={_streak_count}, max_ever={_streak_max_ever}, "
                 f"paused={_streak_pause_until > time.time()}")
    except FileNotFoundError:
        log.info(f"📈 No streak file at {STREAK_FILE} — starting at 0")
    except Exception as e:
        log.warning(f"⚠️ Streak file load failed ({e}) — starting fresh")


def save_streak_state():
    try:
        os.makedirs(os.path.dirname(STREAK_FILE), exist_ok=True)
        with open(STREAK_FILE, "w") as f:
            json.dump({
                "count": _streak_count,
                "pause_until": _streak_pause_until,
                "last_lock_at": _streak_last_lock_at,
                "max_ever": _streak_max_ever,
            }, f, indent=2)
    except Exception as e:
        log.error(f"❌ Failed to save streak state: {e}")


def streak_pause_active():
    return STREAK_ENABLED and time.time() < _streak_pause_until


def streak_pause_remaining_sec():
    return max(0, int(_streak_pause_until - time.time()))


def _bump_streak_on_profit():
    """Called when a profit-lock fires. Increments the consecutive-win counter
    and, if it hits STREAK_THRESHOLD, arms the pause."""
    global _streak_count, _streak_pause_until, _streak_last_lock_at, _streak_max_ever
    if not STREAK_ENABLED:
        return
    _streak_count += 1
    _streak_last_lock_at = datetime.now(timezone.utc).isoformat()
    if _streak_count > _streak_max_ever:
        _streak_max_ever = _streak_count
    log.info(f"📈 Profit-lock streak: {_streak_count}/{STREAK_THRESHOLD}")
    if _streak_count >= STREAK_THRESHOLD:
        _streak_pause_until = time.time() + STREAK_PAUSE_SEC
        until = datetime.fromtimestamp(_streak_pause_until).strftime("%H:%M:%S")
        log.warning(f"🛑 STREAK PAUSE armed — {_streak_count} consecutive profit-locks "
                    f"≥ threshold {STREAK_THRESHOLD}; webhooks rejected until {until} "
                    f"(then counter resets and trading resumes)")
    save_streak_state()


def _reset_streak_on_loss():
    """Called when a loss-lock fires. Breaks the winning streak."""
    global _streak_count
    if not STREAK_ENABLED:
        return
    if _streak_count > 0:
        log.info(f"📉 Loss-lock — streak broken at {_streak_count}, resetting to 0")
        _streak_count = 0
        save_streak_state()


def _maybe_resume_after_streak_pause():
    """If a streak pause has elapsed, clear the counter so trading resumes
    fresh. Called from the webhook gate so the transition is observable."""
    global _streak_count, _streak_pause_until
    if not STREAK_ENABLED:
        return
    if _streak_pause_until and time.time() >= _streak_pause_until and _streak_count > 0:
        log.info(f"✅ Streak pause elapsed — resetting counter (was {_streak_count}) and resuming")
        _streak_count = 0
        _streak_pause_until = 0.0
        save_streak_state()


def close_all_positions(trigger_reason="profit lock", trigger_pct=None, lock_kind="manual"):
    """Close every position in active_trades via market order, clear state,
    and activate the post-lock cooldown. Bumps the daily counter if trigger_pct
    is provided."""
    global _profit_lock_until
    snapshot = list(active_trades.items())
    log.info(f"🔒 LOCK triggered ({trigger_reason}) — closing {len(snapshot)} positions")
    mids = client.all_mids() if PAPER_MODE else {}
    for coin, trade in snapshot:
        try:
            close_qty = float(trade.get("qty", 0) or 0)
            if close_qty <= 0:
                continue
            log.info(f"🔻 LOCK close: {close_qty} {coin}")
            result = place_market(coin, _close_side(trade), close_qty,
                                  trade.get("leverage", DEFAULT_LEVERAGE), reduce_only=True)
            if isinstance(result, dict) and result.get("status") == "error":
                log.warning(f"⚠️ Lock close failed for {coin}: {result.get('message','')}")
            # Paper: realize against current mid so paper wallet reflects the close
            if PAPER_MODE:
                exit_px = mids.get(coin) or trade.get("entry_price")
                record_realized(coin, trade.get("entry_price"), exit_px, close_qty, trade.get("side"))
            log_trade_event(coin, _close_side(trade), "profit_lock", "FILLED", trigger_reason)
        except Exception as e:
            log.error(f"❌ Lock close exception for {coin}: {e}", exc_info=True)
    for coin in list(native_sl_orders.keys()):
        try:
            cancel_native_sl(coin)
        except Exception:
            pass
    # Post-lock sweep — flatten ANY residual exchange position (orphans + any
    # tracked close that silently failed) so "lock complete" actually means flat.
    residual = _sweep_residual_positions()
    active_trades.clear()
    _save_active_trades()
    _profit_lock_until = time.time() + COOLDOWN_AFTER_LOCK_SEC
    cooldown_end = datetime.fromtimestamp(_profit_lock_until).strftime("%H:%M:%S")
    if residual == 0:
        log.info(f"✅ LOCK complete — all positions closed (swept flat), cooldown until {cooldown_end}")
    elif residual is None:
        log.warning(f"⚠️ LOCK complete — closes sent but sweep could NOT verify flat; "
                    f"CHECK EXCHANGE MANUALLY. cooldown until {cooldown_end}")
    else:
        log.warning(f"⚠️ LOCK complete — {residual} position(s) STILL OPEN after sweep; "
                    f"CHECK EXCHANGE MANUALLY. cooldown until {cooldown_end}")
    if trigger_pct is not None:
        _bump_daily_counter(trigger_pct)
    # Streak: profit-lock increments, loss-lock resets, others no-op
    if lock_kind == "profit":
        _bump_streak_on_profit()
    elif lock_kind == "loss":
        _reset_streak_on_loss()


def profit_lock_worker():
    log.info(f"🎯 Profit/loss-lock monitor started — profit≥{PROFIT_LOCK_PCT}% "
             f"(enabled={PROFIT_LOCK_ENABLED}), loss≤-{LOSS_LOCK_PCT}% "
             f"(enabled={LOSS_LOCK_ENABLED}), poll={POLL_INTERVAL_SEC}s, "
             f"cooldown={COOLDOWN_AFTER_LOCK_SEC}s")
    while True:
        try:
            time.sleep(POLL_INTERVAL_SEC)
            if not (PROFIT_LOCK_ENABLED or LOSS_LOCK_ENABLED):
                continue
            if time.time() < _profit_lock_until:
                continue
            if not active_trades:
                continue
            pnl, margin, pct = compute_net_roe()
            if pct is None:
                log.debug("profit/loss-lock: price/state unavailable this cycle")
                continue
            log.info(f"🎯 net_roe check: pnl={pnl:.2f} margin={margin:.2f} "
                     f"net={pct:.2f}% (profit≥{PROFIT_LOCK_PCT}% / loss≤-{LOSS_LOCK_PCT}%) "
                     f"positions={len(active_trades)}")
            if PROFIT_LOCK_ENABLED and pct >= PROFIT_LOCK_PCT:
                close_all_positions(
                    trigger_reason=f"net ROE {pct:.2f}% ≥ {PROFIT_LOCK_PCT}%",
                    trigger_pct=pct, lock_kind="profit")
            elif LOSS_LOCK_ENABLED and pct <= -LOSS_LOCK_PCT:
                # Loss does NOT feed the daily PROFIT cap (trigger_pct=None),
                # but it shares the same cooldown set by close_all_positions.
                close_all_positions(
                    trigger_reason=f"net ROE {pct:.2f}% ≤ -{LOSS_LOCK_PCT}% (loss lock)",
                    trigger_pct=None, lock_kind="loss")
        except Exception as e:
            log.error(f"profit/loss-lock worker error: {e}", exc_info=True)


def in_profit_lock_cooldown():
    return time.time() < _profit_lock_until

def cooldown_remaining_sec():
    return max(0, int(_profit_lock_until - time.time()))


# ═══════════════════════════════════════════════════════════
#  BASELINE HELPERS (USDC)
# ═══════════════════════════════════════════════════════════
def load_baseline_state():
    global _baseline_usdt, _baseline_realized_pnl, _baseline_lock_count
    global _baseline_last_lock_at, _baseline_history
    try:
        with open(BASELINE_FILE) as f:
            state = json.load(f)
        _baseline_usdt          = state.get("baseline")
        _baseline_realized_pnl  = float(state.get("realized_pnl", 0.0))
        _baseline_lock_count    = int(state.get("lock_count", 0))
        _baseline_last_lock_at  = state.get("last_lock_at")
        _baseline_history       = state.get("history", [])
        log.info(f"📐 Baseline loaded: {_baseline_usdt} USDC, "
                 f"realized={_baseline_realized_pnl:.2f}, locks={_baseline_lock_count}")
    except FileNotFoundError:
        log.info(f"📐 No baseline file at {BASELINE_FILE} — baseline starts uninitialized")
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        log.warning(f"⚠️ Baseline file corrupt ({e}) — starting fresh")


def save_baseline_state():
    try:
        os.makedirs(os.path.dirname(BASELINE_FILE), exist_ok=True)
        with open(BASELINE_FILE, "w") as f:
            json.dump({
                "baseline": _baseline_usdt,
                "realized_pnl": _baseline_realized_pnl,
                "lock_count": _baseline_lock_count,
                "last_lock_at": _baseline_last_lock_at,
                "history": _baseline_history[-50:],
            }, f, indent=2)
    except Exception as e:
        log.error(f"❌ Failed to save baseline: {e}")


def baseline_current_equity():
    if _baseline_usdt is None:
        return None
    pnl_open, _, _ = compute_net_roe()
    if pnl_open is None:
        pnl_open = 0.0
    return _baseline_usdt + _baseline_realized_pnl + pnl_open

def in_baseline_cooldown():
    return time.time() < _baseline_cooldown_until

def baseline_cooldown_remaining_sec():
    return max(0, int(_baseline_cooldown_until - time.time()))


def trigger_baseline_lock():
    global _baseline_usdt, _baseline_realized_pnl, _baseline_lock_count
    global _baseline_last_lock_at, _baseline_history, _baseline_cooldown_until

    old_baseline = _baseline_usdt
    if old_baseline is None or old_baseline <= 0:
        return
    equity_at_trigger = baseline_current_equity()
    if equity_at_trigger is None:
        log.warning("⚠️ Baseline trigger aborted — equity unavailable")
        return

    log.info(f"🎯 BASELINE TRIGGER: equity {equity_at_trigger:.2f} ≥ "
             f"target {old_baseline * (1 + BASELINE_TRIGGER_PCT/100):.2f} USDC")

    close_all_positions(
        trigger_reason=f"baseline target — equity {equity_at_trigger:.2f} ≥ "
                       f"{old_baseline * (1 + BASELINE_TRIGGER_PCT/100):.2f}",
        trigger_pct=None, lock_kind="baseline")

    new_baseline = old_baseline * (1 + BASELINE_ROLLOVER_PCT / 100)
    realized_this_cycle = equity_at_trigger - old_baseline

    _baseline_usdt = new_baseline
    _baseline_realized_pnl = 0.0
    _baseline_lock_count += 1
    _baseline_last_lock_at = datetime.now(timezone.utc).isoformat()
    _baseline_history.append({
        "timestamp": _baseline_last_lock_at,
        "old_baseline": old_baseline,
        "trigger_equity": equity_at_trigger,
        "new_baseline": new_baseline,
        "realized_pnl": realized_this_cycle,
    })
    _baseline_cooldown_until = time.time() + BASELINE_COOLDOWN_SEC
    save_baseline_state()
    log.info(f"📈 Baseline rolled: {old_baseline:.2f} → {new_baseline:.2f} USDC "
             f"(banked {new_baseline - old_baseline:.2f}, "
             f"cooldown={BASELINE_COOLDOWN_SEC}s)")


def baseline_add_realized(symbol, pnl_usdc):
    """Add already-computed realized P&L (USDC) to the baseline accumulator."""
    global _baseline_realized_pnl
    if _baseline_usdt is None:
        return
    _baseline_realized_pnl += pnl_usdc
    log.info(f"📐 Realized P&L recorded: {symbol} {pnl_usdc:+.4f} USDC | "
             f"total since baseline: {_baseline_realized_pnl:+.2f}")
    save_baseline_state()


def record_realized(coin, entry_price, exit_price, qty, side):
    """Compute realized P&L (USDC) for a close and feed it to the paper wallet
    (in PAPER_MODE) and the baseline accumulator (if a baseline is set)."""
    global _paper_realized
    try:
        entry = float(entry_price or 0)
        exit_ = float(exit_price or 0)
        q     = float(qty or 0)
        if entry <= 0 or exit_ <= 0 or q <= 0:
            return
        direction = 1 if side == "buy" else -1
        pnl = (exit_ - entry) * q * direction
    except Exception:
        return
    if PAPER_MODE:
        _paper_realized += pnl
        log.info(f"📝 [PAPER] realized {coin} {side} {q} @ {entry}→{exit_} = "
                 f"{pnl:+.4f} USDC | paper_realized={_paper_realized:+.2f}")
    baseline_add_realized(coin, pnl)


def baseline_worker():
    log.info(f"📐 Baseline monitor started — trigger={BASELINE_TRIGGER_PCT}%, "
             f"rollover={BASELINE_ROLLOVER_PCT}%, cooldown={BASELINE_COOLDOWN_SEC}s, "
             f"poll={POLL_INTERVAL_SEC}s, file={BASELINE_FILE}")
    while True:
        try:
            time.sleep(POLL_INTERVAL_SEC)
            if not BASELINE_ENABLED:
                continue
            if _baseline_usdt is None or _baseline_usdt <= 0:
                continue
            if in_baseline_cooldown():
                continue
            equity = baseline_current_equity()
            if equity is None:
                continue
            target = _baseline_usdt * (1 + BASELINE_TRIGGER_PCT / 100)
            if equity >= target:
                trigger_baseline_lock()
        except Exception as e:
            log.error(f"baseline worker error: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════
#  RECONCILER  (LIVE only)
# ═══════════════════════════════════════════════════════════
def reconcile_once():
    """One reconciliation pass: diff active_trades vs the live exchange and
    repair drift. Returns a summary dict. No-op in paper mode or if the
    position fetch fails (never acts blind)."""
    global _reconcile_last
    summary = {"at": datetime.now(timezone.utc).isoformat(),
               "adopted": [], "cleared": [], "qty_drift": [], "checked": 0}
    if PAPER_MODE:
        return summary
    poss = client.positions_live()
    if poss is None:
        summary["error"] = "position fetch failed"
        _reconcile_last = summary
        return summary
    now = time.time()
    ex = {p["coin"]: p for p in poss if p.get("coin") and float(p.get("qty") or 0) > 0}
    summary["checked"] = len(ex)
    tracked = set(active_trades.keys())
    ex_coins = set(ex.keys())

    # ── ORPHANS: on exchange, not tracked → adopt (after grace) ──
    for coin in ex_coins - tracked:
        first = _orphan_first_seen.setdefault(coin, now)
        if not RECONCILE_ADOPT:
            log.warning(f"🔭 RECONCILE: orphan {coin} on exchange, not tracked (adopt disabled)")
            continue
        if now - first < RECONCILE_GRACE_SEC:
            continue  # within grace — may be a position the worker is mid-opening
        p = ex[coin]
        set_active_trade(coin, side=p["side"], qty=p["qty"],
                         entry_price=(p.get("entry") or 0), order_id="reconciled",
                         tp_price=None, sl_price=None,
                         leverage=(p.get("leverage") or DEFAULT_LEVERAGE), margin_ccy="USDC")
        _orphan_first_seen.pop(coin, None)
        summary["adopted"].append(coin)
        log.warning(f"🔭 RECONCILE: ADOPTED orphan {coin} {p['side']} {p['qty']} @ {p.get('entry')} "
                    f"— now under management")

    # forget grace timers for coins no longer orphaned
    for coin in list(_orphan_first_seen.keys()):
        if coin not in (ex_coins - tracked):
            _orphan_first_seen.pop(coin, None)

    # ── GHOSTS: tracked, not on exchange → clear (after grace) ──
    if RECONCILE_CLEAR_GHOSTS:
        for coin in tracked - ex_coins:
            t = active_trades.get(coin) or {}
            age = now - float(t.get("entry_time", 0) or 0)
            if age < RECONCILE_GRACE_SEC:
                continue  # freshly opened — exchange state may just be lagging
            clear_active_trade(coin, "reconcile: gone from exchange")
            summary["cleared"].append(coin)
            log.warning(f"🔭 RECONCILE: CLEARED ghost {coin} — tracked but not on exchange")

    # ── QTY DRIFT: both present, sizes differ → log only ──
    for coin in tracked & ex_coins:
        t = active_trades.get(coin) or {}
        tq = float(t.get("qty", 0) or 0)
        eq = float(ex[coin].get("qty", 0) or 0)
        if tq > 0 and abs(tq - eq) / tq > 0.05:
            summary["qty_drift"].append({"coin": coin, "tracked": tq, "exchange": eq})
            log.warning(f"🔭 RECONCILE: qty drift {coin} tracked={tq} exchange={eq} (not auto-fixed)")

    _reconcile_last = summary
    return summary


def reconcile_worker():
    log.info(f"🔭 Reconciler started — enabled={RECONCILER_ENABLED}, poll={RECONCILE_POLL_SEC}s, "
             f"grace={RECONCILE_GRACE_SEC}s, adopt={RECONCILE_ADOPT}, "
             f"clear_ghosts={RECONCILE_CLEAR_GHOSTS}")
    while True:
        try:
            time.sleep(RECONCILE_POLL_SEC)
            if not RECONCILER_ENABLED or PAPER_MODE:
                continue
            if in_profit_lock_cooldown():
                continue  # post-lock sweep still settling — don't adopt closing positions
            reconcile_once()
        except Exception as e:
            log.error(f"reconcile_worker error: {e}", exc_info=True)


def log_trade_event(symbol, action, alert_type, result, reason=""):
    trade_log.append({
        "time": datetime.now().strftime("%H:%M:%S"),
        "symbol": symbol, "action": action,
        "type": alert_type, "result": result, "reason": reason
    })
    if len(trade_log) > MAX_LOG:
        trade_log.pop(0)

def set_active_trade(coin, side, qty, entry_price, order_id, tp_price=None, sl_price=None,
                     leverage=5, margin_ccy="USDC"):
    active_trades[coin] = {
        "pair": coin, "side": side, "qty": qty, "original_qty": qty,
        "entry_price": entry_price, "entry_time": time.time(),
        "order_id": order_id, "tp_price": tp_price, "sl_price": sl_price,
        "books_done": 0, "leverage": leverage, "margin_ccy": margin_ccy
    }
    log.info(f"📝 Tracked: {side.upper()} {qty} {coin} @ {entry_price} | TP={tp_price} SL={sl_price}")
    _save_active_trades()

def clear_active_trade(coin, reason=""):
    old = active_trades.pop(coin, None)
    if old:
        cancel_native_sl(coin)
        log.info(f"🔓 Cleared: {coin} — {reason}")
        _save_active_trades()

def _close_side(trade):
    return "sell" if trade.get("side") == "buy" else "buy"

def calc_quantity(coin, coin_price, leverage):
    """USDC-margin sizing: notional = margin × leverage; qty = notional / price,
    floored to the coin's szDecimals. Rejects orders below HL's min notional."""
    if FIXED_MARGIN_USDT <= 0 or coin_price <= 0:
        return 0
    notional = FIXED_MARGIN_USDT * WALLET_USAGE_PCT * leverage
    if notional < MIN_NOTIONAL_USDT:
        log.error(f"❌ notional {notional:.2f} < min {MIN_NOTIONAL_USDT} USDC for {coin}")
        return 0
    qty = round_down_quantity(coin, notional / coin_price)
    if qty <= 0 or qty * coin_price < MIN_NOTIONAL_USDT:
        log.error(f"❌ qty {qty} ({qty*coin_price:.2f} USDC) below min for {coin}")
        return 0
    return qty


# ═══════════════════════════════════════════════════════════
#  ORDER PLACEMENT (paper vs live)
# ═══════════════════════════════════════════════════════════
def place_market(coin, side, qty, leverage, reduce_only=False, price_hint=None):
    """Unified market-order entrypoint. Returns the bot's flat contract:
       success -> {"id", "total_quantity", "avg_price"}
       failure -> {"status": "error", "message": "..."}"""
    is_buy = (side == "buy")
    if PAPER_MODE:
        px = price_hint or client.mid(coin) or 0
        return {"id": f"paper-{int(time.time()*1000)}", "total_quantity": float(qty),
                "avg_price": float(px or 0), "paper": True}
    try:
        if reduce_only:
            return client.close_market(coin, sz=qty)
        return client.open_market(coin, is_buy, qty, leverage)
    except Exception as e:
        log.error(f"❌ place_market {coin} failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


# ═══════════════════════════════════════════════════════════
#  NATIVE SL — real HL trigger order, OFF by default
# ═══════════════════════════════════════════════════════════
native_sl_orders = {}  # {coin: {"order_id": "...", "sl_price": ...}}

def place_native_sl(coin, side, qty, sl_price, leverage, margin_ccy):
    """Place a reduce-only stop-market on Hyperliquid as a safety net.
    Disabled by default (NATIVE_SL_ENABLED=false) — Pine drives SL via
    reverse/close webhooks, matching the original deployment."""
    if not NATIVE_SL_ENABLED:
        log.info(f"🛡️ Native SL skipped (disabled): {coin} @ {sl_price}")
        return
    if PAPER_MODE:
        native_sl_orders[coin] = {"order_id": f"paper-sl-{coin}", "sl_price": sl_price}
        log.info(f"🛡️ [PAPER] Native SL noted: {coin} @ {sl_price}")
        return
    try:
        close_side_is_buy = (side == "sell")  # side closing the position
        res = client.place_stop_market(coin, close_side_is_buy, qty, sl_price)
        if isinstance(res, dict) and res.get("status") == "error":
            log.warning(f"⚠️ Native SL place failed for {coin}: {res.get('message')}")
            return
        native_sl_orders[coin] = {"order_id": res.get("id", "unknown"), "sl_price": sl_price}
        log.info(f"🛡️ Native SL placed: {coin} @ {sl_price} (oid={res.get('id')})")
    except Exception as e:
        log.warning(f"⚠️ Native SL exception for {coin}: {e}")


def cancel_native_sl(coin):
    if coin not in native_sl_orders:
        return
    order_info = native_sl_orders.pop(coin)
    order_id = order_info.get("order_id", "")
    if not order_id or order_id == "unknown" or str(order_id).startswith("paper"):
        return
    if PAPER_MODE or not NATIVE_SL_ENABLED:
        return
    try:
        res = client.cancel_order(coin, order_id)
        if isinstance(res, dict) and res.get("status") == "error":
            log.warning(f"⚠️ Native SL cancel may have failed: {coin} — likely already filled")
        else:
            log.info(f"🛡️ Native SL cancelled: {coin} | order={order_id}")
    except Exception as e:
        log.warning(f"⚠️ Native SL cancel error for {coin}: {e}")


# ═══════════════════════════════════════════════════════════
#  WEBHOOK HANDLER — Pine drives all decisions
# ═══════════════════════════════════════════════════════════
@app.route("/webhook", methods=["POST"])
def webhook():
    """Thin, fast handler: validate cheaply and ENQUEUE, then return 200 at
    once so TradingView never waits on the (slow, signed) Hyperliquid order.
    All real work happens in signal_worker. Falls back to synchronous
    processing if ASYNC_QUEUE_ENABLED is off."""
    try:
        data = request.json or json.loads(request.data.decode("utf-8"))
    except Exception:
        return jsonify({"error": "bad json"}), 400

    log.info(f"📩 Webhook: {json.dumps(data)}")

    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    if data.get("action", "").lower() not in ("buy", "sell"):
        return jsonify({"status": "rejected", "reason": "invalid action"}), 200
    if not data.get("symbol", ""):
        return jsonify({"status": "rejected", "reason": "missing symbol"}), 200

    if ASYNC_QUEUE_ENABLED:
        _signal_queue.put((data, time.time()))
        depth = _signal_queue.qsize()
        log.info(f"📥 Queued {data.get('type', 'entry')} {data.get('symbol')} (queue depth={depth})")
        return jsonify({"status": "queued", "depth": depth}), 200

    # Synchronous fallback (runs in this request's context, so jsonify is fine)
    process_signal(data, time.time())
    return jsonify({"status": "processed"}), 200


def signal_worker():
    """Background thread: drains the signal queue one job at a time. Serial by
    design — keeps active_trades coherent and avoids races. Runs each job in an
    app context so the reused process_signal() body can call jsonify() (its
    return values are discarded here)."""
    log.info(f"⚙️ Async signal worker started — enabled={ASYNC_QUEUE_ENABLED}, "
             f"max_job_age={MAX_JOB_AGE_SEC}s")
    while True:
        try:
            data, enqueued_at = _signal_queue.get()
            try:
                with app.app_context():
                    process_signal(data, enqueued_at)
            except Exception as e:
                log.error(f"signal_worker process error: {e}", exc_info=True)
            finally:
                _signal_queue.task_done()
        except Exception as e:
            log.error(f"signal_worker loop error: {e}", exc_info=True)
            time.sleep(0.5)


def process_signal(data, enqueued_at):
    """The actual decision logic (gates + entry/book/reverse/close). Pulled out
    of the route so it can run in the background worker. Its jsonify() returns
    are only meaningful in the synchronous fallback; in the worker they're
    discarded. All real outcomes are logged via log_trade_event / log."""
    try:
        action = data.get("action", "").lower()
        raw_symbol = data.get("symbol", "")
        symbol = coin_from_symbol(raw_symbol)           # HL coin name
        alert_type = data.get("type", "entry").lower()
        leverage = int(data.get("leverage", DEFAULT_LEVERAGE))
        margin_ccy = "USDC"
        coin_price = float(data.get("price", 0))

        tp_price = float(data.get("tp_price", 0)) if data.get("tp_price") else None
        sl_price = float(data.get("sl_price", 0)) if data.get("sl_price") else None
        book_pct = float(data.get("book_pct", 33)) / 100

        if action not in ("buy", "sell"):
            return jsonify({"status": "rejected", "reason": "invalid action"}), 200
        if not symbol:
            return jsonify({"status": "rejected", "reason": "missing symbol"}), 200

        # ─── STALENESS GUARD (entries only) ───────────────────
        # If a burst backed up the queue, an old ENTRY is acting on a price
        # that has since moved — skip it. Books/reverses/closes still run.
        age = time.time() - enqueued_at
        if MAX_JOB_AGE_SEC > 0 and age > MAX_JOB_AGE_SEC and alert_type == "entry":
            log.info(f"⏱ STALE entry skipped: {symbol} ({age:.1f}s old > {MAX_JOB_AGE_SEC}s)")
            log_trade_event(symbol, action, "entry", "STALE", f"{age:.1f}s old")
            return jsonify({"status": "stale", "age_sec": round(age, 1)}), 200

        leverage = clamp_leverage(symbol, leverage)

        # ─── DAILY CAP HARD-STOP GATE ─────────────────────────
        if daily_cap_active():
            log.info(f"🛑 DAILY CAP: rejecting {alert_type} for {symbol} "
                     f"(cumulative {_daily_locked_pct_sum:.2f}% / cap {DAILY_CAP_PCT}%)")
            log_trade_event(symbol, action, alert_type, "DAILY_CAP", f"sum={_daily_locked_pct_sum:.2f}%")
            return jsonify({"status": "rejected",
                            "reason": f"daily cap reached ({_daily_locked_pct_sum:.2f}% ≥ {DAILY_CAP_PCT}%)"}), 200

        # ─── STREAK PAUSE GATE ────────────────────────────────
        # Pause triggered by N consecutive profit-locks. Auto-resumes (with
        # counter reset) once STREAK_PAUSE_SEC elapses.
        _maybe_resume_after_streak_pause()
        if streak_pause_active():
            remaining = streak_pause_remaining_sec()
            log.info(f"📈 STREAK PAUSE: rejecting {alert_type} for {symbol} — {remaining}s left "
                     f"(streak {_streak_count}/{STREAK_THRESHOLD})")
            log_trade_event(symbol, action, alert_type, "STREAK_PAUSE",
                            f"{remaining}s remaining, streak={_streak_count}")
            return jsonify({"status": "rejected",
                            "reason": f"streak pause ({remaining}s)"}), 200

        # ─── PROFIT-LOCK COOLDOWN GATE ────────────────────────
        if in_profit_lock_cooldown():
            remaining = cooldown_remaining_sec()
            log.info(f"🔒 COOLDOWN: rejecting {alert_type} for {symbol} — {remaining}s left")
            log_trade_event(symbol, action, alert_type, "COOLDOWN", f"{remaining}s remaining")
            return jsonify({"status": "rejected", "reason": f"profit-lock cooldown ({remaining}s)"}), 200

        # ─── BASELINE COOLDOWN GATE ───────────────────────────
        if in_baseline_cooldown() and alert_type in ("entry", "reverse"):
            remaining = baseline_cooldown_remaining_sec()
            log.info(f"📐 BASELINE COOLDOWN: rejecting {alert_type} for {symbol} — {remaining}s left")
            log_trade_event(symbol, action, alert_type, "BASELINE_COOLDOWN", f"{remaining}s remaining")
            return jsonify({"status": "rejected", "reason": f"baseline cooldown ({remaining}s)"}), 200

        # ─── SL LOCKOUT GATE ──────────────────────────────────
        if alert_type in ("entry", "reverse"):
            locked, remaining = in_sl_lockout(symbol)
            if locked:
                log.info(f"🚫 SL LOCKOUT: rejecting {alert_type} for {symbol} — {remaining}s left")
                log_trade_event(symbol, action, alert_type, "SL_LOCKOUT", f"{remaining}s remaining")
                return jsonify({"status": "rejected",
                                "reason": f"SL lockout on {symbol} ({remaining}s)"}), 200

        # ─── TARGET CURRENT VALUE GATE ────────────────────────
        if TARGET_ENABLED and _target_hit and alert_type in ("entry", "reverse"):
            cur = _target_last_value
            cur_str = f"{cur:.2f} USDC" if cur is not None else "unknown"
            log.info(f"🎯 TARGET HIT: rejecting {alert_type} for {symbol} — "
                     f"current={cur_str} >= target={_target_value:.2f} USDC")
            log_trade_event(symbol, action, alert_type, "TARGET_HIT",
                            f"current={cur_str} target={_target_value:.2f}")
            return jsonify({"status": "rejected",
                            "reason": f"target {_target_value:.2f} USDC reached (current={cur_str})"}), 200

        # ─── ENTRY ────────────────────────────────────────────
        if alert_type == "entry":
            if symbol in active_trades:
                trade = active_trades[symbol]
                mins = int((time.time() - trade["entry_time"]) // 60)
                log.info(f"🚫 SKIP entry: {symbol} already active ({mins}m, {trade['side']})")
                log_trade_event(symbol, action, "entry", "SKIP", f"already active ({mins}m)")
                return jsonify({"status": "skipped", "reason": "already active"}), 200

            quantity = calc_quantity(symbol, coin_price, leverage)
            if quantity <= 0:
                log.error(f"❌ REJECT: {symbol} — qty=0")
                log_trade_event(symbol, action, "entry", "REJECT", "qty=0")
                return jsonify({"status": "rejected", "reason": "qty=0"}), 200

            mode = "PAPER" if PAPER_MODE else "LIVE"
            log.info(f"🚀 ENTRY [{mode}] {action.upper()} {quantity} {symbol} ({leverage}x) "
                     f"| TP={tp_price} SL={sl_price}")
            result = place_market(symbol, action, quantity, leverage,
                                  reduce_only=False, price_hint=coin_price)

            if isinstance(result, dict) and result.get("status") == "error":
                err = result.get("message", "")
                log.error(f"❌ REJECT: {symbol} — {err}")
                log_trade_event(symbol, action, "entry", "REJECT", err)
                return jsonify({"status": "rejected", "reason": err}), 200

            order_id = result.get("id", "unknown")
            filled_qty = float(result.get("total_quantity", quantity) or quantity)
            entry_px = float(result.get("avg_price") or coin_price)
            set_active_trade(symbol, action, filled_qty, entry_px, order_id,
                             tp_price=tp_price, sl_price=sl_price,
                             leverage=leverage, margin_ccy=margin_ccy)
            log_trade_event(symbol, action, "entry", "FILLED", f"TP={tp_price} SL={sl_price}")

            if sl_price:
                place_native_sl(symbol, action, filled_qty, sl_price, leverage, margin_ccy)

            return jsonify({"status": "success", "order": result}), 200

        # ─── BOOK ─────────────────────────────────────────────
        elif alert_type == "book":
            if symbol not in active_trades:
                log.info(f"⚠️ SKIP book: {symbol} not tracked")
                log_trade_event(symbol, action, "book", "SKIP", "not tracked")
                return jsonify({"status": "skipped"}), 200

            trade = active_trades[symbol]
            book_qty = round_down_quantity(symbol, trade["original_qty"] * book_pct)
            book_qty = min(book_qty, trade["qty"])
            if book_qty <= 0:
                log.info(f"⚠️ SKIP book: {symbol} — book qty too small")
                return jsonify({"status": "skipped", "reason": "book qty too small"}), 200

            cancel_native_sl(symbol)
            log.info(f"📦 BOOK #{trade['books_done']+1}: closing {book_qty} of {trade['qty']} {symbol}")
            result = place_market(symbol, _close_side(trade), book_qty,
                                  trade.get("leverage", leverage), reduce_only=True,
                                  price_hint=coin_price)

            if isinstance(result, dict) and result.get("status") == "error":
                err = result.get("message", "")
                log.error(f"❌ Book failed: {symbol} — {err}")
                log_trade_event(symbol, action, "book", "REJECT", err)
                if trade.get("sl_price"):
                    place_native_sl(symbol, trade["side"], trade["qty"], trade["sl_price"], leverage, margin_ccy)
                return jsonify({"status": "rejected", "reason": err}), 200

            trade["qty"] -= book_qty
            trade["books_done"] += 1
            if tp_price:
                trade["tp_price"] = tp_price
            if sl_price:
                trade["sl_price"] = sl_price
            _save_active_trades()

            record_realized(symbol, trade["entry_price"], coin_price, book_qty, trade["side"])

            log.info(f"✅ Booked {book_qty} {symbol} — remaining: {trade['qty']:.6f} "
                     f"| new TP={tp_price} SL={sl_price}")
            log_trade_event(symbol, action, "book", "FILLED",
                            f"book #{trade['books_done']}, remaining={trade['qty']:.6f}")

            if trade["qty"] > 0 and sl_price:
                place_native_sl(symbol, trade["side"], trade["qty"], sl_price, leverage, margin_ccy)

            return jsonify({"status": "booked", "remaining_qty": trade["qty"]}), 200

        # ─── REVERSE ──────────────────────────────────────────
        elif alert_type == "reverse":
            cancel_native_sl(symbol)
            if symbol in active_trades:
                trade = active_trades[symbol]
                close_qty = trade["qty"]
                if close_qty > 0:
                    log.info(f"🔻 REVERSE close: {close_qty} {symbol}")
                    close_result = place_market(symbol, _close_side(trade), close_qty,
                                                trade.get("leverage", leverage), reduce_only=True,
                                                price_hint=coin_price)
                    if isinstance(close_result, dict) and close_result.get("status") == "error":
                        log.warning(f"⚠️ Reverse close failed (likely already closed): "
                                    f"{close_result.get('message','')}")
                record_realized(symbol, trade["entry_price"], coin_price, close_qty, trade["side"])
                clear_active_trade(symbol, "reverse — SL hit")
                log_trade_event(symbol, _close_side(trade), "reverse_close", "FILLED", "Pine reverse")

            time.sleep(1)

            quantity = calc_quantity(symbol, coin_price, leverage)
            if quantity <= 0:
                log.error(f"❌ REJECT reverse entry: {symbol} — qty=0")
                return jsonify({"status": "rejected", "reason": "reverse entry qty=0"}), 200

            log.info(f"🔄 REVERSE entry: {action.upper()} {quantity} {symbol} | TP={tp_price} SL={sl_price}")
            result = place_market(symbol, action, quantity, leverage,
                                  reduce_only=False, price_hint=coin_price)
            if isinstance(result, dict) and result.get("status") == "error":
                err = result.get("message", "")
                log.error(f"❌ Reverse entry failed: {err}")
                log_trade_event(symbol, action, "reverse_entry", "REJECT", err)
                return jsonify({"status": "rejected", "reason": err}), 200

            order_id = result.get("id", "unknown")
            filled_qty = float(result.get("total_quantity", quantity) or quantity)
            entry_px = float(result.get("avg_price") or coin_price)
            set_active_trade(symbol, action, filled_qty, entry_px, order_id,
                             tp_price=tp_price, sl_price=sl_price,
                             leverage=leverage, margin_ccy=margin_ccy)
            log_trade_event(symbol, action, "reverse_entry", "FILLED", f"TP={tp_price} SL={sl_price}")

            if sl_price:
                place_native_sl(symbol, action, filled_qty, sl_price, leverage, margin_ccy)

            return jsonify({"status": "reversed", "order": result}), 200

        # ─── CLOSE — kill switch / SL-wait close-only ─────────
        elif alert_type == "close":
            reason = data.get("reason", "unknown")
            ret_pct = data.get("return_pct", "?")
            if reason == "sl_wait":
                log.info(f"⏳ SL-WAIT close: {symbol}")
                if symbol in active_trades:
                    mark_sl_lockout(symbol, reason="Pine sl_wait close on tracked position")
                else:
                    log.info(f"⏳ SL-WAIT for {symbol} ignored for lockout — not in active_trades")
            elif reason == "tp_hit":
                log.info(f"✓ TP HIT: {symbol} — return={ret_pct}%")
            elif reason == "sl_hit":
                log.info(f"✗ SL HIT: {symbol} — return={ret_pct}%")
            elif reason == "timer_expired":
                log.info(f"⏱ TIMER EXIT: {symbol} — return={ret_pct}%")
            elif reason == "kill_switch":
                log.info(f"☠️ KILL SWITCH: {symbol} — return={ret_pct}%")
            else:
                log.info(f"☠️ CLOSE ({reason}): {symbol} — return={ret_pct}%")

            cancel_native_sl(symbol)

            if symbol in active_trades:
                trade = active_trades[symbol]
                close_qty = trade["qty"]
                if close_qty > 0:
                    reason_label = {"sl_wait": "SL-WAIT", "tp_hit": "TP", "sl_hit": "SL",
                                    "timer_expired": "TIMER", "kill_switch": "KILL"}.get(reason, reason.upper())
                    log.info(f"🔻 {reason_label} close: {close_qty} {symbol}")
                    result = place_market(symbol, _close_side(trade), close_qty,
                                          trade.get("leverage", leverage), reduce_only=True,
                                          price_hint=coin_price)
                    if isinstance(result, dict) and result.get("status") == "error":
                        log.warning(f"⚠️ Close may have failed: {result.get('message','')}")

                record_realized(symbol, trade["entry_price"], coin_price, close_qty, trade["side"])
                clear_active_trade(symbol, f"{reason}")
                log_trade_event(symbol, _close_side(trade),
                                reason if reason in ("sl_wait", "tp_hit", "sl_hit", "timer_expired", "kill_switch") else "close",
                                "FILLED", f"reason={reason}")
            else:
                log.info(f"⚠️ Close for {symbol} but not tracked — no action needed")
                log_trade_event(symbol, action,
                                reason if reason in ("sl_wait", "tp_hit", "sl_hit", "timer_expired", "kill_switch") else "close",
                                "SKIP", "not tracked")

            return jsonify({"status": "closed", "symbol": symbol, "reason": reason}), 200

        else:
            return jsonify({"status": "unknown_type", "type": alert_type}), 200

    except Exception as e:
        log.error(f"❌ Error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 200


# ═══════════════════════════════════════════════════════════
#  UTILITY ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.route("/status", methods=["GET"])
def status():
    pnl, margin, pct = compute_net_roe()
    pct_str = f"{pct:.2f}" if pct is not None else "unavailable"
    _check_and_reset_daily_counter()
    return jsonify({
        "mode": "paper" if PAPER_MODE else "live",
        "network": HL_NETWORK,
        "exchange_ready": client.ready(),
        "queue_depth": _signal_queue.qsize(),
        "async_enabled": ASYNC_QUEUE_ENABLED,
        "account_value_usdc": get_current_value_usdt(),
        "wallet_usdc": get_wallet_usdt(),
        "paper_realized_usdc": round(_paper_realized, 4) if PAPER_MODE else None,
        "active_trades": active_trades,
        "native_sl_orders": native_sl_orders,
        "positions": len(active_trades),
        "profit_lock": {
            "enabled": PROFIT_LOCK_ENABLED,
            "threshold_pct": PROFIT_LOCK_PCT,
            "current_net_pct": pct_str,
            "net_pnl_usdc": round(pnl, 2) if pnl is not None else None,
            "total_margin_usdc": round(margin, 2) if margin is not None else None,
            "in_cooldown": in_profit_lock_cooldown(),
            "cooldown_remaining_sec": cooldown_remaining_sec(),
        },
        "loss_lock": {
            "enabled": LOSS_LOCK_ENABLED,
            "threshold_pct": LOSS_LOCK_PCT,
            "trigger_at_net_pct": -LOSS_LOCK_PCT,
            "current_net_pct": pct_str,
            "would_trigger": (pct is not None and LOSS_LOCK_ENABLED and pct <= -LOSS_LOCK_PCT),
        },
        "daily_cap": {
            "enabled": DAILY_CAP_ENABLED,
            "cap_pct": DAILY_CAP_PCT,
            "cumulative_locked_pct": round(_daily_locked_pct_sum, 2),
            "lock_count_today": _daily_lock_count,
            "is_paused": _daily_paused,
            "ist_date": str(_daily_counter_date) if _daily_counter_date else None,
            "ist_now": datetime.now(IST_TZ).isoformat(),
        },
        "reconciler": {
            "enabled": RECONCILER_ENABLED,
            "poll_sec": RECONCILE_POLL_SEC,
            "adopt": RECONCILE_ADOPT,
            "last_pass": _reconcile_last,
        },
        "streak": {
            "enabled": STREAK_ENABLED,
            "count": _streak_count,
            "threshold": STREAK_THRESHOLD,
            "max_ever": _streak_max_ever,
            "paused": streak_pause_active(),
            "pause_remaining_sec": streak_pause_remaining_sec(),
            "pause_sec": STREAK_PAUSE_SEC,
            "last_lock_at": _streak_last_lock_at,
        },
        "time": datetime.now().isoformat()
    })

@app.route("/profit-lock/check", methods=["GET"])
def profit_lock_check():
    pnl, margin, pct = compute_net_roe()
    per_pos = {}
    if PAPER_MODE:
        mids = client.all_mids()
        for coin, t in active_trades.items():
            mark = mids.get(coin)
            entry = float(t.get("entry_price", 0) or 0)
            qty = float(t.get("qty", 0) or 0)
            d = 1 if t.get("side") == "buy" else -1
            per_pos[coin] = {
                "mark": mark, "entry": entry, "qty": qty,
                "unrealized_usdc": (round((mark - entry) * qty * d, 4) if mark else None),
            }
    else:
        poss = client.positions_live() or []
        for p in poss:
            if p.get("coin") in active_trades:
                per_pos[p["coin"]] = {
                    "unrealized_usdc": p.get("unrealized"),
                    "margin_used_usdc": p.get("margin_used"),
                    "entry": p.get("entry"),
                }
    return jsonify({
        "mode": "paper" if PAPER_MODE else "live",
        "net_pnl_usdc": pnl, "total_margin_usdc": margin, "net_pct": pct,
        "threshold_pct": PROFIT_LOCK_PCT,
        "would_trigger": (pct is not None and pct >= PROFIT_LOCK_PCT),
        "in_cooldown": in_profit_lock_cooldown(),
        "cooldown_remaining_sec": cooldown_remaining_sec(),
        "positions": per_pos,
    })

@app.route("/profit-lock/force", methods=["POST", "GET"])
def profit_lock_force():
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    if not active_trades:
        return jsonify({"status": "no_positions", "active": 0})
    count = len(active_trades)
    pnl, margin, pct = compute_net_roe()
    close_all_positions(trigger_reason="manual force", trigger_pct=(pct if pct is not None else 0.0), lock_kind="manual")
    return jsonify({"status": "locked", "closed": count, "trigger_pct": pct,
                    "cooldown_remaining_sec": cooldown_remaining_sec(),
                    "daily_cumulative_pct": _daily_locked_pct_sum,
                    "daily_paused": _daily_paused})

@app.route("/loss-lock/check", methods=["GET"])
def loss_lock_check():
    """Read-only view of the loss-lock drawdown stop. Does not trigger anything.
    For a manual close-all on a loss, use /profit-lock/force (it closes every
    position regardless of P&L direction)."""
    pnl, margin, pct = compute_net_roe()
    return jsonify({
        "enabled": LOSS_LOCK_ENABLED,
        "threshold_pct": LOSS_LOCK_PCT,
        "trigger_at_net_pct": -LOSS_LOCK_PCT,
        "current_net_pct": pct,
        "net_pnl_usdc": pnl,
        "total_margin_usdc": margin,
        "would_trigger": (pct is not None and LOSS_LOCK_ENABLED and pct <= -LOSS_LOCK_PCT),
        "in_cooldown": in_profit_lock_cooldown(),
        "cooldown_remaining_sec": cooldown_remaining_sec(),
    })

@app.route("/flatten", methods=["POST", "GET"])
def flatten():
    """Force-close EVERY open position on the exchange — including orphans the
    bot isn't tracking — then clear tracking. Use this when /status shows
    positions:0 but the exchange still has live positions (orphaned state).
    Unlike /profit-lock/force (which only closes active_trades), this sweeps
    the exchange directly. Requires ?secret=... . Does NOT set a cooldown."""
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    before = client.positions_live()
    before_n = len(before) if before is not None else None
    residual = _sweep_residual_positions()
    active_trades.clear()
    _save_active_trades()
    log.info(f"🧹 /flatten invoked — exchange positions before={before_n}, residual_after={residual}")
    return jsonify({
        "status": "flattened" if residual == 0 else "incomplete",
        "positions_before": before_n,
        "residual_after": residual,
        "note": ("exchange confirmed flat" if residual == 0 else
                 "could not verify flat — check exchange" if residual is None else
                 f"{residual} still open — retry or close manually"),
    })

@app.route("/daily-cap/reset", methods=["POST", "GET"])
def daily_cap_reset():
    global _daily_locked_pct_sum, _daily_lock_count, _daily_paused, _daily_counter_date
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    prev_sum, prev_count, prev_paused = _daily_locked_pct_sum, _daily_lock_count, _daily_paused
    _daily_locked_pct_sum = 0.0
    _daily_lock_count = 0
    _daily_paused = False
    _daily_counter_date = _current_ist_date()
    log.info(f"🔄 Daily cap manually reset (was: locks={prev_count}, sum={prev_sum:.2f}%, paused={prev_paused})")
    return jsonify({"status": "reset",
                    "previous": {"lock_count": prev_count, "cumulative_pct": round(prev_sum, 2),
                                 "was_paused": prev_paused},
                    "current": {"lock_count": 0, "cumulative_pct": 0.0, "is_paused": False,
                                "ist_date": str(_daily_counter_date)}})

@app.route("/daily-cap/check", methods=["GET"])
def daily_cap_check():
    _check_and_reset_daily_counter()
    return jsonify({
        "enabled": DAILY_CAP_ENABLED, "cap_pct": DAILY_CAP_PCT,
        "cumulative_locked_pct": round(_daily_locked_pct_sum, 2),
        "remaining_pct": round(max(0, DAILY_CAP_PCT - _daily_locked_pct_sum), 2),
        "lock_count_today": _daily_lock_count, "is_paused": _daily_paused,
        "ist_date": str(_daily_counter_date) if _daily_counter_date else None,
        "ist_now": datetime.now(IST_TZ).isoformat(),
    })

@app.route("/stats", methods=["GET"])
def stats():
    filled = [e for e in trade_log if e["result"] == "FILLED"]
    skipped = [e for e in trade_log if e["result"] == "SKIP"]
    rejected = [e for e in trade_log if e["result"] == "REJECT"]
    return jsonify({
        "active_trades": active_trades, "positions": len(active_trades),
        "summary": {"filled": len(filled), "skipped": len(skipped), "rejected": len(rejected)},
        "recent_events": list(reversed(trade_log[-20:])),
        "time": datetime.now().isoformat()
    })

@app.route("/clear-lock", methods=["POST"])
def clear_lock():
    symbol = request.args.get("symbol") or (request.get_json(silent=True) or {}).get("symbol")
    if not symbol:
        return jsonify({"status": "error",
                        "reason": "symbol required — pass ?symbol=BTC. Mass-clear is not supported."}), 400
    symbol = coin_from_symbol(symbol)
    was_tracked = symbol in active_trades
    clear_active_trade(symbol, "manual clear via /clear-lock")
    log.info(f"🧹 /clear-lock invoked for {symbol} (was_tracked={was_tracked})")
    return jsonify({"status": "ok", "cleared": symbol, "was_tracked": was_tracked})


@app.route("/clear-tracking", methods=["GET", "POST"])
def clear_tracking():
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"status": "error", "reason": "invalid or missing secret"}), 403
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"status": "error", "reason": "symbol required (e.g. ?symbol=BTC)"}), 400
    symbol = coin_from_symbol(symbol)
    was_tracked = symbol in active_trades
    cancel_sl = request.args.get("cancel_sl", "").lower() == "true"
    if was_tracked:
        active_trades.pop(symbol, None)
        _save_active_trades()
        if cancel_sl:
            cancel_native_sl(symbol)
            log.info(f"🧹 /clear-tracking: {symbol} — cleared tracking + canceled native SL")
        else:
            log.info(f"🧹 /clear-tracking: {symbol} — cleared tracking only (native SL preserved)")
    else:
        log.info(f"🧹 /clear-tracking: {symbol} — no-op (not currently tracked)")
    return jsonify({"status": "ok", "symbol": symbol, "was_tracked": was_tracked,
                    "sl_canceled": cancel_sl and was_tracked})


# ═══════════════════════════════════════════════════════════
#  SL LOCKOUT ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.route("/sl-lockout/status", methods=["GET"])
def sl_lockout_status():
    now = time.time()
    locked = {}
    for sym, until in list(_sl_lockout.items()):
        remaining = int(until - now)
        if remaining > 0:
            locked[sym] = remaining
        else:
            _sl_lockout.pop(sym, None)
    return jsonify({"enabled": SL_LOCKOUT_ENABLED, "duration_sec": SL_LOCKOUT_SEC,
                    "check_interval_sec": SL_LOCKOUT_CHECK_SEC,
                    "locked_count": len(locked), "locked": locked}), 200


@app.route("/sl-lockout/clear", methods=["POST", "GET"])
def sl_lockout_clear():
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    symbol = request.args.get("symbol")
    if symbol:
        symbol = coin_from_symbol(symbol)
        existed = _sl_lockout.pop(symbol, None) is not None
        if existed:
            log.info(f"🔓 SL LOCKOUT manually cleared: {symbol}")
        return jsonify({"status": "ok", "cleared": symbol if existed else None})
    count = len(_sl_lockout)
    _sl_lockout.clear()
    if count:
        log.info(f"🔓 SL LOCKOUT manually cleared all ({count} symbols)")
    return jsonify({"status": "ok", "cleared_count": count})


# ═══════════════════════════════════════════════════════════
#  TARGET CURRENT VALUE ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.route("/target/status", methods=["GET"])
def target_status():
    return jsonify({
        "enabled": TARGET_ENABLED, "target_value_usdc": _target_value,
        "hit": _target_hit, "hit_at": _target_hit_at,
        "last_wallet_value_usdc": _target_last_value,
        "last_check_at": _target_last_check_at, "last_error": _target_last_error,
        "poll_sec": TARGET_POLL_SEC,
        "distance_to_target": (None if _target_last_value is None or _target_value is None
                               or _target_value <= 0 else round(_target_value - _target_last_value, 2)),
    }), 200


@app.route("/target/set", methods=["POST", "GET"])
def target_set():
    global _target_value, _target_hit, _target_hit_at
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    raw = request.args.get("value")
    if raw is None:
        return jsonify({"error": "value required, e.g. /target/set?value=2400&secret=..."}), 400
    try:
        new_value = float(raw)
    except ValueError:
        return jsonify({"error": f"value must be numeric, got: {raw}"}), 400
    if new_value < 0:
        return jsonify({"error": "value must be >= 0 (use 0 to disable)"}), 400
    old_value, old_hit = _target_value, _target_hit
    _target_value = new_value
    cur = _target_last_value
    if cur is None or new_value > cur:
        _target_hit = False
        _target_hit_at = None
    save_target_state()
    log.info(f"🎯 TARGET SET: {old_value} -> {new_value} USDC | hit: {old_hit} -> {_target_hit}")
    return jsonify({"status": "ok", "old_value": old_value, "new_value": new_value,
                    "hit": _target_hit, "last_wallet_value": cur}), 200


@app.route("/target/clear", methods=["POST", "GET"])
def target_clear():
    global _target_value, _target_hit, _target_hit_at
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    old_value = _target_value
    _target_value = 0.0
    _target_hit = False
    _target_hit_at = None
    save_target_state()
    log.info(f"🎯 TARGET CLEARED: was {old_value} -> 0 (disabled)")
    return jsonify({"status": "ok", "old_value": old_value, "new_value": 0.0}), 200


# ═══════════════════════════════════════════════════════════
#  BASELINE ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.route("/baseline/status", methods=["GET"])
def baseline_status():
    if _baseline_usdt is None:
        return jsonify({"enabled": BASELINE_ENABLED, "baseline": None,
                        "message": "Baseline not set. Call /baseline/set?value=N&secret=... to initialize.",
                        "trigger_pct": BASELINE_TRIGGER_PCT, "rollover_pct": BASELINE_ROLLOVER_PCT,
                        "cooldown_sec": BASELINE_COOLDOWN_SEC}), 200
    pnl_open, _, _ = compute_net_roe()
    pnl_open_safe = pnl_open if pnl_open is not None else 0.0
    current_equity = _baseline_usdt + _baseline_realized_pnl + pnl_open_safe
    target = _baseline_usdt * (1 + BASELINE_TRIGGER_PCT / 100)
    next_target_after_lock = (_baseline_usdt * (1 + BASELINE_ROLLOVER_PCT / 100)) * (1 + BASELINE_TRIGGER_PCT / 100)
    return jsonify({
        "enabled": BASELINE_ENABLED, "baseline_usdc": round(_baseline_usdt, 2),
        "realized_pnl_since_baseline": round(_baseline_realized_pnl, 2),
        "unrealized_pnl_open": round(pnl_open_safe, 2) if pnl_open is not None else "unavailable",
        "current_equity": round(current_equity, 2), "trigger_pct": BASELINE_TRIGGER_PCT,
        "target_value": round(target, 2),
        "pct_to_target": round((current_equity / target - 1) * 100, 2),
        "rollover_pct": BASELINE_ROLLOVER_PCT,
        "next_baseline_after_lock": round(_baseline_usdt * (1 + BASELINE_ROLLOVER_PCT / 100), 2),
        "next_target_after_lock": round(next_target_after_lock, 2),
        "in_cooldown": in_baseline_cooldown(),
        "cooldown_remaining_sec": baseline_cooldown_remaining_sec(),
        "lock_count": _baseline_lock_count, "last_lock_at": _baseline_last_lock_at,
        "history_count": len(_baseline_history), "history_recent": _baseline_history[-5:],
    }), 200


@app.route("/baseline/set", methods=["POST", "GET"])
def baseline_set():
    if request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    raw = request.args.get("value")
    if raw is None:
        return jsonify({"error": "missing 'value' query param (e.g. ?value=2000)"}), 400
    try:
        value = float(raw)
        if value <= 0:
            raise ValueError("must be positive")
    except ValueError:
        return jsonify({"error": f"invalid value '{raw}'"}), 400
    global _baseline_usdt, _baseline_realized_pnl
    old = _baseline_usdt
    _baseline_usdt = value
    _baseline_realized_pnl = 0.0
    save_baseline_state()
    log.info(f"📐 Baseline manually set: {old} → {value} USDC (realized P&L reset to 0)")
    return jsonify({"status": "set", "baseline_usdc": value, "previous_baseline": old,
                    "trigger_value": round(value * (1 + BASELINE_TRIGGER_PCT / 100), 2),
                    "trigger_pct": BASELINE_TRIGGER_PCT}), 200


@app.route("/baseline/reset", methods=["POST", "GET"])
def baseline_reset():
    if request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    global _baseline_usdt, _baseline_realized_pnl, _baseline_lock_count
    global _baseline_last_lock_at, _baseline_history, _baseline_cooldown_until
    _baseline_usdt = None
    _baseline_realized_pnl = 0.0
    _baseline_lock_count = 0
    _baseline_last_lock_at = None
    _baseline_history = []
    _baseline_cooldown_until = 0.0
    save_baseline_state()
    log.info("📐 Baseline RESET — uninitialized, all history cleared")
    return jsonify({"status": "reset"}), 200


@app.route("/reconcile/status", methods=["GET"])
def reconcile_status():
    """Read-only: config, last pass summary, and current live drift (orphans /
    ghosts) computed fresh without acting. No auth needed."""
    info = {"enabled": RECONCILER_ENABLED, "poll_sec": RECONCILE_POLL_SEC,
            "grace_sec": RECONCILE_GRACE_SEC, "adopt": RECONCILE_ADOPT,
            "clear_ghosts": RECONCILE_CLEAR_GHOSTS, "last_pass": _reconcile_last}
    if not PAPER_MODE:
        poss = client.positions_live()
        if poss is not None:
            ex = {p["coin"] for p in poss if p.get("coin") and float(p.get("qty") or 0) > 0}
            tracked = set(active_trades.keys())
            info["current_orphans"] = sorted(ex - tracked)
            info["current_ghosts"] = sorted(tracked - ex)
            info["in_sync"] = (ex == tracked)
        else:
            info["error"] = "position fetch failed"
    return jsonify(info), 200


@app.route("/reconcile/run", methods=["POST", "GET"])
def reconcile_run():
    """Force an immediate reconciliation pass (adopts orphans / clears ghosts
    per config). Requires ?secret=... ."""
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify({"status": "ok", "result": reconcile_once()}), 200


@app.route("/streak/status", methods=["GET"])
def streak_status_endpoint():
    """Read-only view of the consecutive-profit-lock streak and any active pause."""
    _maybe_resume_after_streak_pause()
    return jsonify({
        "enabled": STREAK_ENABLED,
        "count": _streak_count,
        "threshold": STREAK_THRESHOLD,
        "max_ever": _streak_max_ever,
        "paused": streak_pause_active(),
        "pause_remaining_sec": streak_pause_remaining_sec(),
        "pause_sec": STREAK_PAUSE_SEC,
        "last_lock_at": _streak_last_lock_at,
    }), 200


@app.route("/streak/reset", methods=["POST", "GET"])
def streak_reset():
    """Manually clear the streak counter and lift any active streak pause.
    Requires ?secret=... ."""
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    global _streak_count, _streak_pause_until
    prev_count = _streak_count
    was_paused = streak_pause_active()
    _streak_count = 0
    _streak_pause_until = 0.0
    save_streak_state()
    log.info(f"🔄 Streak manually reset (was count={prev_count}, paused={was_paused})")
    return jsonify({
        "status": "reset",
        "previous_count": prev_count,
        "was_paused": was_paused,
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "mode": "paper" if PAPER_MODE else "live",
                    "positions": len(active_trades), "active": list(active_trades.keys()),
                    "time": datetime.now().isoformat()})


# ─── Startup ──────────────────────────────────────────────────
try:
    client.load_meta()          # szDecimals / tick sizes / max leverage
except Exception as e:
    log.warning(f"meta preload failed (will lazy-load on first use): {e}")

_load_active_trades()
log.info(f"🤖 Trail TP/SL Rev Bot ready — Hyperliquid {HL_NETWORK} | "
         f"mode={'PAPER' if PAPER_MODE else 'LIVE'} | exchange_ready={client.ready()}")

_check_and_reset_daily_counter()
log.info(f"📅 Daily cap initialized — IST date {_daily_counter_date}, "
         f"cap={DAILY_CAP_PCT}%, enabled={DAILY_CAP_ENABLED}")

load_baseline_state()
log.info(f"📐 Baseline initialized — enabled={BASELINE_ENABLED}, "
         f"current={_baseline_usdt if _baseline_usdt is not None else 'unset'} USDC, "
         f"trigger={BASELINE_TRIGGER_PCT}%, rollover={BASELINE_ROLLOVER_PCT}%")

threading.Thread(target=profit_lock_worker, daemon=True).start()
threading.Thread(target=baseline_worker, daemon=True).start()
threading.Thread(target=sl_lockout_worker, daemon=True).start()

load_target_state()
log.info(f"🎯 Target initialized — enabled={TARGET_ENABLED}, "
         f"value={_target_value} USDC, hit={_target_hit}, poll={TARGET_POLL_SEC}s")
threading.Thread(target=target_worker, daemon=True).start()

# Load streak state from disk (persists across redeploys if volume mounted).
load_streak_state()
log.info(f"📈 Streak initialized — enabled={STREAK_ENABLED}, count={_streak_count}, "
         f"threshold={STREAK_THRESHOLD}, pause={STREAK_PAUSE_SEC}s")

# Start the position reconciler (self-gates on RECONCILER_ENABLED + live mode).
threading.Thread(target=reconcile_worker, daemon=True).start()

# Start the async signal worker (drains the webhook queue serially).
if ASYNC_QUEUE_ENABLED:
    threading.Thread(target=signal_worker, daemon=True).start()
    log.info(f"⚙️ Async webhook queue ENABLED — max_job_age={MAX_JOB_AGE_SEC}s")
else:
    log.info("⚙️ Async webhook queue DISABLED — processing synchronously")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
