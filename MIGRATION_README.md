# CoinDCX → Hyperliquid port

Same brain, new exchange. All risk subsystems (profit-lock, daily cap, SL
lockout, baseline rollover, target current-value) and the Pine webhook contract
are preserved. Only the exchange surface, denomination (INR → USDC), and symbol
format (`B-BTC_USDT` → `BTC`) changed.

## Files
- `server2.py` — Flask webhook brain (ported)
- `hyperliquid_client.py` — exchange adapter over the official `hyperliquid-python-sdk`
- `requirements.txt`, `Procfile`, `runtime.txt`
- `.env.example` — every env var with notes
- `pine_hyperliquid_patch.txt` — optional 2-line Pine tweak

## Auth (Hyperliquid has no API key/secret)
1. Go to https://app.hyperliquid.xyz/API and **Authorize API Wallet**.
2. Set env vars (Railway → Variables, never in code):
   - `HL_SECRET_KEY` = the API/agent wallet **private** key (0x…). It can trade
     but **cannot withdraw**, so a leak is bounded. Do not paste it in chat.
   - `HL_ACCOUNT_ADDRESS` = your **main** wallet **public** address (0x…).
3. Fund the main wallet with USDC on Hyperliquid.

## Deploy (Railway)
1. Push these files to the repo.
2. Add a **Volume** mounted at `/app/data` (persists active trades / target /
   baseline across restarts).
3. Set env vars from `.env.example`. Keep `PAPER_MODE=true` to start.
4. Point your TradingView alert webhook at `https://<your-app>/webhook`.

## Paper → live (your standing rule: paper ≥ 2 weeks first)
- **Paper** (default): `PAPER_MODE=true`. No orders are sent; no key needed.
  The full brain runs against live Hyperliquid mid-prices. Watch `/status`,
  `/profit-lock/check`, `/target/status`. `PAPER_START_USDT` seeds the sim wallet.
- **Live**: set `PAPER_MODE=false` **and** add `HL_SECRET_KEY` +
  `HL_ACCOUNT_ADDRESS`. Start on `HL_NETWORK=testnet` for a live-wiring smoke
  test if you want, then `mainnet`.

## Sizing
USDC-native: `notional = FIXED_MARGIN_USDT × WALLET_USAGE_PCT × leverage`,
`qty = notional / price` floored to the coin's szDecimals. Orders below
`MIN_NOTIONAL_USDT` (HL's ~$10 minimum) are rejected.

## What changed vs CoinDCX, briefly
- P&L / equity now come from one `user_state` call (live) or live mids (paper) —
  no per-symbol mark polling, no INR rate.
- `TARGET_CURRENT_VALUE` is in USDC and tracks banked balance (equity minus
  floating P&L) — same "banked money" intent as before.
- Native SL is a real reduce-only HL trigger order, but stays **off by default**
  (`NATIVE_SL_ENABLED=false`) so Pine keeps driving SL exactly as today.
- Single gunicorn worker (state + background threads live in one process).

## Endpoints (unchanged set)
`/webhook` · `/status` · `/health` · `/stats` · `/profit-lock/{check,force}` ·
`/daily-cap/{check,reset}` · `/sl-lockout/{status,clear}` ·
`/target/{status,set,clear}` · `/baseline/{status,set,reset}` ·
`/clear-lock` · `/clear-tracking`
