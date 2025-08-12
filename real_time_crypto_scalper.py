#!/usr/bin/env python3
"""
Synchronous WebSocket scalper for Alpaca (paper). 

- Streams 1-min crypto bars via CryptoDataStream in a background thread
- Maintains an in-memory 1-min bar buffer (pandas DataFrame)
- Computes RSI(14) and MA(20)
- Submits bracket market orders (entry + stop loss + take profit) on signals
- Avoids nested asyncio by running stream.run() inside a thread with its own event loop
"""

import os
import time
import threading
from decimal import Decimal, ROUND_DOWN
from collections import defaultdict

import pandas as pd
import ta

from alpaca.data.live import CryptoDataStream
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# ---------------- CONFIG ----------------
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Watchlist / symbol
SYMBOL = "BTC/USD"

# Indicators
RSI_PERIOD = 14
MA_PERIOD = 20

# Order sizing
ORDER_QTY = 0.001  # fractional crypto size (example)
# Risk management
STOP_LOSS_PCT = 0.01   # 1%
TAKE_PROFIT_PCT = 0.02 # 2%

# Keep this many 1-min closes for indicators (should be >= MA period & RSI period)
MAX_BARS = 500

# ----------------------------------------

# Alpaca trading client (sync)
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)

# In-memory bar storage (timestamp, close) as pandas DataFrame
bars_df = pd.DataFrame(columns=["timestamp", "close"])

# Simple in-memory cooldown/position tracking
last_order_time = defaultdict(lambda: 0.0)   # symbol -> timestamp of last order
cooldown_seconds = 30.0                      # don't place more than one order per symbol this often

# --------- Helpers ---------
def round_price(price, decimals=4):
    d = Decimal(str(price))
    return float(d.quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_DOWN))

def compute_rsi_from_series(series: pd.Series, period: int = 14):
    if len(series) < period + 1:
        return None
    return ta.momentum.RSIIndicator(series, window=period).rsi().iloc[-1]

def compute_ma_from_series(series: pd.Series, period: int = 20):
    if len(series) < period:
        return None
    return series.rolling(window=period).mean().iloc[-1]

def has_open_position(symbol: str) -> bool:
    """
    Returns True if there's an open position for the symbol.
    Uses TradingClient.get_all_positions to avoid exceptions.
    """
    try:
        positions = trading_client.get_all_positions()
        for p in positions:
            # positions come as objects with .symbol property
            if getattr(p, "symbol", None) == symbol.replace("/", "") or getattr(p, "symbol", None) == symbol:
                return True
    except Exception as e:
        print("Warning: get_all_positions failed:", e)
    return False

def place_bracket_order(symbol: str, qty: float, side: OrderSide, entry_price: float):
    """
    Submits a market bracket order with SL and TP (paper trading).
    side: OrderSide.BUY or OrderSide.SELL
    entry_price: last close used to calculate SL/TP levels
    """
    stop_loss_pct = STOP_LOSS_PCT
    take_profit_pct = TAKE_PROFIT_PCT

    if side == OrderSide.BUY:
        sl_price = round_price(entry_price * (1 - stop_loss_pct))
        tp_price = round_price(entry_price * (1 + take_profit_pct))
    else:  # SELL
        sl_price = round_price(entry_price * (1 + stop_loss_pct))
        tp_price = round_price(entry_price * (1 - take_profit_pct))

    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=side,
        time_in_force=TimeInForce.GTC,
        order_class="bracket",
        take_profit=dict(limit_price=str(tp_price)),
        stop_loss=dict(stop_price=str(sl_price)),
    )
    try:
        resp = trading_client.submit_order(order)
        print(f"Submitted {side} bracket order for {symbol} qty={qty} entry={entry_price:.2f} sl={sl_price} tp={tp_price}")
        return resp
    except Exception as e:
        print("Order submission failed:", e)
        return None

# --------- Signal logic ---------
def check_and_maybe_trade(symbol: str):
    """
    Calculate RSI and MA from the current bars_df and decide buys/sells.
    """
    global bars_df, last_order_time

    if bars_df.empty:
        return

    closes = bars_df["close"].astype(float)
    rsi = compute_rsi_from_series(closes, RSI_PERIOD)
    ma = compute_ma_from_series(closes, MA_PERIOD)
    last_price = float(closes.iloc[-1])

    if rsi is None or ma is None:
        # not enough data
        return

    print(f"[{bars_df['timestamp'].iloc[-1]}] {symbol} price={last_price:.2f} RSI={rsi:.2f} MA={ma:.2f}")

    # simple cooldown check
    now_ts = time.time()
    if now_ts - last_order_time[symbol] < cooldown_seconds:
        # recently placed an order for this symbol -> skip
        return

    # only place orders if no existing position
    if rsi < 30.0 and last_price > ma:
        if not has_open_position(symbol):
            print("Signal -> BUY")
            resp = place_bracket_order(symbol, ORDER_QTY, OrderSide.BUY, last_price)
            if resp is not None:
                last_order_time[symbol] = now_ts
        else:
            print("Buy signal but already have an open position; skipping.")
    elif rsi > 70.0 and last_price < ma:
        if not has_open_position(symbol):
            print("Signal -> SELL")
            resp = place_bracket_order(symbol, ORDER_QTY, OrderSide.SELL, last_price)
            if resp is not None:
                last_order_time[symbol] = now_ts
        else:
            print("Sell signal but already have an open position; skipping.")

# --------- WebSocket handler (synchronous-friendly) ---------
def on_bars(bar):
    """
    Called by CryptoDataStream subscriber when a new 1-min bar arrives.
    bar : object with fields like .timestamp, .close, ...
    """
    global bars_df
    ts = getattr(bar, "timestamp", None)
    close = float(getattr(bar, "close", float(getattr(bar, "c", 0.0))))

    new_row = {"timestamp": ts, "close": close}
    bars_df = pd.concat([bars_df, pd.DataFrame([new_row])], ignore_index=True).tail(MAX_BARS)
    bars_df.reset_index(drop=True, inplace=True)

    try:
        check_and_maybe_trade(SYMBOL)
    except Exception as e:
        print("Signal handling error:", e)

# --------- Stream thread with own asyncio loop ---------
def stream_thread():
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stream = CryptoDataStream(API_KEY, API_SECRET)
    stream.subscribe_bars(on_bars, SYMBOL)

    print("Starting CryptoDataStream (asyncio loop in thread)...")
    try:
        loop.run_until_complete(stream.run())
    except Exception as e:
        print("Stream stopped with exception:", e)
    finally:
        loop.close()

# --------- Main ---------
def main():
    if not API_KEY or not API_SECRET:
        print("Please set APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables.")
        return

    print("Launching stream thread...")
    t = threading.Thread(target=stream_thread, daemon=True)
    t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping... (Ctrl+C)")
        print("Exiting.")

if __name__ == "__main__":
    main()
