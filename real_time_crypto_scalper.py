"""
real_time_crypto_scalper.py

- Uses alpaca-trade-api Stream to subscribe to crypto trades.
- Aggregates trades into 1-minute bars in memory.
- Calculates RSI(14) on 1-min closes and submits market orders on thresholds.
- Designed for PAPER trading only (easy to flip to live later).
"""

import os
import asyncio
import math
from datetime import datetime, timedelta
from collections import defaultdict, deque

import numpy as np
import pandas as pd
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.rest import REST, TimeFrame
from alpaca_trade_api.common import URL
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import ta

# config
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Trading client (paper)
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)

# Watchlist - use Alpaca pair format
SYMBOLS = ["BTC/USD", "ETH/USD"]  # add more pairs as desired

# RSI config
RSI_PERIOD = 14
BUY_THRESHOLD = 30.0
SELL_THRESHOLD = 70.0

# Order size per symbol (adjust)
ORDER_QTY = {
    "BTC/USD": 0.001,  # fractional crypto size example
    "ETH/USD": 0.01,
}

# We'll aggregate trades into 1-minute bars
# Structure: bars_buffer[symbol] = dict with current minute bucket (open, high, low, close, volume)
bars_buffer = {}
# History of 1-minute closes for RSI calc: deque per symbol
close_history = defaultdict(lambda: deque(maxlen=5000))

# A small helper to compute RSI from pandas Series
def compute_rsi_from_deque(deq, period=14):
    if len(deq) < period + 1:
        return None
    s = pd.Series(list(deq))
    return ta.momentum.RSIIndicator(s, window=period).rsi().iloc[-1]


async def on_trade_msg(trade):
    """
    Called for each incoming trade message from the Alpaca Stream.
    The object 'trade' is a model object from the Stream (has .symbol, .price, .size, .timestamp).
    """
    # extract symbol and price
    symbol = trade.symbol  # e.g., "BTC/USD"
    price = float(trade.price)
    ts = trade.timestamp  # aware datetime

    # align to minute bucket (floor)
    minute = ts.replace(second=0, microsecond=0)

    key = (symbol, minute)
    # initialize buffer for new bucket or symbol
    buf = bars_buffer.get(key)
    if buf is None:
        # start new bar
        buf = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": float(trade.size) if hasattr(trade, "size") else 0.0,
            "minute": minute,
            "symbol": symbol,
        }
        bars_buffer[key] = buf
    else:
        # update bar
        buf["high"] = max(buf["high"], price)
        buf["low"] = min(buf["low"], price)
        buf["close"] = price
        buf["volume"] += float(trade.size) if hasattr(trade, "size") else 0.0

    # Now check if the previous minute has completed: if there is any bucket older than current minute, flush it
    await flush_old_buckets(symbol, current_minute=minute)


async def flush_old_buckets(symbol, current_minute):
    """
    Flush any bars for `symbol` whose minute < current_minute.
    When flushed, we append close to close_history and compute RSI.
    """
    # find keys for this symbol that are older than current_minute
    old_keys = [k for k in bars_buffer.keys() if k[0] == symbol and k[1] < current_minute]
    for key in sorted(old_keys):
        buf = bars_buffer.pop(key)
        close = buf["close"]
        # append close to series
        close_history[symbol].append(close)

        # compute RSI
        rsi = compute_rsi_from_deque(close_history[symbol], RSI_PERIOD)

        # debug logging
        print(f"[{buf['minute'].isoformat()}] {symbol} O:{buf['open']:.2f} H:{buf['high']:.2f} "
              f"L:{buf['low']:.2f} C:{buf['close']:.2f} RSI:{(rsi if rsi is not None else 'n/a')}")

        # strategy: RSI trigger on completed 1-min bar (change this rule as needed)
        if rsi is not None:
            # Example logic: enter when RSI < BUY_THRESHOLD, exit when RSI > SELL_THRESHOLD.
            # IMPORTANT: this is a demo. Add position checks, risk management, cooldowns, etc.
            try:
                if rsi < BUY_THRESHOLD:
                    print(f"BUY signal for {symbol} (RSI {rsi:.2f}) - submitting market order")
                    qty = ORDER_QTY.get(symbol, 0.001)
                    order = MarketOrderRequest(symbol=symbol, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.GTC)
                    trading_client.submit_order(order)
                elif rsi > SELL_THRESHOLD:
                    print(f"SELL signal for {symbol} (RSI {rsi:.2f}) - submitting market order")
                    qty = ORDER_QTY.get(symbol, 0.001)
                    order = MarketOrderRequest(symbol=symbol, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.GTC)
                    trading_client.submit_order(order)
            except Exception as e:
                print("Order submission failed:", e)


async def main():
    # Create Stream object for crypto market data
    # data_stream defaults to 'polygon' in older libs; for crypto use data_stream='crypto' by subscribing to trades.
    # The Stream class will connect to Alpaca's data websocket automatically.
    stream = Stream(API_KEY, API_SECRET, data_feed='iex')  # data_feed not used for crypto trades, Stream handles channels

    # register trade handlers for each symbol
    for sym in SYMBOLS:
        # alpaca-trade-api Stream provides subscribe_trades method
        stream.subscribe_trades(on_trade_msg, sym)

    print("Starting stream... (hit Ctrl+C to stop)")
    await stream._run_forever()  # run the stream loop


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")
