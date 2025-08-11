"""
Real-time crypto scalper using alpaca-py for streaming and trading.
Calculates RSI(14) on 1-min bars and submits bracket orders on signals.
Designed for paper trading.
"""

import os
import asyncio
from datetime import datetime
from collections import defaultdict, deque
from decimal import Decimal, ROUND_DOWN

import pandas as pd
import ta

from alpaca.data.live import CryptoDataStream
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

trading_client = TradingClient(API_KEY, API_SECRET, paper=True)

SYMBOLS = ["BTC/USD", "ETH/USD"]

RSI_PERIOD = 14
BUY_THRESHOLD = 30.0
SELL_THRESHOLD = 70.0

ORDER_QTY = {
    "BTC/USD": 0.001,
    "ETH/USD": 0.01,
}

STOP_LOSS_PCT = 0.01
TAKE_PROFIT_PCT = 0.02

bars_buffer = {}
close_history = defaultdict(lambda: deque(maxlen=5000))


def round_price(price, decimals=4):
    d = Decimal(str(price))
    return float(d.quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_DOWN))


def compute_rsi_from_deque(deq, period=14):
    if len(deq) < period + 1:
        return None
    s = pd.Series(list(deq))
    return ta.momentum.RSIIndicator(s, window=period).rsi().iloc[-1]


async def handle_trade(trade):
    symbol = trade.symbol
    price = float(trade.price)
    ts = trade.timestamp  # aware datetime

    minute = ts.replace(second=0, microsecond=0)
    key = (symbol, minute)

    buf = bars_buffer.get(key)
    if buf is None:
        # New bar
        buf = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": float(getattr(trade, "size", 0.0)),
            "minute": minute,
            "symbol": symbol,
        }
        bars_buffer[key] = buf
    else:
        # Update existing bar
        buf["high"] = max(buf["high"], price)
        buf["low"] = min(buf["low"], price)
        buf["close"] = price
        buf["volume"] += float(getattr(trade, "size", 0.0))

    await flush_old_bars(symbol, current_minute=minute)


async def flush_old_bars(symbol, current_minute):
    old_keys = [k for k in bars_buffer if k[0] == symbol and k[1] < current_minute]
    for key in sorted(old_keys):
        buf = bars_buffer.pop(key)
        close = buf["close"]
        close_history[symbol].append(close)

        rsi = compute_rsi_from_deque(close_history[symbol], RSI_PERIOD)

        print(f"[{buf['minute'].isoformat()}] {symbol} O:{buf['open']:.2f} "
              f"H:{buf['high']:.2f} L:{buf['low']:.2f} C:{buf['close']:.2f} "
              f"RSI:{rsi if rsi is not None else 'n/a'}")

        if rsi is None:
            continue

        qty = ORDER_QTY.get(symbol, 0.001)

        try:
            if rsi < BUY_THRESHOLD:
                print(f"BUY signal for {symbol} (RSI {rsi:.2f}) - submitting bracket order")
                sl_price = round_price(close * (1 - STOP_LOSS_PCT))
                tp_price = round_price(close * (1 + TAKE_PROFIT_PCT))

                order = MarketOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.GTC,
                    order_class="bracket",
                    take_profit=dict(limit_price=str(tp_price)),
                    stop_loss=dict(stop_price=str(sl_price)),
                )
                trading_client.submit_order(order)

            elif rsi > SELL_THRESHOLD:
                print(f"SELL signal for {symbol} (RSI {rsi:.2f}) - submitting bracket order")
                sl_price = round_price(close * (1 + STOP_LOSS_PCT))
                tp_price = round_price(close * (1 - TAKE_PROFIT_PCT))

                order = MarketOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.GTC,
                    order_class="bracket",
                    take_profit=dict(limit_price=str(tp_price)),
                    stop_loss=dict(stop_price=str(sl_price)),
                )
                trading_client.submit_order(order)

        except Exception as e:
            print(f"Order submission failed: {e}")

async def main():
    stream = CryptoDataStream(API_KEY, API_SECRET)

    for sym in SYMBOLS:
        stream.subscribe_trades(handle_trade, sym)

    print("Starting real-time crypto scalper stream... (Ctrl+C to stop)")
    # This avoids nested asyncio.run()
    await stream.run_async()



if __name__ == "__main__":
    asyncio.run(main())
