import os
import asyncio
from datetime import datetime, timedelta
from collections import deque

import pandas as pd
import ta
from decimal import Decimal, ROUND_DOWN

from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce


API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Alpaca clients
data_client = CryptoHistoricalDataClient()  # No API keys required
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)

SYMBOLS = ["BTC/USD", "ETH/USD"]
ORDER_QTY = {
    "BTC/USD": 0.001,
    "ETH/USD": 0.01,
}

RSI_PERIOD = 14
SMA_50 = 50
SMA_200 = 200
BUY_THRESHOLD = 30.0
SELL_THRESHOLD = 70.0

STOP_LOSS_PCT = 0.01
TAKE_PROFIT_PCT = 0.02

# Store close prices per symbol for indicator calculation
close_prices = {sym: deque(maxlen=5000) for sym in SYMBOLS}


def round_price(price, decimals=4):
    d = Decimal(str(price))
    return float(d.quantize(Decimal("1." + "0" * decimals), rounding=ROUND_DOWN))


async def fetch_latest_bar(symbol: str):
    now = datetime.utcnow()
    start = now - timedelta(minutes=10)  # get recent bars, buffer for safety
    request_params = CryptoBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=start,
        end=now,
        limit=100,
    )
    bars = data_client.get_crypto_bars(request_params)
    df = bars.df
    if symbol in df.index.get_level_values(0):
        symbol_df = df.loc[symbol].copy()
        return symbol_df
    else:
        return None


async def compute_indicators(symbol: str, df: pd.DataFrame):
    close_series = df["close"]
    close_prices[symbol].extend(close_series.values)

    # Use latest close_prices deque for indicators
    close_list = list(close_prices[symbol])
    if len(close_list) < max(RSI_PERIOD, SMA_200):
        return None, None, None

    s = pd.Series(close_list)
    rsi = ta.momentum.RSIIndicator(s, window=RSI_PERIOD).rsi().iloc[-1]
    sma50 = s.rolling(SMA_50).mean().iloc[-1]
    sma200 = s.rolling(SMA_200).mean().iloc[-1]

    return rsi, sma50, sma200


async def place_bracket_order(symbol: str, qty: float, side: OrderSide, price: float):
    if side == OrderSide.BUY:
        sl_price = round_price(price * (1 - STOP_LOSS_PCT))
        tp_price = round_price(price * (1 + TAKE_PROFIT_PCT))
    else:
        sl_price = round_price(price * (1 + STOP_LOSS_PCT))
        tp_price = round_price(price * (1 - TAKE_PROFIT_PCT))

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
        order_response = trading_client.submit_order(order)
        print(f"Order submitted successfully: ID {order_response.id}, status {order_response.status}")
    except Exception as e:
        print(f"Order submission failed: {e}")

async def main_loop():
    print("Starting async REST polling crypto scalper...")
    while True:
        for symbol in SYMBOLS:
            df = await fetch_latest_bar(symbol)
            if df is None or df.empty:
                print(f"No bars received for {symbol}")
                continue

            rsi, sma50, sma200 = await compute_indicators(symbol, df)
            if rsi is None:
                print(f"Not enough data for indicators for {symbol}")
                continue

            last_close = df["close"].iloc[-1]
            qty = ORDER_QTY.get(symbol, 0.001)

            print(
                f"{datetime.utcnow().isoformat()} {symbol} Close={last_close:.2f} "
                f"RSI={rsi:.2f} SMA50={sma50:.2f} SMA200={sma200:.2f}"
            )

            # Trading logic
            if rsi < BUY_THRESHOLD and sma50 > sma200:
                print(f"BUY signal for {symbol}")
                await place_bracket_order(symbol, qty, OrderSide.BUY, last_close)
            elif rsi > SELL_THRESHOLD and sma50 < sma200:
                print(f"SELL signal for {symbol}")
                await place_bracket_order(symbol, qty, OrderSide.SELL, last_close)
            else:
                print(f"No trade signal for {symbol}")

        # Sleep until next minute (approx)
        now = datetime.utcnow()
        seconds_to_next_min = 60 - now.second
        await asyncio.sleep(seconds_to_next_min)


if __name__ == "__main__":
    asyncio.run(main_loop())
