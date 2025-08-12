import os
import time
from datetime import datetime
from collections import deque
from decimal import Decimal, ROUND_DOWN

import pandas as pd
import ta

from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Initialize clients
data_client = CryptoHistoricalDataClient()  # no keys needed for historical data
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)

SYMBOLS = ["BTC/USD", "ETH/USD"]

RSI_PERIOD = 14
MA_SHORT = 50
MA_LONG = 200
BUY_RSI_THRESHOLD = 30.0
SELL_RSI_THRESHOLD = 70.0

ORDER_QTY = {
    "BTC/USD": 0.001,
    "ETH/USD": 0.01,
}

STOP_LOSS_PCT = 0.01
TAKE_PROFIT_PCT = 0.02

# Store prices in memory for indicators
price_history = {symbol: deque(maxlen=5000) for symbol in SYMBOLS}

def round_price(price, decimals=4):
    d = Decimal(str(price))
    return float(d.quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_DOWN))

def calculate_indicators(prices_deque):
    if len(prices_deque) < MA_LONG:
        return None, None, None  # not enough data yet
    s = pd.Series(list(prices_deque))
    rsi = ta.momentum.RSIIndicator(s, window=RSI_PERIOD).rsi().iloc[-1]
    sma_short = s.rolling(MA_SHORT).mean().iloc[-1]
    sma_long = s.rolling(MA_LONG).mean().iloc[-1]
    return rsi, sma_short, sma_long

def place_order(symbol, side, qty, close_price):
    try:
        sl_price = round_price(close_price * (1 - STOP_LOSS_PCT)) if side == OrderSide.BUY else round_price(close_price * (1 + STOP_LOSS_PCT))
        tp_price = round_price(close_price * (1 + TAKE_PROFIT_PCT)) if side == OrderSide.BUY else round_price(close_price * (1 - TAKE_PROFIT_PCT))

        order = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.GTC,
            order_class="bracket",
            take_profit=dict(limit_price=str(tp_price)),
            stop_loss=dict(stop_price=str(sl_price)),
        )
        resp = trading_client.submit_order(order)
        print(f"Order placed: {side} {qty} {symbol} | TP: {tp_price} | SL: {sl_price} | Order ID: {resp.id}")
    except Exception as e:
        print(f"Order submission failed: {e}")

def main_loop():
    print("Starting synchronous crypto scalper...")

    while True:
        for symbol in SYMBOLS:
            try:
                # Fetch latest 1-min bars - can adjust timeframe or limit as needed
                bars = data_client.get_crypto_bars(
                    symbol_or_symbols=symbol,
                    timeframe="1Min",
                    limit=MA_LONG + 10  # get enough bars for MA calculations
                )
                df = bars.df
                if symbol not in df.index.get_level_values(0):
                    print(f"No data for {symbol}")
                    continue
                df_symbol = df.loc[symbol]
                closes = df_symbol['close'].tolist()
                price_history[symbol].extend(closes)

                rsi, sma_short, sma_long = calculate_indicators(price_history[symbol])
                if rsi is None:
                    print(f"Not enough data for indicators on {symbol}")
                    continue

                latest_close = price_history[symbol][-1]

                print(f"{datetime.now().isoformat()} - {symbol} | Close: {latest_close:.2f} | RSI: {rsi:.2f} | SMA{MA_SHORT}: {sma_short:.2f} | SMA{MA_LONG}: {sma_long:.2f}")

                qty = ORDER_QTY.get(symbol, 0.001)

                # Trading logic
                if rsi < BUY_RSI_THRESHOLD and sma_short > sma_long:
                    place_order(symbol, OrderSide.BUY, qty, latest_close)
                elif rsi > SELL_RSI_THRESHOLD and sma_short < sma_long:
                    place_order(symbol, OrderSide.SELL, qty, latest_close)
                else:
                    print("No trade signal.")

            except Exception as e:
                print(f"Error processing {symbol}: {e}")

        time.sleep(60)  # wait 1 min before next check

if __name__ == "__main__":
    main_loop()
