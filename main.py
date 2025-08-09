import os
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import pandas as pd
import ta

# Alpaca API credentials (from environment variables)
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Data and trading clients
data_client = StockHistoricalDataClient(API_KEY, API_SECRET)
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)

symbol = "AAPL"
qty = 1

# Define date range for 1 year of daily bars
end = datetime.now()
start = end - timedelta(days=365)

# Request historical daily bar data
request_params = StockBarsRequest(
    symbol_or_symbols=symbol,
    timeframe=TimeFrame.Day,
    start=start,
    end=end
)
bars = data_client.get_stock_bars(request_params)
df = bars.df  # Pandas DataFrame with MultiIndex: (symbol, timestamp)
df = df[symbol].copy()  # isolate the AAPL data

# Calculate indicators
df["SMA50"] = df["close"].rolling(50).mean()
df["SMA200"] = df["close"].rolling(200).mean()
df["RSI"] = ta.momentum.RSIIndicator(df["close"], window=14).rsi()

latest = df.iloc[-1]
print(latest[["close", "SMA50", "SMA200", "RSI"]])

# Trading signal logic
if latest["RSI"] < 30 and latest["SMA50"] > latest["SMA200"]:
    print("BUY signal!")
    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY
    )
    trading_client.submit_order(order)
elif latest["RSI"] > 70 and latest["SMA50"] < latest["SMA200"]:
    print("SELL signal!")
    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY
    )
    trading_client.submit_order(order)
else:
    print("No trade today.")
