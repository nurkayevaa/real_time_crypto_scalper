import os
from datetime import datetime, timedelta
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import ta

# API credentials
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Alpaca clients
data_client = CryptoHistoricalDataClient()  # crypto data doesn't require API keys
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)

symbol = "BTC/USD"  # crypto pair
qty = 0.001         # fractional qty works for crypto

# Date range for last year of daily bars
end = datetime.now()
start = end - timedelta(days=365)

# Get crypto bars
request_params = CryptoBarsRequest(
    symbol_or_symbols=symbol,
    timeframe=TimeFrame.Hour,
    start=start,
    end=end
)
bars = data_client.get_crypto_bars(request_params)

# Convert MultiIndex DataFrame to single symbol DataFrame
bars_df = bars.df
if symbol in bars_df.index.get_level_values(0):
    df = bars_df.loc[symbol].copy()
else:
    raise ValueError(f"No data found for {symbol}")

# Technical indicators
df["SMA50"] = df["close"].rolling(50).mean()
df["SMA200"] = df["close"].rolling(200).mean()
df["RSI"] = ta.momentum.RSIIndicator(df["close"], window=14).rsi()

latest = df.iloc[-1]
print(latest[["close", "SMA50", "SMA200", "RSI"]])

# Trading signals
if latest["RSI"] < 30 and latest["SMA50"] > latest["SMA200"]:
    print("BUY signal!")
    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.GTC
    )
    trading_client.submit_order(order)
elif latest["RSI"] > 70 and latest["SMA50"] < latest["SMA200"]:
    print("SELL signal!")
    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC
    )
    trading_client.submit_order(order)
else:
    print("No trade today.")
