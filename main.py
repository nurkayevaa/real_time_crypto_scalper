import os
import yfinance as yf
import ta
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

client = TradingClient(API_KEY, API_SECRET, paper=True)

symbol = "AAPL"
qty = 1

df = yf.download(symbol, period="1y", interval="1d")
df["SMA50"] = df["Close"].rolling(50).mean()
df["SMA200"] = df["Close"].rolling(200).mean()
df["RSI"] = ta.momentum.RSIIndicator(df["Close"].squeeze(), window=14).rsi()


latest = df.iloc[-1]
print(latest[["Close", "SMA50", "SMA200", "RSI"]])

if latest["RSI"] < 30 and latest["SMA50"] > latest["SMA200"]:
    print("BUY signal!")
    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY
    )
    client.submit_order(order)
elif latest["RSI"] > 70 and latest["SMA50"] < latest["SMA200"]:
    print("SELL signal!")
    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY
    )
    client.submit_order(order)
else:
    print("No trade today.")
