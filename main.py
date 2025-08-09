import os
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import ta

# API credentials
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Alpaca clients
data_client = StockHistoricalDataClient(API_KEY, API_SECRET)


trading_client = TradingClient(
    API_KEY,
    API_SECRET,
    paper=True,
   # base_url=URL("https://paper-api.alpaca.markets/v2")
)


symbol = "TSLA"
qty = 1

# Get last year of daily bars from IEX feed
end = datetime.now()
start = end - timedelta(days=365)

request_params = StockBarsRequest(
    symbol_or_symbols=symbol,
    timeframe=TimeFrame.Day,
    start=start,
    end=end,
    feed="iex"  # <<--- IMPORTANT: Use free IEX feed instead of SIP
)
bars = data_client.get_stock_bars(request_params)

# Convert MultiIndex DataFrame to single symbol DataFrame
df = bars.df[symbol].copy()

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
