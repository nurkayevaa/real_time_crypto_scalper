
# scanner.py
import json
import alpaca_trade_api as tradeapi

API_KEY = "YOUR_KEY"
API_SECRET = "YOUR_SECRET"
BASE_URL = "https://paper-api.alpaca.markets"

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL)

crypto_list =  ["HYPE", "XMR", "XRP", "TRX", "BCH", "BTC", "BNB", "ETH", "SAROS", "XCN", "ZBCN", "SYRUP", "TOSHI"]

selected = []

for symbol in crypto_list:
    bars = api.get_crypto_bars(symbol, "1Day").df
    rsi = ... # calculate RSI here
    if rsi < 30 or rsi > 70:
        selected.append(symbol)

with open("tickers.json", "w") as f:
    json.dump(selected, f)

print(f"Selected cryptos: {selected}")
