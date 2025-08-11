import asyncio
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
import pandas as pd
from datetime import datetime, timedelta

# Replace with your Alpaca API keys
API_KEY = "YOUR_API_KEY"
API_SECRET = "YOUR_SECRET_KEY"

# Create a client (no need for async here — Alpaca's SDK handles it internally)
client = CryptoHistoricalDataClient(API_KEY, API_SECRET)

async def fetch_crypto_bars(symbol: str):
    """Fetch recent crypto bars for a symbol"""
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=1)

    request_params = CryptoBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=start_time,
        end=end_time
    )

    bars = client.get_crypto_bars(request_params)
    df = bars.df
    print(df.tail())
    return df

async def main():
    # List of crypto symbols
    symbols = ["BTC/USD", "ETH/USD"]

    # Run all tasks concurrently
    tasks = [fetch_crypto_bars(symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks)

    # Example: Save to CSV
    for symbol, df in zip(symbols, results):
        filename = symbol.replace("/", "_") + ".csv"
        df.to_csv(filename)
        print(f"Saved {filename}")

# Only run asyncio if this is the main file
if __name__ == "__main__":
    asyncio.run(main())
