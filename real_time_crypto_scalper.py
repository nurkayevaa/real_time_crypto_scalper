import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from itertools import product
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

data_client = CryptoHistoricalDataClient()
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)

symbols = [
    "BTC/USD", "ETH/USD", "SOL/USD", "LTC/USD",
    "DOGE/USD", "ADA/USD", "AVAX/USD", "BNB/USD"
]

def fetch_data(symbol, days=30):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    request_params = CryptoBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=start_time,
        end=end_time
    )
    bars = data_client.get_crypto_bars(request_params).df

    # Filter correctly whether symbol is a column or multi-index
    if 'symbol' in bars.columns:
        bars = bars[bars['symbol'] == symbol]
    else:
        bars = bars.reset_index()
        if 'symbol' in bars.columns:
            bars = bars[bars['symbol'] == symbol]

    return bars.reset_index(drop=True)

def backtest(df, rsi_period, ma_period, rsi_buy, rsi_sell):
    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=rsi_period).rsi()
    df['ma'] = df['close'].rolling(window=ma_period).mean()
    position = None
    entry_price = 0
    pnl = 0
    
    for i in range(max(rsi_period, ma_period), len(df)):
        price = df['close'].iloc[i]
        rsi = df['rsi'].iloc[i]
        ma = df['ma'].iloc[i]
        
        if position is None:
            if rsi < rsi_buy and price > ma:
                position = 'long'
                entry_price = price
        elif position == 'long':
            if rsi > rsi_sell and price < ma:
                pnl += (price - entry_price)
                position = None
    
    return pnl

def grid_search(df):
    best_params = None
    best_score = -float('inf')
    
    for rsi_period, ma_period, rsi_buy, rsi_sell in product(
        [10, 12, 14],
        [15, 20, 50],
        [25, 28, 30],
        [70, 72, 75]
    ):
        score = backtest(df.copy(), rsi_period, ma_period, rsi_buy, rsi_sell)
        if score > best_score:
            best_score = score
            best_params = {
                "rsi_period": rsi_period,
                "ma_period": ma_period,
                "rsi_buy": rsi_buy,
                "rsi_sell": rsi_sell
            }
    return best_params

def apply_strategy(df, params):
    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=params['rsi_period']).rsi()
    df['ma'] = df['close'].rolling(window=params['ma_period']).mean()
    latest = df.iloc[-1]
    
    if latest['rsi'] < params['rsi_buy'] and latest['close'] > latest['ma']:
        return "buy"
    elif latest['rsi'] > params['rsi_sell'] and latest['close'] < latest['ma']:
        return "sell"
    return None

def place_order(symbol, side, qty=0.001):
    order = MarketOrderRequest(
        symbol=symbol.replace("/", ""),
        qty=qty,
        side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
        time_in_force=TimeInForce.GTC
    )
    trading_client.submit_order(order)
    print(f"{datetime.utcnow()} Placed {side.upper()} order for {symbol}")

def run_bot():
    coin_params = {}
    
    # Find best params for each coin
    for symbol in symbols:
        df = fetch_data(symbol)
        if df.empty:
            print(f"No data for {symbol}")
            continue
        best_params = grid_search(df)
        coin_params[symbol] = best_params
        print(f"{symbol} best params: {best_params}")
    
    # Live trading step
    for symbol, params in coin_params.items():
        df = fetch_data(symbol, days=1)
        if len(df) < max(params['rsi_period'], params['ma_period']):
            continue
        signal = apply_strategy(df, params)
        if signal:
            place_order(symbol, signal)

if __name__ == "__main__":
    run_bot()
