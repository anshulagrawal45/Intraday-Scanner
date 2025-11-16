import numpy as np
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import datetime as dt
from tabulate import tabulate

def get_latest_indicators(ticker):
    """
    Get latest technical indicators for a stock
    Returns current RSI, EMA status, ADX and price info
    """
    try:
        # Get last 60 days of data to calculate indicators
        end_date = dt.datetime.now()
        start_date = end_date - dt.timedelta(days=90)
        
        data = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d")
        if data is None or len(data) == 0:
            return None
        
        DF = pd.DataFrame(data)
        DF = DF.dropna()
        
        if len(DF) < 50:
            return None
        
        # Calculate indicators using pandas_ta
        DF.ta.ema(length=20, append=True, col_names=('EMA_20',))
        DF.ta.ema(length=50, append=True, col_names=('EMA_50',))
        DF.ta.rsi(length=14, append=True, col_names=('RSI',))
        DF.ta.adx(length=14, append=True)
        
        # Rename ADX column if needed (pandas_ta uses 'ADX_14')
        if 'ADX_14' in DF.columns:
            DF['ADX'] = DF['ADX_14']
        
        # Get latest values
        latest = DF.iloc[-1]
        prev = DF.iloc[-2]
        
        # Calculate additional metrics
        price_change_pct = ((latest['Close'] - prev['Close']) / prev['Close']) * 100
        volume_ratio = latest['Volume'] / DF['Volume'].tail(20).mean() if DF['Volume'].tail(20).mean() > 0 else 0
        
        # Trading signals
        is_uptrend = latest['RSI'] > 50
        ema_bullish = latest['EMA_20'] > latest['EMA_50']
        strong_trend = latest['ADX'] > 25
        
        # Overall signal
        signal_score = sum([is_uptrend, ema_bullish, strong_trend])
        
        if signal_score >= 2 and is_uptrend:
            signal = "BUY"
        elif signal_score >= 2:
            signal = "WATCH"
        else:
            signal = "SKIP"
        
        return {
            'ticker': ticker,
            'current_price': latest['Close'],
            'open_price': latest['Open'],
            'high': latest['High'],
            'low': latest['Low'],
            'price_change_pct': price_change_pct,
            'volume_ratio': volume_ratio,
            'rsi': latest['RSI'],
            'ema_20': latest['EMA_20'],
            'ema_50': latest['EMA_50'],
            'adx': latest['ADX'],
            'is_uptrend': is_uptrend,
            'ema_bullish': ema_bullish,
            'strong_trend': strong_trend,
            'signal': signal,
            'signal_score': signal_score
        }
    
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
        return None


def scan_stocks(stock_list):
    """
    Scan multiple stocks and return trading candidates
    """
    results = []
    
    for ticker in stock_list:
        data = get_latest_indicators(ticker)
        if data:
            results.append(data)
    
    return results


def display_scan_results(results):
    """
    Display scan results in a formatted table
    """
    # Separate by signal
    buy_signals = [r for r in results if r['signal'] == 'BUY']
    watch_signals = [r for r in results if r['signal'] == 'WATCH']
    skip_signals = [r for r in results if r['signal'] == 'SKIP']
    
    print(f"Scan Time: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # BUY Signals
    if buy_signals:
        print("\n BUY SIGNALS")
        
        table_data = []
        for r in sorted(buy_signals, key=lambda x: x['rsi'], reverse=True):
            table_data.append([
                r['ticker'],
                f"{r['current_price']:.2f}",
                f"{r['price_change_pct']:+.2f}%",
                f"{r['rsi']:.1f}",
                f"{r['adx']:.1f}",
                "Bull" if r['ema_bullish'] else "Bear",
                f"{r['volume_ratio']:.2f}x",
                r['signal_score']
            ])
        
        headers = ["Ticker", "Price", "Change%", "RSI", "ADX", "EMA", "Volume", "Score"]
        print(tabulate(table_data, headers=headers, tablefmt="simple"))
    
    # WATCH Signals
    if watch_signals:
        print("\n WATCH LIST")
        
        table_data = []
        for r in sorted(watch_signals, key=lambda x: x['signal_score'], reverse=True):
            table_data.append([
                r['ticker'],
                f"{r['current_price']:.2f}",
                f"{r['rsi']:.1f}",
                f"{r['adx']:.1f}",
                "Bull" if r['ema_bullish'] else "Bear",
                r['signal_score']
            ])
        
        headers = ["Ticker", "Price", "RSI", "ADX", "EMA", "Score"]
        print(tabulate(table_data, headers=headers, tablefmt="simple"))
    
    return buy_signals, watch_signals

if __name__ == "__main__":
    # Major Indian stocks on NSE
    indian_stocks = [
        "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
        "HINDUNILVR.NS", "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "KOTAKBANK.NS",
        "LT.NS", "AXISBANK.NS", "BAJFINANCE.NS", "WIPRO.NS", "ASIANPAINT.NS",
        "MARUTI.NS", "TITAN.NS", "SUNPHARMA.NS", "ULTRACEMCO.NS", "NESTLEIND.NS",
        "HCLTECH.NS", "TATAMOTORS.NS", "POWERGRID.NS", "NTPC.NS", "TECHM.NS",
        "BAJAJFINSV.NS", "ONGC.NS", "M&M.NS", "ADANIPORTS.NS", "DIVISLAB.NS"
    ]
    
    # Run the scan
    results = scan_stocks(indian_stocks)
    
    if results:
        buy_signals, watch_signals = display_scan_results(results)
    else:
        print("No results found. Please check your internet connection or stock symbols.")
