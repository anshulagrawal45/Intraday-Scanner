"""
pre_market_watchlist.py

What it does:
- Fetches US & Asian index moves using yfinance (previous close -> pre-open change)
- Fetches India VIX (^INDIAVIX) using yfinance
- Scrapes a public page for GIFT/SGX Nifty quote as a quick pre-open indicator
- Pulls a community pre-open F&O snapshot (pre-open movers) endpoint (public CSV/JSON style)
- Scores market bias and ranks candidate stocks

Notes:
- This is a prototype scanner. Replace scraping with official APIs for production.
- Run this script before NSE open (e.g. 08:45 - 09:15 IST) to create your intraday watchlist.
"""

import requests
import yfinance as yf
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta

# ---------------------------
# Config / symbols
# ---------------------------
US_SYMBOLS = {"S&P500": "^GSPC", "Dow": "^DJI", "Nasdaq": "^IXIC"}
ASIA_SYMBOLS = {"Nikkei": "^N225", "HangSeng": "^HSI"}  # proxies
INDIA_VIX = "^INDIAVIX"  # India VIX on Yahoo
# For gift/sgx nifty we will scrape a public page (moneycontrol/groww) as fallback
GIFT_NIFTY_URL = "https://www.moneycontrol.com/live-index/gift-nifty"

# Pre-open snapshot / preopen F&O - community endpoint (used by some GitHub scripts)
# This is a known community URL that many use to get pre-open snapshot for F&O movers.
PREOPEN_FO_URL = "https://howutrade.in/snapdata/?data=PreOpen_FO"

# Stocks we consider (you can expand)
# Default to top F&O names or NIFTY50 — here example list (use real list for live)
DEFAULT_STOCK_POOL = [
        "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
        "HINDUNILVR.NS", "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "KOTAKBANK.NS",
        "LT.NS", "AXISBANK.NS", "BAJFINANCE.NS", "WIPRO.NS", "ASIANPAINT.NS",
        "MARUTI.NS", "TITAN.NS", "SUNPHARMA.NS", "ULTRACEMCO.NS", "NESTLEIND.NS",
        "HCLTECH.NS", "TATAMOTORS.NS", "POWERGRID.NS", "NTPC.NS", "TECHM.NS",
        "BAJAJFINSV.NS", "ONGC.NS", "M&M.NS", "ADANIPORTS.NS", "DIVISLAB.NS"
]

# ---------------------------
# Helpers
# ---------------------------
def pct_change(curr, prev):
    try:
        return (curr - prev) / prev * 100.0
    except Exception:
        return None

def fetch_yf_symbols(symbols: dict):
    out = {}
    for name, sym in symbols.items():
        ticker = yf.Ticker(sym)
        hist = ticker.history(period="2d")  # get last 2 days
        if hist.empty:
            out[name] = None
            continue
        # use last close and latest price if market open
        last_close = hist['Close'].iloc[-2] if len(hist) >= 2 else hist['Close'].iloc[-1]
        latest = hist['Close'].iloc[-1]
        out[name] = {"symbol": sym, "last_close": float(last_close), "latest": float(latest),
                     "pct": pct_change(float(latest), float(last_close))}
    return out

def fetch_india_vix():
    t = yf.Ticker(INDIA_VIX)
    hist = t.history(period="2d")
    if hist.empty:
        return None
    last_close = hist['Close'].iloc[-2] if len(hist) >= 2 else hist['Close'].iloc[-1]
    latest = hist['Close'].iloc[-1]
    return {"symbol": INDIA_VIX, "last_close": float(last_close), "latest": float(latest),
            "pct": pct_change(float(latest), float(last_close))}

def scrape_gift_nifty():
    """
    Quick scraper for Gift Nifty from moneycontrol page. This is a fallback quick indicator.
    Replace with official API if available.
    """
    try:
        r = requests.get(GIFT_NIFTY_URL, timeout=8)
        soup = BeautifulSoup(r.text, "lxml")
        # Moneycontrol page contains a numeric value; selectors may change. We try robust parsing.
        # Look for an element containing 'Gift Nifty' or 'GIFT NIFTY' and a numeric sibling.
        text = soup.get_text(separator="|")
        # find first occurrence of "gift-nifty" or "GIFT" phrase - fallback: search for numeric groups
        import re
        # find numbers like 259xx or with commas
        nums = re.findall(r"\b\d{2,3}[,\d]*\.\d+|\b\d{4,6}\b", text.replace(',', ''))
        if nums:
            # pick the first reasonable large number (likely the index)
            for n in nums:
                try:
                    val = float(n)
                    if val > 1000:  # simple filter
                        return {"value": val}
                except:
                    continue
        return None
    except Exception as e:
        return None

def fetch_preopen_fo():
    """
    Fetch pre-open F&O snapshot from community endpoint.
    Response is usually JSON. If unavailable, return None.
    """
    try:
        r = requests.get(PREOPEN_FO_URL, timeout=6)
        if r.status_code != 200:
            return None
        data = r.json()
        # data format varies. We will try to take a table-like list of dicts.
        return data
    except Exception:
        return None

# ---------------------------
# Scoring logic
# ---------------------------
def score_market(us_data, asia_data, gift, vix):
    """
    Simple scoring:
    +1 for each US index positive
    +1 for each Asia index positive
    +1 if GIFT is positive vs NIFTY last close (if available)
    -1 if India VIX rose > 3% (risk-off)
    """
    score = 0
    details = []
    for name, info in us_data.items():
        if info and info.get("pct", 0) is not None:
            if info["pct"] > 0:
                score += 1
                details.append(f"{name} up {info['pct']:.2f}%")
            else:
                score -= 1
                details.append(f"{name} down {info['pct']:.2f}%")
    for name, info in asia_data.items():
        if info and info.get("pct", 0) is not None:
            if info["pct"] > 0:
                score += 0.5
                details.append(f"{name} up {info['pct']:.2f}%")
            else:
                score -= 0.5
                details.append(f"{name} down {info['pct']:.2f}%")

    if gift and isinstance(gift, dict) and gift.get("value"):
        # we can't compute exact pct vs nifty, so treat >0 as small bull bias
        details.append(f"GIFT Nifty ~ {gift['value']}")
        # no robust baseline here — leave neutral, or add small weight if obvious
    if vix:
        if vix.get("pct") is not None and vix["pct"] > 3.0:
            score -= 1.5
            details.append(f"India VIX up {vix['pct']:.2f}% (risk-on off)")
        elif vix.get("pct") is not None and vix["pct"] < -2.0:
            score += 0.5
            details.append(f"India VIX down {vix['pct']:.2f}% (calmer)")

    return score, details

def analyze_preopen_and_pick_stocks(preopen_data, pool = DEFAULT_STOCK_POOL, top_n=6):
    """
    Use preopen_data to extract gap%, quantity and other metrics.
    The community endpoint returns rows — find top gap ups / gap downs and liquidity.
    If preopen_data not available, fallback to scanning the given 'pool' via yfinance for overnight gap.
    """
    candidates = []
    if preopen_data:
        # The exact keys are provider-dependent. Try to handle common table structures.
        # Many pre-open snapshots include 'symbol', 'open', 'prev_close', 'qty' etc.
        rows = []
        if isinstance(preopen_data, dict):
            # try different keys
            if "data" in preopen_data and isinstance(preopen_data["data"], list):
                rows = preopen_data["data"]
            elif "result" in preopen_data and isinstance(preopen_data["result"], list):
                rows = preopen_data["result"]
            else:
                # maybe it's a dict of lists
                rows = []
                for v in preopen_data.values():
                    if isinstance(v, list):
                        rows = v
                        break
        elif isinstance(preopen_data, list):
            rows = preopen_data
        # now try to extract sensible rows
        for r in rows:
            # find fields flexibly
            sym = r.get("symbol") or r.get("scrip") or r.get("name")
            try:
                prev = float(r.get("prev_close") or r.get("prevClose") or r.get("previousClose") or 0)
                op = float(r.get("open") or r.get("preopen_price") or r.get("preopen") or prev)
                qty = float(r.get("qty") or r.get("quantity") or r.get("tradedQty") or 0)
            except Exception:
                continue
            if not sym:
                continue
            gap = pct_change(op, prev) if prev>0 else 0
            rowscore = {
                "symbol": sym,
                "prev_close": prev,
                "preopen": op,
                "gap_pct": gap,
                "qty": qty
            }
            candidates.append(rowscore)
    # if candidates empty -> fallback to checking pool overnight gaps via yfinance
    if not candidates:
        for s in pool:
            try:
                t = yf.Ticker(s)
                hist = t.history(period="2d")
                if hist.shape[0] < 2:
                    continue
                prev = float(hist['Close'].iloc[-2])
                latest = float(hist['Close'].iloc[-1])
                gap = pct_change(latest, prev)
                candidates.append({
                    "symbol": s.replace(".NS", ""),
                    "prev_close": prev,
                    "preopen": latest,
                    "gap_pct": gap,
                    "qty": None
                })
            except Exception:
                continue

    # rank candidates: primary by absolute gap% and qty (if present)
    df = pd.DataFrame(candidates)
    if df.empty:
        return []
    # scoring heuristic
    df['abs_gap'] = df['gap_pct'].abs()
    # qty normalization
    if df['qty'].notnull().any():
        df['qty_norm'] = (df['qty'] - df['qty'].min()) / (df['qty'].max() - df['qty'].min() + 1e-9)
    else:
        df['qty_norm'] = 0.0
    # final score: gap magnitude * 0.7 + qty_norm * 0.3 ; prefer directional gap sign if market bias same
    df['score'] = df['abs_gap'] * 0.7 + df['qty_norm'] * 0.3
    df = df.sort_values('score', ascending=False)
    # select top_n
    watch = df.head(top_n).to_dict('records')
    return watch

# ---------------------------
# Main scanning routine
# ---------------------------

def run_scan():
    ts = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    us = fetch_yf_symbols(US_SYMBOLS)
    asia = fetch_yf_symbols(ASIA_SYMBOLS)
    vix = fetch_india_vix()
    gift = scrape_gift_nifty()
    preopen = fetch_preopen_fo()

    score, details = score_market(us, asia, gift, vix)
    bias = "BULLISH" if score > 1.0 else ("BEARISH" if score < -1.0 else "NEUTRAL")

    print("\nBias score:", score, " =>", bias)
    print("Details:", "; ".join(details))

    # pick candidate stocks
    watch = analyze_preopen_and_pick_stocks(preopen, DEFAULT_STOCK_POOL, top_n=6)
    print("\n--- Watchlist (Top candidates) ---")
    if not watch:
        print("No pre-open candidates found. Consider running again or use broker API.")
    else:
        for i, w in enumerate(watch, 1):
            direction = "UP" if w['gap_pct']>0 else ("DOWN" if w['gap_pct']<0 else "FLAT")
            qtydisp = f", qty={int(w['qty'])}" if w.get('qty') else ""
            print(f"{i}. {w['symbol']} | gap={w['gap_pct']:.2f}% | preopen={w['preopen']} | prev={w['prev_close']}{qtydisp} | score={w['score']:.3f} | dir={direction}")

if __name__ == "__main__":
    run_scan()
