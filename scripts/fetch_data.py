#!/usr/bin/env python3
"""
Fetch S&P 500 insider trading data from Finnhub + price candles from Yahoo Finance.
Saves static JSON files for GitHub Pages dashboard.
Runs daily via GitHub Actions.
"""

import json
import os
import time
import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

try:
    import yfinance as yf
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance"])
    import yfinance as yf

API_KEY = os.environ.get("FINNHUB_API_KEY", "")
BASE = "https://finnhub.io/api/v1"
DATA_DIR = Path(__file__).parent.parent / "data"
CANDLES_DIR = DATA_DIR / "candles"

# Major S&P 500 symbols + sectors
SP500 = {
    "AAPL": {"n": "Apple", "s": "Technology"},
    "MSFT": {"n": "Microsoft", "s": "Technology"},
    "AMZN": {"n": "Amazon", "s": "Consumer"},
    "NVDA": {"n": "NVIDIA", "s": "Technology"},
    "GOOGL": {"n": "Alphabet", "s": "Technology"},
    "META": {"n": "Meta", "s": "Technology"},
    "TSLA": {"n": "Tesla", "s": "Consumer"},
    "JPM": {"n": "JPMorgan", "s": "Financials"},
    "V": {"n": "Visa", "s": "Financials"},
    "JNJ": {"n": "J&J", "s": "Healthcare"},
    "UNH": {"n": "UnitedHealth", "s": "Healthcare"},
    "XOM": {"n": "ExxonMobil", "s": "Energy"},
    "WMT": {"n": "Walmart", "s": "Consumer"},
    "MA": {"n": "Mastercard", "s": "Financials"},
    "PG": {"n": "P&G", "s": "Consumer"},
    "HD": {"n": "Home Depot", "s": "Consumer"},
    "CVX": {"n": "Chevron", "s": "Energy"},
    "MRK": {"n": "Merck", "s": "Healthcare"},
    "ABBV": {"n": "AbbVie", "s": "Healthcare"},
    "KO": {"n": "Coca-Cola", "s": "Consumer"},
    "PEP": {"n": "PepsiCo", "s": "Consumer"},
    "AVGO": {"n": "Broadcom", "s": "Technology"},
    "LLY": {"n": "Eli Lilly", "s": "Healthcare"},
    "COST": {"n": "Costco", "s": "Consumer"},
    "TMO": {"n": "Thermo Fisher", "s": "Healthcare"},
    "MCD": {"n": "McDonald's", "s": "Consumer"},
    "ABT": {"n": "Abbott", "s": "Healthcare"},
    "CSCO": {"n": "Cisco", "s": "Technology"},
    "ACN": {"n": "Accenture", "s": "Technology"},
    "NKE": {"n": "Nike", "s": "Consumer"},
    "NEE": {"n": "NextEra", "s": "Utilities"},
    "CRM": {"n": "Salesforce", "s": "Technology"},
    "LIN": {"n": "Linde", "s": "Materials"},
    "ORCL": {"n": "Oracle", "s": "Technology"},
    "AMD": {"n": "AMD", "s": "Technology"},
    "INTC": {"n": "Intel", "s": "Technology"},
    "BA": {"n": "Boeing", "s": "Industrials"},
    "RTX": {"n": "RTX", "s": "Industrials"},
    "CAT": {"n": "Caterpillar", "s": "Industrials"},
    "GE": {"n": "GE", "s": "Industrials"},
    "DE": {"n": "Deere", "s": "Industrials"},
    "UPS": {"n": "UPS", "s": "Industrials"},
    "GS": {"n": "Goldman Sachs", "s": "Financials"},
    "MS": {"n": "Morgan Stanley", "s": "Financials"},
    "BLK": {"n": "BlackRock", "s": "Financials"},
    "AXP": {"n": "Amex", "s": "Financials"},
    "SPGI": {"n": "S&P Global", "s": "Financials"},
    "DUK": {"n": "Duke Energy", "s": "Utilities"},
    "SO": {"n": "Southern Co", "s": "Utilities"},
    "AMT": {"n": "American Tower", "s": "Real Estate"},
    "PLD": {"n": "Prologis", "s": "Real Estate"},
    "COP": {"n": "ConocoPhillips", "s": "Energy"},
    "SLB": {"n": "Schlumberger", "s": "Energy"},
    "EOG": {"n": "EOG", "s": "Energy"},
    "APD": {"n": "Air Products", "s": "Materials"},
    "SHW": {"n": "Sherwin-Williams", "s": "Materials"},
    "ADBE": {"n": "Adobe", "s": "Technology"},
    "NOW": {"n": "ServiceNow", "s": "Technology"},
    "INTU": {"n": "Intuit", "s": "Technology"},
    "QCOM": {"n": "Qualcomm", "s": "Technology"},
    "ISRG": {"n": "Intuitive Surg.", "s": "Healthcare"},
    "GILD": {"n": "Gilead", "s": "Healthcare"},
    "AMGN": {"n": "Amgen", "s": "Healthcare"},
    "MDT": {"n": "Medtronic", "s": "Healthcare"},
    "PFE": {"n": "Pfizer", "s": "Healthcare"},
    "BMY": {"n": "Bristol-Myers", "s": "Healthcare"},
    "T": {"n": "AT&T", "s": "Telecom"},
    "VZ": {"n": "Verizon", "s": "Telecom"},
    "TMUS": {"n": "T-Mobile", "s": "Telecom"},
    "DIS": {"n": "Disney", "s": "Consumer"},
    "NFLX": {"n": "Netflix", "s": "Consumer"},
    "CMCSA": {"n": "Comcast", "s": "Consumer"},
    "PM": {"n": "Philip Morris", "s": "Consumer"},
}

RATE_LIMIT_DELAY = 1.2  # seconds between calls (safe for 60/min)


def api_call(endpoint, retries=3):
    """Make Finnhub API call with retry logic."""
    url = f"{BASE}{endpoint}&token={API_KEY}" if "?" in endpoint else f"{BASE}{endpoint}?token={API_KEY}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                print(f"  403 Forbidden for {endpoint}, waiting 60s...")
                time.sleep(60)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  Error on attempt {attempt+1}: {e}")
            time.sleep(5)
    return None


def fetch_insider_transactions():
    """Fetch insider transactions for all S&P 500 symbols."""
    print("=" * 60)
    print("Fetching insider transactions...")
    print("=" * 60)

    all_tx = []
    symbols = list(SP500.keys())

    for i, sym in enumerate(symbols):
        print(f"  [{i+1}/{len(symbols)}] {sym}...", end=" ")
        data = api_call(f"/stock/insider-transactions?symbol={sym}")

        if data and "data" in data and data["data"]:
            # Filter: only P (Purchase) and S (Sale), last 180 days
            cutoff = datetime.datetime.now() - datetime.timedelta(days=180)
            filtered = []
            for tx in data["data"]:
                code = (tx.get("transactionCode") or "").upper()
                if code not in ("P", "S"):
                    continue
                tx_date_str = tx.get("transactionDate") or tx.get("filingDate") or ""
                try:
                    tx_date = datetime.datetime.strptime(tx_date_str, "%Y-%m-%d")
                except ValueError:
                    continue
                if tx_date < cutoff:
                    continue
                if not tx.get("change") or tx["change"] == 0:
                    continue

                filtered.append({
                    "sym": sym,
                    "name": tx.get("name", "Unknown"),
                    "code": code,
                    "change": tx["change"],
                    "price": tx.get("transactionPrice", 0),
                    "share": tx.get("share", 0),
                    "txDate": tx_date_str,
                    "fileDate": tx.get("filingDate", ""),
                })

            print(f"{len(filtered)} trades")
            all_tx.extend(filtered)
        else:
            print("no data")

        time.sleep(RATE_LIMIT_DELAY)

    # Sort by date desc
    all_tx.sort(key=lambda x: x["txDate"], reverse=True)

    # Deduplicate
    seen = set()
    deduped = []
    for tx in all_tx:
        key = f"{tx['name']}-{tx['txDate']}-{tx['change']}-{tx['sym']}"
        if key not in seen:
            seen.add(key)
            deduped.append(tx)

    print(f"\nTotal: {len(deduped)} unique transactions (P/S only, 180d)")
    return deduped


def fetch_candles(symbols_needed):
    """Fetch 200 days of daily candles using Yahoo Finance (free, no rate limit)."""
    print("\n" + "=" * 60)
    print("Fetching price candles via Yahoo Finance...")
    print("=" * 60)

    for i, sym in enumerate(symbols_needed):
        print(f"  [{i+1}/{len(symbols_needed)}] {sym}...", end=" ")
        try:
            ticker = yf.Ticker(sym)
            df = ticker.history(period="200d", interval="1d")

            if df.empty:
                print("no data")
                continue

            # Convert to same format as before: timestamps + close prices
            timestamps = [int(ts.timestamp()) for ts in df.index]
            closes = [round(float(p), 2) for p in df["Close"]]
            highs = [round(float(p), 2) for p in df["High"]]
            lows = [round(float(p), 2) for p in df["Low"]]

            candle = {
                "t": timestamps,
                "c": closes,
                "h": highs,
                "l": lows,
            }

            out_path = CANDLES_DIR / f"{sym}.json"
            with open(out_path, "w") as f:
                json.dump(candle, f, separators=(",", ":"))
            print(f"{len(closes)} days")

        except Exception as e:
            print(f"error: {e}")

        time.sleep(0.3)  # Light delay to be polite


def build_summary(transactions):
    """Build summary statistics."""
    buys = [tx for tx in transactions if tx["code"] == "P"]
    sells = [tx for tx in transactions if tx["code"] == "S"]

    buy_val = sum(abs(tx["change"] * tx["price"]) for tx in buys)
    sell_val = sum(abs(tx["change"] * tx["price"]) for tx in sells)

    # Top stocks by buy/sell volume
    buy_stocks = {}
    sell_stocks = {}
    for tx in transactions:
        val = abs(tx["change"] * tx["price"])
        sym = tx["sym"]
        if tx["code"] == "P":
            buy_stocks.setdefault(sym, {"total": 0, "count": 0})
            buy_stocks[sym]["total"] += val
            buy_stocks[sym]["count"] += 1
        else:
            sell_stocks.setdefault(sym, {"total": 0, "count": 0})
            sell_stocks[sym]["total"] += val
            sell_stocks[sym]["count"] += 1

    # Top insiders
    buy_insiders = {}
    sell_insiders = {}
    for tx in transactions:
        val = abs(tx["change"] * tx["price"])
        name = tx["name"]
        sym = tx["sym"]
        if tx["code"] == "P":
            if name not in buy_insiders or buy_insiders[name]["total"] < val + buy_insiders[name]["total"]:
                buy_insiders.setdefault(name, {"total": 0, "sym": sym, "txs": []})
                buy_insiders[name]["total"] += val
                buy_insiders[name]["txs"].append({"sym": sym, "date": tx["txDate"], "val": val})
        else:
            sell_insiders.setdefault(name, {"total": 0, "sym": sym, "txs": []})
            sell_insiders[name]["total"] += val
            sell_insiders[name]["txs"].append({"sym": sym, "date": tx["txDate"], "val": val})

    # Sectors
    sectors = {}
    for tx in transactions:
        info = SP500.get(tx["sym"])
        if not info:
            continue
        sec = info["s"]
        sectors.setdefault(sec, {"buys": 0, "sells": 0})
        val = abs(tx["change"] * tx["price"])
        if tx["code"] == "P":
            sectors[sec]["buys"] += val
        else:
            sectors[sec]["sells"] += val

    return {
        "updated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "buyCount": len(buys),
        "sellCount": len(sells),
        "buyVal": round(buy_val, 2),
        "sellVal": round(sell_val, 2),
        "uniqueSymbols": len(set(tx["sym"] for tx in transactions)),
        "topBuyStocks": sorted(buy_stocks.items(), key=lambda x: -x[1]["total"])[:5],
        "topSellStocks": sorted(sell_stocks.items(), key=lambda x: -x[1]["total"])[:5],
        "topBuyInsiders": sorted(
            [(k, {"total": v["total"], "sym": v["sym"], "txs": sorted(v["txs"], key=lambda t: -t["val"])[:5]})
             for k, v in buy_insiders.items()],
            key=lambda x: -x[1]["total"]
        )[:5],
        "topSellInsiders": sorted(
            [(k, {"total": v["total"], "sym": v["sym"], "txs": sorted(v["txs"], key=lambda t: -t["val"])[:5]})
             for k, v in sell_insiders.items()],
            key=lambda x: -x[1]["total"]
        )[:5],
        "sectors": sorted(sectors.items(), key=lambda x: -(x[1]["buys"] + x[1]["sells"])),
    }


def main():
    if not API_KEY:
        print("ERROR: FINNHUB_API_KEY env var not set!")
        return

    # Ensure directories exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CANDLES_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Fetch insider transactions
    transactions = fetch_insider_transactions()

    # Save raw transactions
    with open(DATA_DIR / "insider.json", "w") as f:
        json.dump(transactions, f, separators=(",", ":"))
    print(f"Saved {len(transactions)} transactions to data/insider.json")

    # 2. Build & save summary
    summary = build_summary(transactions)
    with open(DATA_DIR / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Saved summary to data/summary.json")

    # 3. Fetch candles for symbols with activity
    active_symbols = sorted(set(tx["sym"] for tx in transactions))
    print(f"\n{len(active_symbols)} symbols with insider activity: {', '.join(active_symbols[:10])}...")
    fetch_candles(active_symbols)

    print("\n" + "=" * 60)
    print("DONE!")
    print(f"  Transactions: {len(transactions)}")
    print(f"  Buys: {summary['buyCount']}, Sells: {summary['sellCount']}")
    print(f"  Buy value: ${summary['buyVal']:,.0f}")
    print(f"  Sell value: ${summary['sellVal']:,.0f}")
    print(f"  Candles fetched: {len(active_symbols)} symbols")
    print("=" * 60)


if __name__ == "__main__":
    main()
