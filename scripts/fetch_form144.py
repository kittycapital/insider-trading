#!/usr/bin/env python3
"""
Fetch SEC Form 144 (Notice of Proposed Sale of Securities) data from EDGAR.
100% FREE - No API key required. Only needs User-Agent header.
Saves static JSON for GitHub Pages dashboard.
Runs daily via GitHub Actions.
"""

import json
import os
import time
import datetime
import re
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

# ============================================================
# CONFIG
# ============================================================
DATA_DIR = Path(__file__).parent.parent / "data"
CANDLES_DIR = DATA_DIR / "candles"  # Shared with insider trading page

# SEC EDGAR requires a descriptive User-Agent
USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "HerdVibe/1.0 (contact@herdvibe.com)"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/html, */*",
}

# EDGAR EFTS (Full-Text Search System) API
EFTS_BASE = "https://efts.sec.gov/LATEST/search-index"

# Major tickers to track (S&P 500 + popular stocks)
TRACKED_TICKERS = {
    "AAPL": "Apple Inc", "MSFT": "Microsoft Corp", "AMZN": "Amazon.com Inc",
    "NVDA": "NVIDIA Corp", "GOOGL": "Alphabet Inc", "META": "Meta Platforms Inc",
    "TSLA": "Tesla Inc", "JPM": "JPMorgan Chase", "V": "Visa Inc",
    "JNJ": "Johnson & Johnson", "UNH": "UnitedHealth Group", "XOM": "Exxon Mobil",
    "WMT": "Walmart Inc", "MA": "Mastercard Inc", "PG": "Procter & Gamble",
    "HD": "Home Depot", "CVX": "Chevron Corp", "MRK": "Merck & Co",
    "ABBV": "AbbVie Inc", "KO": "Coca-Cola Co", "PEP": "PepsiCo Inc",
    "AVGO": "Broadcom Inc", "LLY": "Eli Lilly", "COST": "Costco Wholesale",
    "CRM": "Salesforce Inc", "ORCL": "Oracle Corp", "AMD": "AMD Inc",
    "INTC": "Intel Corp", "BA": "Boeing Co", "GS": "Goldman Sachs",
    "MS": "Morgan Stanley", "NFLX": "Netflix Inc", "DIS": "Walt Disney",
    "ADBE": "Adobe Inc", "NOW": "ServiceNow", "QCOM": "Qualcomm",
    "AMGN": "Amgen Inc", "PFE": "Pfizer Inc", "T": "AT&T Inc",
    "VZ": "Verizon", "TMUS": "T-Mobile US", "PLTR": "Palantir Technologies",
    "COIN": "Coinbase Global", "SQ": "Block Inc", "SNOW": "Snowflake Inc",
    "UBER": "Uber Technologies", "ABNB": "Airbnb Inc", "RIVN": "Rivian Automotive",
    "SOFI": "SoFi Technologies", "HOOD": "Robinhood Markets", "MSTR": "MicroStrategy",
    "ARM": "Arm Holdings", "SMCI": "Super Micro Computer",
    "CAT": "Caterpillar Inc", "DE": "Deere & Co",
    "GE": "GE Aerospace", "RTX": "RTX Corp",
    "BLK": "BlackRock Inc", "AXP": "American Express",
    "NEE": "NextEra Energy", "DUK": "Duke Energy",
    "AMT": "American Tower", "PLD": "Prologis Inc",
    "COP": "ConocoPhillips", "EOG": "EOG Resources",
    "ISRG": "Intuitive Surgical", "GILD": "Gilead Sciences",
    "BMY": "Bristol-Myers Squibb", "MDT": "Medtronic",
}

# Reverse lookup: company name fragments -> ticker
NAME_TO_TICKER = {}
for ticker, name in TRACKED_TICKERS.items():
    # Add various fragments for matching
    NAME_TO_TICKER[name.upper()] = ticker
    # Add first word (e.g., "APPLE" -> AAPL)
    first_word = name.split()[0].upper()
    if first_word not in NAME_TO_TICKER:
        NAME_TO_TICKER[first_word] = ticker

RATE_LIMIT_DELAY = 0.12  # SEC allows 10 req/sec


def sec_get(url, retries=3):
    """Make SEC EDGAR request with retry logic."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"  Error attempt {attempt+1}: {e}")
            time.sleep(3)
    return None


def resolve_ticker(company_name, cik=None):
    """Try to resolve company name to ticker symbol."""
    if not company_name:
        return None
    
    name_upper = company_name.upper().strip()
    
    # Direct match
    if name_upper in NAME_TO_TICKER:
        return NAME_TO_TICKER[name_upper]
    
    # Partial match
    for known_name, ticker in NAME_TO_TICKER.items():
        if known_name in name_upper or name_upper in known_name:
            return ticker
    
    # Try CIK lookup via EDGAR company tickers
    if cik:
        try:
            cik_str = str(cik).zfill(10)
            url = f"https://data.sec.gov/submissions/CIK{cik_str}.json"
            resp = sec_get(url)
            if resp and resp.status_code == 200:
                data = resp.json()
                tickers = data.get("tickers", [])
                if tickers:
                    return tickers[0]
            time.sleep(RATE_LIMIT_DELAY)
        except Exception:
            pass
    
    return None


def fetch_form144_filings(days_back=90):
    """
    Fetch recent Form 144 filings from EDGAR full-text search.
    Returns list of filing metadata.
    """
    print("=" * 60)
    print(f"Fetching Form 144 filings (last {days_back} days)...")
    print("=" * 60)
    
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=days_back)
    
    all_filings = []
    start = 0
    page_size = 50
    max_pages = 20  # Safety limit
    
    for page in range(max_pages):
        url = (
            f"https://efts.sec.gov/LATEST/search-index"
            f"?q=%22form+144%22"
            f"&forms=144"
            f"&dateRange=custom"
            f"&startdt={start_date.isoformat()}"
            f"&enddt={end_date.isoformat()}"
            f"&from={start}"
        )
        
        print(f"  Page {page+1} (from={start})...", end=" ")
        resp = sec_get(url)
        
        if not resp:
            print("failed")
            break
        
        try:
            data = resp.json()
        except Exception:
            # Try alternate EFTS endpoint format
            print("trying alternate endpoint...")
            url2 = (
                f"https://efts.sec.gov/LATEST/search-index"
                f"?forms=144"
                f"&dateRange=custom"
                f"&startdt={start_date.isoformat()}"
                f"&enddt={end_date.isoformat()}"
                f"&from={start}"
            )
            resp = sec_get(url2)
            if not resp:
                break
            try:
                data = resp.json()
            except Exception:
                print("parse error")
                break
        
        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {})
        if isinstance(total, dict):
            total_count = total.get("value", 0)
        else:
            total_count = total or 0
        
        print(f"{len(hits)} hits (total: {total_count})")
        
        if not hits:
            break
        
        for hit in hits:
            source = hit.get("_source", {})
            filing = {
                "accession": source.get("file_num", ""),
                "fileDate": source.get("file_date", ""),
                "company": source.get("display_names", [""])[0] if source.get("display_names") else source.get("entity_name", ""),
                "cik": source.get("entity_id", ""),
                "formType": source.get("file_type", "144"),
                "url": "",
            }
            
            # Build filing URL
            file_id = hit.get("_id", "")
            if file_id:
                filing["url"] = f"https://www.sec.gov/Archives/edgar/data/{filing['cik']}/{file_id}"
            
            all_filings.append(filing)
        
        start += page_size
        if start >= total_count:
            break
        
        time.sleep(RATE_LIMIT_DELAY)
    
    print(f"\nFound {len(all_filings)} Form 144 filings from EFTS")
    return all_filings


def fetch_form144_via_rss(days_back=90):
    """
    Alternative: Fetch Form 144 via EDGAR full-text search RSS/JSON.
    Uses the newer EDGAR search API.
    """
    print("=" * 60)
    print(f"Fetching Form 144 via EDGAR Search API (last {days_back} days)...")
    print("=" * 60)
    
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=days_back)
    
    all_filings = []
    start = 0
    max_pages = 30
    
    for page in range(max_pages):
        url = (
            f"https://efts.sec.gov/LATEST/search-index"
            f"?q=&forms=144"
            f"&dateRange=custom"
            f"&startdt={start_date.isoformat()}"
            f"&enddt={end_date.isoformat()}"
            f"&from={start}"
        )
        
        print(f"  Page {page+1} (from={start})...", end=" ")
        resp = sec_get(url)
        
        if not resp or resp.status_code != 200:
            # Try the EDGAR full-text search API v2
            url = (
                f"https://efts.sec.gov/LATEST/search-index"
                f"?q=&forms=144"
                f"&startdt={start_date.isoformat()}"
                f"&enddt={end_date.isoformat()}"
                f"&from={start}"
            )
            resp = sec_get(url)
            if not resp:
                print("failed")
                break
        
        try:
            data = resp.json()
        except Exception:
            print("parse error")
            break
        
        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {})
        if isinstance(total, dict):
            total_count = total.get("value", 0)
        else:
            total_count = total or 0
        
        print(f"{len(hits)} hits (total: {total_count})")
        
        if not hits:
            break
        
        for hit in hits:
            source = hit.get("_source", {})
            names = source.get("display_names", [])
            company = names[0] if names else source.get("entity_name", "Unknown")
            
            filing = {
                "fileDate": source.get("file_date", ""),
                "company": company,
                "cik": str(source.get("entity_id", "")),
                "formType": source.get("file_type", "144"),
            }
            
            # Get filing document URL
            root_form = source.get("root_form", "")
            period = source.get("period_of_report", "")
            file_num = source.get("file_num", "")
            
            all_filings.append(filing)
        
        start += len(hits)
        if start >= total_count:
            break
        
        time.sleep(RATE_LIMIT_DELAY)
    
    return all_filings


def fetch_form144_from_submissions():
    """
    Fetch Form 144 data by checking recent submissions for tracked companies.
    This is the most reliable method - query each tracked company's EDGAR filings.
    """
    print("=" * 60)
    print("Fetching Form 144 from company submissions...")
    print("=" * 60)
    
    # First, get CIK mapping for tracked tickers
    print("  Loading SEC ticker-CIK mapping...")
    resp = sec_get("https://www.sec.gov/files/company_tickers.json")
    if not resp:
        print("  Failed to load CIK mapping!")
        return []
    
    cik_map = {}  # ticker -> cik
    company_map = {}  # cik -> company name
    try:
        tickers_data = resp.json()
        for entry in tickers_data.values():
            ticker = entry.get("ticker", "").upper()
            cik = str(entry.get("cik_str", ""))
            name = entry.get("title", "")
            if ticker in TRACKED_TICKERS:
                cik_map[ticker] = cik
                company_map[cik] = {"ticker": ticker, "name": name}
    except Exception as e:
        print(f"  Error parsing CIK data: {e}")
        return []
    
    print(f"  Mapped {len(cik_map)} tickers to CIKs")
    time.sleep(RATE_LIMIT_DELAY)
    
    # Now check each company for Form 144 filings
    all_form144 = []
    cutoff = datetime.date.today() - datetime.timedelta(days=180)
    
    tickers = list(cik_map.keys())
    for i, ticker in enumerate(tickers):
        cik = cik_map[ticker]
        cik_padded = cik.zfill(10)
        
        print(f"  [{i+1}/{len(tickers)}] {ticker} (CIK: {cik})...", end=" ")
        
        url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        resp = sec_get(url)
        
        if not resp or resp.status_code != 200:
            print("skip")
            time.sleep(RATE_LIMIT_DELAY)
            continue
        
        try:
            data = resp.json()
        except Exception:
            print("parse error")
            time.sleep(RATE_LIMIT_DELAY)
            continue
        
        # Check recent filings for Form 144
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        reporters = recent.get("reportOwner", []) if "reportOwner" in recent else [None] * len(forms)
        
        count_144 = 0
        for j, form in enumerate(forms):
            if form != "144":
                continue
            
            file_date = dates[j] if j < len(dates) else ""
            
            # Check date cutoff
            try:
                fd = datetime.date.fromisoformat(file_date)
                if fd < cutoff:
                    continue
            except ValueError:
                continue
            
            accession = accessions[j] if j < len(accessions) else ""
            primary_doc = primary_docs[j] if j < len(primary_docs) else ""
            
            # Build EDGAR URL - primary_doc may have xsl prefix like "xsl144X01/primary_doc.xml"
            # We need the raw XML: just "primary_doc.xml" in the accession directory
            acc_clean = accession.replace("-", "")
            # Raw XML URL (always primary_doc.xml directly)
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/primary_doc.xml"
            # Display URL (with XSLT rendering)
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{primary_doc}" if primary_doc else xml_url
            
            filing_entry = {
                "ticker": ticker,
                "company": TRACKED_TICKERS.get(ticker, data.get("name", "Unknown")),
                "cik": cik,
                "fileDate": file_date,
                "accession": accession,
                "docUrl": doc_url,
                "xmlUrl": xml_url,
                "insiderName": "",
                "shares": 0,
                "approxPrice": 0,
                "relationship": "",
            }
            
            all_form144.append(filing_entry)
            count_144 += 1
        
        if count_144 > 0:
            print(f"{count_144} Form 144 filings")
        else:
            print("none")
        
        time.sleep(RATE_LIMIT_DELAY)
    
    # Sort by filing date desc
    all_form144.sort(key=lambda x: x["fileDate"], reverse=True)
    
    print(f"\nTotal: {len(all_form144)} Form 144 filings found")
    return all_form144


def parse_form144_details(filings, max_parse=500):
    """
    Parse individual Form 144 XML documents to extract insider name, shares, value.
    Uses the raw XML URL (primary_doc.xml) directly.
    """
    print("\n" + "=" * 60)
    print(f"Parsing Form 144 details (up to {max_parse} filings)...")
    print("=" * 60)
    
    parsed_count = 0
    
    for i, filing in enumerate(filings[:max_parse]):
        # Use raw XML URL
        url = filing.get("xmlUrl", "")
        if not url:
            continue
        
        print(f"  [{i+1}/{min(len(filings), max_parse)}] {filing['ticker']} {filing['fileDate']}...", end=" ")
        
        resp = sec_get(url)
        if not resp or resp.status_code != 200:
            print("skip")
            time.sleep(RATE_LIMIT_DELAY)
            continue
        
        text = resp.text
        
        try:
            parsed = parse_form144_xml(text)
            
            if parsed and (parsed.get("name") or parsed.get("shares")):
                filing["insiderName"] = parsed.get("name", "")
                filing["shares"] = parsed.get("shares", 0)
                filing["approxPrice"] = parsed.get("price", 0)
                filing["relationship"] = parsed.get("relationship", "")
                filing["totalValue"] = parsed.get("totalValue", 0)
                filing["remarks"] = parsed.get("remarks", "")
                
                val_str = f"${filing['totalValue']:,.0f}" if filing['totalValue'] else "?"
                shares_str = f"{filing['shares']:,}" if filing['shares'] else "?"
                print(f"{filing['insiderName'] or '?'} | {shares_str} shares | {val_str}")
                parsed_count += 1
            else:
                print("no data parsed")
        except Exception as e:
            print(f"parse error: {e}")
        
        time.sleep(RATE_LIMIT_DELAY)
    
    print(f"\nParsed details for {parsed_count}/{min(len(filings), max_parse)} filings")
    return filings


def parse_form144_xml(text):
    """
    Parse Form 144 XML using actual EDGAR tag structure.
    
    Actual tags (from real filings):
      <nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold>Peter Thiel</...>
      <relationshipToIssuer>Officer</relationshipToIssuer>
      <noOfUnitsSold>2000000</noOfUnitsSold>
      <aggregateMarketValue>280000000</aggregateMarketValue>
      <approxSaleDate>03/02/2026</approxSaleDate>
      <remarks>...</remarks>
    """
    result = {}
    
    # ===== NAME =====
    # Primary: exact EDGAR tag
    name_match = re.search(
        r'<nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold>\s*([^<]+?)\s*</nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold>',
        text, re.IGNORECASE
    )
    if not name_match:
        # Fallback: shorter tag variants
        for pat in [
            r'<nameOfPerson[^>]*>\s*([^<]+?)\s*</nameOfPerson[^>]*>',
            r'<reportingOwnerName>\s*([^<]+?)\s*</reportingOwnerName>',
            r'<rptOwnerName>\s*([^<]+?)\s*</rptOwnerName>',
        ]:
            name_match = re.search(pat, text, re.IGNORECASE)
            if name_match:
                break
    
    if name_match:
        name = name_match.group(1).strip()
        name = re.sub(r'<[^>]+>', '', name).strip()
        if len(name) > 1 and len(name) < 100:
            result["name"] = name.title()
    
    # ===== SHARES (noOfUnitsSold) =====
    shares_match = re.search(
        r'<noOfUnitsSold>\s*([0-9,]+)\s*</noOfUnitsSold>',
        text, re.IGNORECASE
    )
    if not shares_match:
        for pat in [
            r'<noOfUnits[^>]*>\s*([0-9,]+)\s*</noOfUnits[^>]*>',
            r'<numberOfSharesOrUnits[^>]*>\s*([0-9,]+)\s*</numberOfSharesOrUnits>',
            r'<amountOfSecurities[^>]*>\s*([0-9,]+)\s*</amountOfSecurities[^>]*>',
        ]:
            shares_match = re.search(pat, text, re.IGNORECASE)
            if shares_match:
                break
    
    if shares_match:
        try:
            result["shares"] = int(shares_match.group(1).replace(",", ""))
        except ValueError:
            pass
    
    # ===== AGGREGATE MARKET VALUE =====
    value_match = re.search(
        r'<aggregateMarketValue>\s*([0-9,.]+)\s*</aggregateMarketValue>',
        text, re.IGNORECASE
    )
    if value_match:
        try:
            result["totalValue"] = float(value_match.group(1).replace(",", ""))
        except ValueError:
            pass
    
    # Calculate price per share from aggregate value
    if result.get("totalValue") and result.get("shares"):
        result["price"] = round(result["totalValue"] / result["shares"], 2)
    
    # ===== APPROXIMATE SALE PRICE (fallback if no aggregate value) =====
    if "price" not in result:
        price_match = re.search(
            r'<approxSalePrice>\s*\$?\s*([0-9,.]+)\s*</approxSalePrice>',
            text, re.IGNORECASE
        )
        if not price_match:
            price_match = re.search(
                r'<approximatePricePerUnit>\s*\$?\s*([0-9,.]+)\s*</approximatePricePerUnit>',
                text, re.IGNORECASE
            )
        if price_match:
            try:
                result["price"] = float(price_match.group(1).replace(",", ""))
            except ValueError:
                pass
    
    # ===== RELATIONSHIP =====
    rel_match = re.search(
        r'<relationshipToIssuer>\s*([^<]+?)\s*</relationshipToIssuer>',
        text, re.IGNORECASE
    )
    if rel_match:
        result["relationship"] = rel_match.group(1).strip()
    else:
        # Check boolean flags
        if re.search(r'<isOfficer>\s*(?:true|1|Y)\s*</isOfficer>', text, re.IGNORECASE):
            result["relationship"] = "Officer"
        elif re.search(r'<isDirector>\s*(?:true|1|Y)\s*</isDirector>', text, re.IGNORECASE):
            result["relationship"] = "Director"
        elif re.search(r'<isTenPercentOwner>\s*(?:true|1|Y)\s*</isTenPercentOwner>', text, re.IGNORECASE):
            result["relationship"] = "10% Owner"
    
    # ===== REMARKS =====
    remarks_match = re.search(
        r'<remarks>\s*([^<]+?)\s*</remarks>',
        text, re.DOTALL | re.IGNORECASE
    )
    if remarks_match:
        result["remarks"] = remarks_match.group(1).strip()[:500]
    
    # ===== CALCULATE TOTAL VALUE if not from aggregateMarketValue =====
    if "totalValue" not in result:
        shares = result.get("shares", 0)
        price = result.get("price", 0)
        result["totalValue"] = round(shares * price, 2) if shares and price else 0
    
    return result if result else None


def fetch_current_prices(tickers):
    """Fetch current stock prices for value estimation."""
    print("\n" + "=" * 60)
    print("Fetching current prices via Yahoo Finance...")
    print("=" * 60)
    
    prices = {}
    for i, sym in enumerate(tickers):
        print(f"  [{i+1}/{len(tickers)}] {sym}...", end=" ")
        try:
            ticker = yf.Ticker(sym)
            info = ticker.fast_info
            price = info.get("lastPrice", 0) or info.get("regularMarketPrice", 0)
            if price:
                prices[sym] = round(float(price), 2)
                print(f"${prices[sym]}")
            else:
                print("no price")
        except Exception as e:
            print(f"error: {e}")
        time.sleep(0.3)
    
    return prices


def build_form144_summary(filings, prices):
    """Build summary statistics for the dashboard."""
    now = datetime.date.today()
    
    # Time-based stats
    filings_7d = []
    filings_30d = []
    filings_90d = []
    
    for f in filings:
        try:
            fd = datetime.date.fromisoformat(f["fileDate"])
            delta = (now - fd).days
            if delta <= 7:
                filings_7d.append(f)
            if delta <= 30:
                filings_30d.append(f)
            if delta <= 90:
                filings_90d.append(f)
        except ValueError:
            pass
    
    # Estimate values using current prices where filing price is missing
    for f in filings:
        if not f.get("totalValue") and f.get("shares") and f["ticker"] in prices:
            f["approxPrice"] = prices[f["ticker"]]
            f["totalValue"] = f["shares"] * f["approxPrice"]
    
    # Top filings by value
    valued_filings = [f for f in filings if f.get("totalValue", 0) > 0]
    valued_filings.sort(key=lambda x: x["totalValue"], reverse=True)
    
    # Top companies by total filing value
    company_totals = {}
    for f in filings:
        sym = f["ticker"]
        company_totals.setdefault(sym, {"total": 0, "count": 0, "name": f["company"]})
        company_totals[sym]["total"] += f.get("totalValue", 0)
        company_totals[sym]["count"] += 1
    
    top_companies = sorted(company_totals.items(), key=lambda x: -x[1]["total"])[:10]
    
    # Top insiders
    insider_totals = {}
    for f in filings:
        name = f.get("insiderName", "")
        if not name:
            continue
        insider_totals.setdefault(name, {"total": 0, "count": 0, "ticker": f["ticker"], "company": f["company"]})
        insider_totals[name]["total"] += f.get("totalValue", 0)
        insider_totals[name]["count"] += 1
    
    top_insiders = sorted(insider_totals.items(), key=lambda x: -x[1]["total"])[:10]
    
    total_value = sum(f.get("totalValue", 0) for f in filings)
    
    return {
        "updated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totalFilings": len(filings),
        "filings7d": len(filings_7d),
        "filings30d": len(filings_30d),
        "filings90d": len(filings_90d),
        "totalValue": round(total_value, 2),
        "uniqueCompanies": len(set(f["ticker"] for f in filings)),
        "topCompanies": [[sym, data] for sym, data in top_companies],
        "topInsiders": [[name, data] for name, data in top_insiders],
        "topFilings": [
            {
                "ticker": f["ticker"],
                "company": f["company"],
                "insiderName": f.get("insiderName", ""),
                "shares": f.get("shares", 0),
                "approxPrice": f.get("approxPrice", 0),
                "totalValue": f.get("totalValue", 0),
                "fileDate": f["fileDate"],
                "relationship": f.get("relationship", ""),
            }
            for f in valued_filings[:20]
        ],
    }


def fetch_candles_for_form144(tickers):
    """Fetch price candles for Form 144 companies (reuses shared candles dir)."""
    print("\n" + "=" * 60)
    print("Fetching price candles via Yahoo Finance...")
    print("=" * 60)
    
    for i, sym in enumerate(tickers):
        candle_path = CANDLES_DIR / f"{sym}.json"
        
        # Skip if already fetched recently (by insider trading page)
        if candle_path.exists():
            mod_time = datetime.datetime.fromtimestamp(candle_path.stat().st_mtime)
            if (datetime.datetime.now() - mod_time).days < 1:
                print(f"  [{i+1}/{len(tickers)}] {sym}... cached (skip)")
                continue
        
        print(f"  [{i+1}/{len(tickers)}] {sym}...", end=" ")
        try:
            ticker = yf.Ticker(sym)
            df = ticker.history(period="200d", interval="1d")
            
            if df.empty:
                print("no data")
                continue
            
            candle = {
                "t": [int(ts.timestamp()) for ts in df.index],
                "c": [round(float(p), 2) for p in df["Close"]],
                "h": [round(float(p), 2) for p in df["High"]],
                "l": [round(float(p), 2) for p in df["Low"]],
            }
            
            with open(candle_path, "w") as f:
                json.dump(candle, f, separators=(",", ":"))
            print(f"{len(candle['c'])} days")
        except Exception as e:
            print(f"error: {e}")
        
        time.sleep(0.3)


def main():
    # Ensure directories exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CANDLES_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Fetch Form 144 filings from company submissions
    filings = fetch_form144_from_submissions()
    
    if not filings:
        print("\nNo Form 144 filings found. Trying EFTS search...")
        filings = fetch_form144_filings(days_back=90)
    
    if not filings:
        print("\nERROR: No Form 144 data available!")
        # Save empty data
        with open(DATA_DIR / "form144.json", "w") as f:
            json.dump([], f)
        with open(DATA_DIR / "form144_summary.json", "w") as f:
            json.dump({"updated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "totalFilings": 0}, f)
        return
    
    # 2. Parse filing details (insider name, shares, price)
    filings = parse_form144_details(filings, max_parse=500)
    
    # 3. Get current prices for value estimation
    active_tickers = sorted(set(f["ticker"] for f in filings))
    prices = fetch_current_prices(active_tickers)
    
    # 3.5 Fill in missing values using current prices
    for f in filings:
        if f.get("shares", 0) > 0 and not f.get("totalValue"):
            sym = f["ticker"]
            if sym in prices:
                f["approxPrice"] = prices[sym]
                f["totalValue"] = round(f["shares"] * prices[sym], 2)
    
    # 4. Save raw filings
    with open(DATA_DIR / "form144.json", "w") as f:
        json.dump(filings, f, separators=(",", ":"))
    print(f"\nSaved {len(filings)} filings to data/form144.json")
    
    # 5. Build & save summary
    summary = build_form144_summary(filings, prices)
    with open(DATA_DIR / "form144_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Saved summary to data/form144_summary.json")
    
    # 6. Fetch candles (shares candles dir with insider page)
    fetch_candles_for_form144(active_tickers)
    
    # Report
    print("\n" + "=" * 60)
    print("DONE - Form 144 Data Collection")
    print(f"  Total filings: {len(filings)}")
    print(f"  Unique companies: {len(active_tickers)}")
    print(f"  Total estimated value: ${summary['totalValue']:,.0f}")
    print(f"  Last 7 days: {summary['filings7d']} filings")
    print(f"  Last 30 days: {summary['filings30d']} filings")
    print("=" * 60)


if __name__ == "__main__":
    main()
