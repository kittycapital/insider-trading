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
            
            # Build EDGAR URL
            acc_clean = accession.replace("-", "")
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{primary_doc}" if primary_doc else ""
            
            filing_entry = {
                "ticker": ticker,
                "company": TRACKED_TICKERS.get(ticker, data.get("name", "Unknown")),
                "cik": cik,
                "fileDate": file_date,
                "accession": accession,
                "docUrl": doc_url,
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


def parse_form144_details(filings, max_parse=300):
    """
    Parse individual Form 144 documents to extract insider name, shares, price.
    First tries the filing index to find the XML doc, then parses it.
    """
    print("\n" + "=" * 60)
    print(f"Parsing Form 144 details (up to {max_parse} filings)...")
    print("=" * 60)
    
    parsed_count = 0
    
    for i, filing in enumerate(filings[:max_parse]):
        doc_url = filing.get("docUrl", "")
        if not doc_url:
            continue
        
        print(f"  [{i+1}/{min(len(filings), max_parse)}] {filing['ticker']} {filing['fileDate']}...", end=" ")
        
        # Try to get the filing index page first (to find the XML document)
        accession = filing.get("accession", "")
        cik = filing.get("cik", "")
        xml_url = None
        
        if accession and cik:
            acc_clean = accession.replace("-", "")
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/"
            resp = sec_get(index_url)
            if resp and resp.status_code == 200:
                # Look for XML document in the index
                idx_text = resp.text
                # Find .xml files that are Form 144 documents
                xml_matches = re.findall(r'href="([^"]+\.xml)"', idx_text, re.IGNORECASE)
                for xm in xml_matches:
                    if '144' in xm.lower() or 'primary' in xm.lower() or 'doc' in xm.lower():
                        xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{xm}"
                        break
                if not xml_url and xml_matches:
                    xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{xml_matches[0]}"
                time.sleep(RATE_LIMIT_DELAY)
        
        # Fetch the document (prefer XML, fallback to primary doc)
        fetch_url = xml_url or doc_url
        resp = sec_get(fetch_url)
        if not resp:
            print("skip")
            time.sleep(RATE_LIMIT_DELAY)
            continue
        
        text = resp.text
        
        try:
            parsed = parse_form144_doc(text)
            
            if parsed and (parsed.get("name") or parsed.get("shares")):
                filing["insiderName"] = parsed.get("name", "")
                filing["shares"] = parsed.get("shares", 0)
                filing["approxPrice"] = parsed.get("price", 0)
                filing["relationship"] = parsed.get("relationship", "")
                filing["totalValue"] = parsed.get("totalValue", 0)
                
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


def parse_form144_doc(text):
    """
    Parse Form 144 document - handles both XML and HTML formats.
    EDGAR Form 144 XML uses nested <value> tags for data.
    """
    result = {}
    
    # ===== EXTRACT NAME =====
    # Pattern 1: EDGAR XML with <value> wrapper
    # <nameOfPersonForWhoseAccountSecuritiesToBeSold><value>THIEL PETER</value>
    name_patterns_xml = [
        r'<nameOfPerson[^>]*>.*?<value>\s*([^<]+?)\s*</value>',
        r'<reportingOwner[Nn]ame>.*?<value>\s*([^<]+?)\s*</value>',
        r'<rptOwnerName>\s*([^<]+?)\s*</rptOwnerName>',
        r'<reportingPersonName>.*?<value>\s*([^<]+?)\s*</value>',
        r'<personName>.*?<value>\s*([^<]+?)\s*</value>',
    ]
    
    for pat in name_patterns_xml:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            # Clean up: remove any tag remnants or noise
            name = re.sub(r'<[^>]+>', '', name).strip()
            if len(name) > 2 and len(name) < 80:
                result["name"] = name.title()
                break
    
    # Pattern 2: HTML table - look for the specific cell after "Name of Person"
    if "name" not in result:
        # Match table structure: <td>Name of Person...</td><td>VALUE</td>
        html_name_pats = [
            r'Name of Person[^<]*?Account[^<]*?(?:Sold|Sale)[^<]*?</(?:td|div|span)>\s*<(?:td|div|span)[^>]*>\s*([A-Z][A-Za-z\s.\-\']+?)(?:\s*</(?:td|div|span)>)',
            r'(?:Reporting|Filing)\s*Person[^<]*</(?:td|div|span)>\s*<(?:td|div|span)[^>]*>\s*([A-Z][A-Za-z\s.\-\']+?)\s*</(?:td|div|span)>',
        ]
        for pat in html_name_pats:
            m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
            if m:
                name = re.sub(r'<[^>]+>', '', m.group(1)).strip()
                name = re.sub(r'\s+', ' ', name)
                # Filter out noise phrases
                noise = ['see ', 'the definition', 'paragraph', 'information', 'not only', 'person for']
                if not any(n in name.lower() for n in noise) and len(name) > 2 and len(name) < 80:
                    result["name"] = name.title()
                    break
    
    # ===== EXTRACT SHARES =====
    shares_patterns_xml = [
        r'<noOfUnitOrShares[^>]*>.*?<value>\s*([0-9,]+)\s*</value>',
        r'<numberOfSharesOrUnits[^>]*>.*?<value>\s*([0-9,]+)\s*</value>',
        r'<amountOfSecurities[^>]*>.*?<value>\s*([0-9,]+)\s*</value>',
        r'<securitiesToBeSold>.*?<noOfUnitOrShares[^>]*>.*?<value>\s*([0-9,]+)\s*</value>',
        # Direct XML without <value> wrapper
        r'<noOfUnitOrShares[^>]*>\s*([0-9,]+)\s*</noOfUnitOrShares>',
        r'<numberOfSharesOrUnits[^>]*>\s*([0-9,]+)\s*</numberOfSharesOrUnits>',
    ]
    
    for pat in shares_patterns_xml:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m:
            try:
                result["shares"] = int(m.group(1).replace(",", "").strip())
                break
            except ValueError:
                pass
    
    # HTML fallback for shares
    if "shares" not in result:
        shares_html_pats = [
            r'(?:Number|Amount|No\.?)\s*(?:of)?\s*(?:Shares|Units|Securities)[^<]*?</(?:td|div|span)>\s*<(?:td|div|span)[^>]*>\s*([0-9,]+)',
            r'(?:shares|units)\s*(?:to be sold)?[^<]*?</(?:td|div|span)>\s*<(?:td|div|span)[^>]*>\s*([0-9,]+)',
            r'>([0-9,]{3,})\s*(?:shares|units)',
        ]
        for pat in shares_html_pats:
            m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
            if m:
                try:
                    val = int(m.group(1).replace(",", ""))
                    if val > 0:
                        result["shares"] = val
                        break
                except ValueError:
                    pass
    
    # ===== EXTRACT PRICE =====
    price_patterns_xml = [
        r'<approx(?:imate)?(?:Sale)?Price[^>]*>.*?<value>\s*\$?\s*([0-9,.]+)\s*</value>',
        r'<pricePerUnit[^>]*>.*?<value>\s*\$?\s*([0-9,.]+)\s*</value>',
        r'<aggregateMarketValue[^>]*>.*?<value>\s*\$?\s*([0-9,.]+)\s*</value>',
        # Direct value
        r'<approx(?:imate)?(?:Sale)?Price[^>]*>\s*\$?\s*([0-9,.]+)\s*</',
        r'<pricePerUnit[^>]*>\s*\$?\s*([0-9,.]+)\s*</',
    ]
    
    for pat in price_patterns_xml:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m:
            try:
                price = float(m.group(1).replace(",", "").strip())
                if price > 0:
                    result["price"] = price
                    break
            except ValueError:
                pass
    
    # Check for aggregate market value (might be total, not per-share)
    if "price" not in result:
        agg_pats = [
            r'<aggregateMarketValue[^>]*>.*?<value>\s*\$?\s*([0-9,.]+)\s*</value>',
            r'[Aa]ggregate\s*[Mm]arket\s*[Vv]alue[^<]*?</(?:td|div|span)>\s*<(?:td|div|span)[^>]*>\s*\$?\s*([0-9,.]+)',
        ]
        for pat in agg_pats:
            m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    if val > 0 and result.get("shares", 0) > 0:
                        result["price"] = round(val / result["shares"], 2)
                        result["totalValue"] = val
                    elif val > 0:
                        result["totalValue"] = val
                    break
                except ValueError:
                    pass
    
    # HTML fallback for price
    if "price" not in result:
        price_html_pats = [
            r'(?:Approximate|Estimated)\s*(?:Sale\s*)?(?:Price|Value)[^<]*?</(?:td|div|span)>\s*<(?:td|div|span)[^>]*>\s*\$?\s*([0-9,.]+)',
            r'(?:Price|Value)\s*(?:per|/)\s*(?:share|unit)[^<]*?</(?:td|div|span)>\s*<(?:td|div|span)[^>]*>\s*\$?\s*([0-9,.]+)',
        ]
        for pat in price_html_pats:
            m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
            if m:
                try:
                    price = float(m.group(1).replace(",", ""))
                    if price > 0:
                        result["price"] = price
                        break
                except ValueError:
                    pass
    
    # ===== EXTRACT RELATIONSHIP =====
    rel_patterns = [
        r'<relationshipToIssuer>.*?<value>\s*([^<]+?)\s*</value>',
        r'<isOfficer>.*?<value>\s*(?:true|1|yes)\s*</value>',
        r'<isDirector>.*?<value>\s*(?:true|1|yes)\s*</value>',
        r'<isTenPercentOwner>.*?<value>\s*(?:true|1|yes)\s*</value>',
    ]
    
    for j, pat in enumerate(rel_patterns):
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m:
            if j == 0:
                result["relationship"] = m.group(1).strip()
            elif j == 1:
                result["relationship"] = "Officer"
            elif j == 2:
                result["relationship"] = "Director"
            elif j == 3:
                result["relationship"] = "10% Owner"
            break
    
    if "relationship" not in result:
        # HTML fallback
        rel_html = re.search(
            r'[Rr]elationship[^<]*?[Ii]ssuer[^<]*?</(?:td|div|span)>\s*<(?:td|div|span)[^>]*>\s*([^<]+)',
            text, re.DOTALL
        )
        if rel_html:
            rel = rel_html.group(1).strip()
            if len(rel) < 40:
                result["relationship"] = rel
    
    # ===== CALCULATE TOTAL VALUE =====
    if "totalValue" not in result:
        shares = result.get("shares", 0)
        price = result.get("price", 0)
        if shares and price:
            result["totalValue"] = round(shares * price, 2)
        else:
            result["totalValue"] = 0
    
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
