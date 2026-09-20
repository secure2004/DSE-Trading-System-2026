# DSE + SME + DSEX / DSES / DS30 DAILY EOD CLOUD DATA FEEDER
# No TradingView and no lxml dependency.

import argparse
import logging
import re
import sys
import time
import urllib3
from datetime import datetime, time as dt_time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
LOG_FILE = PROJECT_DIR / "dse_data_feeder.log"

DSE_INDEX_URL = "https://www.dsebd.org/index.php"
DSE_ARCHIVE_URL = "https://www.dsebd.org/day_end_archive.php"
SME_HTML_URL = "https://sme.dsebd.org/sme_latest_share_price_scroll_by_ltp.php"
LANKABANGLA_PRICEFILE_URL = "https://www.lankabd.com/Home/PriceFile"

BD_TZ = ZoneInfo("Asia/Dhaka")
DATE_FMT = "%Y-%m-%d"
FEED_TIME = dt_time(14, 40)
RETRY_TIME = dt_time(14, 45)
MAX_ATTEMPTS = 2

DSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.dsebd.org/",
    "Connection": "keep-alive",
}
SME_HEADERS = {**DSE_HEADERS, "Referer": "https://sme.dsebd.org/"}
LANKABANGLA_HEADERS = {**DSE_HEADERS, "Referer": "https://www.lankabd.com/"}

DATA_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(filename=str(LOG_FILE), level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def safe_float(value):
    try:
        s = str(value).strip().replace(",", "")
        return 0.0 if s in ("", "--", "N/A", "NA", "None", "nan", "NaN") else float(s)
    except Exception:
        return 0.0


def safe_int(value):
    try:
        s = str(value).strip().replace(",", "")
        return 0 if s in ("", "--", "N/A", "NA", "None", "nan", "NaN") else int(float(s))
    except Exception:
        return 0


def clean_text(value):
    return re.sub(r"\s+", " ", str(value).strip())


def norm(value):
    return clean_text(value).upper()


def now_bd():
    return datetime.now(BD_TZ)


def today_bd():
    return now_bd().strftime(DATE_FMT)


def find_column(columns, candidates):
    normalized = {norm(c): c for c in columns}
    for candidate in candidates:
        if norm(candidate) in normalized:
            return normalized[norm(candidate)]
    for col in columns:
        for candidate in candidates:
            if norm(candidate) in norm(col):
                return col
    return None


def valid_ticker(value):
    return bool(str(value).strip().upper())


def get_dse_official_date():
    print("\n" + "=" * 70)
    print("0. GETTING OFFICIAL DSE TRADING DATE")
    print("=" * 70)
    try:
        response = requests.get(DSE_INDEX_URL, headers=DSE_HEADERS, timeout=30, verify=False)
        response.raise_for_status()
        print("DSE Date URL:", DSE_INDEX_URL)
        print("DSE Date Response:", len(response.content), "bytes")
        match = re.search(r"Last\s+update\s+on\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", response.text, re.I)
        if not match:
            raise RuntimeError("DSE official trading date not found")
        month_name, day, year = match.groups()
        parsed = None
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                parsed = datetime.strptime(f"{month_name} {day} {year}", fmt)
                break
            except ValueError:
                pass
        if parsed is None:
            raise RuntimeError("Could not parse DSE official date")
        result = parsed.strftime(DATE_FMT)
        print("Official DSE Trading Date:", result)
        return result
    except Exception as exc:
        logging.exception("Official DSE date failed")
        print("DSE DATE ERROR:", exc)
        return None


def date_gate_only():
    current_date = today_bd()
    dse_date = get_dse_official_date()
    print("\n" + "=" * 70)
    print("DATE GATE")
    print("=" * 70)
    print("Current Bangladesh Date :", current_date)
    print("DSE Official Trading Date:", dse_date)
    if dse_date is None:
        print("STATUS: FAILED")
        return 1
    if current_date != dse_date:
        print("STATUS: STOP")
        print("Dates do not match. No market data will be collected.")
        return 0
    print("STATUS: MATCH")
    return 0


def scrape_dse_day_end_archive(trading_date, sme_tickers):
    print("\n" + "=" * 70)
    print("1. DSE STOCK DATA - DAY END ARCHIVE")
    print("=" * 70)
    params = {"startDate": trading_date, "endDate": trading_date, "inst": "All Instrument", "archive": "data"}
    try:
        response = requests.get(DSE_ARCHIVE_URL, params=params, headers=DSE_HEADERS, timeout=45, verify=False)
        response.raise_for_status()
        print("DSE Archive URL:", response.url)
        print("HTTP Status:", response.status_code)
        print("Response Size:", len(response.content), "bytes")
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as exc:
        logging.exception("DSE Day End Archive request failed")
        print("DSE ARCHIVE ERROR:", repr(exc))
        return None

    target = None
    target_index = None
    tables = soup.find_all("table")
    print("HTML Tables Found:", len(tables))
    required_headers = {"TRADING CODE", "HIGH", "LOW", "OPENP*", "CLOSEP*", "VOLUME"}
    for i, table in enumerate(tables):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [norm(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])]
        if required_headers.issubset(set(headers)):
            target, target_index = table, i
            break
    if target is None:
        print("ERROR: DSE Day End Archive stock table not found")
        return None
    print("DSE Stock Table Found: TABLE", target_index)
    rows = target.find_all("tr")
    if len(rows) < 2:
        return None
    headers = [norm(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])]
    positions = {h: i for i, h in enumerate(headers)}
    def pos(*names):
        for name in names:
            if norm(name) in positions:
                return positions[norm(name)]
        return None
    ticker_pos = pos("TRADING CODE", "TRADINGCODE")
    date_pos = pos("DATE")
    open_pos = pos("OPENP*", "OPENP", "OPEN")
    high_pos = pos("HIGH")
    low_pos = pos("LOW")
    close_pos = pos("CLOSEP*", "CLOSEP", "CLOSE")
    volume_pos = pos("VOLUME")
    print("\nDSE Archive Detected Columns:")
    print("Ticker :", "TRADING CODE" if ticker_pos is not None else None)
    print("Date   :", "DATE" if date_pos is not None else None)
    print("Open   :", "OPENP*" if open_pos is not None else None)
    print("High   :", "HIGH" if high_pos is not None else None)
    print("Low    :", "LOW" if low_pos is not None else None)
    print("Close  :", "CLOSEP*" if close_pos is not None else None)
    print("Volume :", "VOLUME" if volume_pos is not None else None)
    required = [ticker_pos, open_pos, high_pos, low_pos, close_pos, volume_pos]
    if any(x is None for x in required):
        print("ERROR: Required DSE archive columns missing")
        return None

    records = []
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) <= max(required):
            continue
        values = [clean_text(c.get_text(" ", strip=True)) for c in cells]
        ticker = values[ticker_pos].strip().upper()
        if not valid_ticker(ticker):
            continue
        if date_pos is not None and values[date_pos].strip() and values[date_pos].strip() != trading_date:
            continue
        records.append({"Ticker": ticker, "Date": trading_date, "Open": safe_float(values[open_pos]), "High": safe_float(values[high_pos]), "Low": safe_float(values[low_pos]), "Close": safe_float(values[close_pos]), "Volume": safe_int(values[volume_pos])})
    if not records:
        print("ERROR: No valid DSE stock rows extracted")
        return None
    result = pd.DataFrame(records)
    sme_set = {str(x).strip().upper() for x in sme_tickers if valid_ticker(x)}
    before_sme = len(result)
    if sme_set:
        result = result[~result["Ticker"].isin(sme_set)].copy()
    removed_sme = before_sme - len(result)
    result.drop_duplicates(subset=["Ticker"], keep="first", inplace=True)
    result.sort_values("Ticker", inplace=True)
    result.reset_index(drop=True, inplace=True)
    if result.empty:
        return None
    print("DSE Stock Table Rows:", len(rows) - 1)
    print("DSE Archive Symbols:", len(result))
    print("SME Symbols Removed:", removed_sme)
    print("Zero Volume Retained:", int((result["Volume"] == 0).sum()))
    print("Zero Close Retained:", int((result["Close"] == 0).sum()))
    print("\nDSE ARCHIVE FIRST 5:")
    print(result.head(5).to_string(index=False))
    return result[["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]]


def scrape_sme_data(trading_date):
    print("\n" + "=" * 70)
    print("2. SME DSE DATA")
    print("=" * 70)
    try:
        response = requests.get(SME_HTML_URL, headers=SME_HEADERS, timeout=30, verify=False)
        response.raise_for_status()
        print("SME URL:", SME_HTML_URL)
        print("HTTP Status:", response.status_code)
        print("Response Size:", len(response.content), "bytes")
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")
        print("SME Tables Found:", len(tables))
        target = None
        for table in tables:
            text = norm(table.get_text(" ", strip=True))
            headers = [norm(x.get_text(" ", strip=True)) for x in table.find_all("th")]
            if "TRADING CODE" in headers or "TRADINGCODE" in headers or "TRADING CODE" in text:
                target = table
                break
        if target is None:
            raise RuntimeError("SME table not found")
        rows = target.find_all("tr")
        if len(rows) < 2:
            raise RuntimeError("SME data rows not found")
        headers = [x.get_text(" ", strip=True).upper() for x in rows[0].find_all(["th", "td"])]
        records = []
        for row in rows[1:]:
            values = [x.get_text(" ", strip=True) for x in row.find_all("td")]
            if len(values) == len(headers):
                records.append(values)
        if not records:
            raise RuntimeError("SME rows could not be parsed")
        df = pd.DataFrame(records, columns=headers)
        ticker_col = find_column(df.columns, ["TRADING CODE", "TRADINGCODE"])
        ltp_col = find_column(df.columns, ["LTP*", "LTP"])
        ycp_col = find_column(df.columns, ["YCP*", "YCP"])
        high_col = find_column(df.columns, ["HIGH"])
        low_col = find_column(df.columns, ["LOW"])
        close_col = find_column(df.columns, ["CLOSEP*", "CLOSEP", "CLOSE"])
        volume_col = find_column(df.columns, ["VOLUME"])
        print("\nSME DETECTED COLUMNS:")
        print("Ticker :", ticker_col)
        print("LTP    :", ltp_col)
        print("YCP    :", ycp_col)
        print("High   :", high_col)
        print("Low    :", low_col)
        print("Close  :", close_col)
        print("Volume :", volume_col)
        if ticker_col is None:
            raise RuntimeError("SME ticker column missing")
        if close_col is None and ltp_col is None:
            raise RuntimeError("SME close/LTP column missing")
        source_close = close_col or ltp_col
        result = pd.DataFrame()
        result["Ticker"] = df[ticker_col].astype(str).str.strip().str.upper()
        result["Date"] = trading_date
        result["Open"] = df[ycp_col].apply(safe_float) if ycp_col else df[source_close].apply(safe_float)
        result["High"] = df[high_col].apply(safe_float) if high_col else result["Open"]
        result["Low"] = df[low_col].apply(safe_float) if low_col else result["Open"]
        result["Close"] = df[source_close].apply(safe_float)
        result["Volume"] = df[volume_col].apply(safe_int) if volume_col else 0
        result = result[result["Ticker"].apply(valid_ticker)].copy()
        result.drop_duplicates(subset=["Ticker"], keep="first", inplace=True)
        result.sort_values("Ticker", inplace=True)
        result.reset_index(drop=True, inplace=True)
        if result.empty:
            raise RuntimeError("SME returned zero valid symbols")
        print("\nSME Close Source:", source_close)
        print("SME Symbols:", len(result))
        print("SME Trading Date:", trading_date)
        return result[["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]]
    except Exception as exc:
        logging.exception("SME collection failed")
        print("SME ERROR:", exc)
        return None


def combine_dse_sme(dse_df, sme_df):
    print("\n" + "=" * 70)
    print("3. COMBINING DSE + SME")
    print("=" * 70)
    if dse_df is None or sme_df is None:
        return None
    combined = pd.concat([dse_df, sme_df], ignore_index=True)
    before = len(combined)
    combined.drop_duplicates(subset=["Ticker"], keep="last", inplace=True)
    print("DSE Symbols       :", len(dse_df))
    print("SME Symbols       :", len(sme_df))
    print("Duplicates Removed:", before - len(combined))
    combined.sort_values("Ticker", inplace=True)
    combined.reset_index(drop=True, inplace=True)
    print("Combined Symbols  :", len(combined))
    if combined.empty:
        return None
    return combined[["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]]


def get_lankabangla_indices(trading_date):
    print("\n" + "=" * 70)
    print("4. LANKABANGLA DSEX / DSES / DS30")
    print("=" * 70)
    url = f"{LANKABANGLA_PRICEFILE_URL}?dateinp={trading_date}"
    try:
        response = requests.get(url, headers=LANKABANGLA_HEADERS, timeout=45, verify=False)
        response.raise_for_status()
        print("LankaBangla URL:", url)
        print("LankaBangla HTTP Status:", response.status_code)
        print("LankaBangla Response Size:", len(response.content), "bytes")
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")
        print("LankaBangla Tables Found:", len(tables))
        target = None
        required_headers = {"SYMBOL", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"}
        for table in tables:
            rows = table.find_all("tr")
            if not rows:
                continue
            headers = [clean_text(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])]
            if required_headers.issubset({norm(x) for x in headers}):
                target = table
                break
        if target is None:
            raise RuntimeError("LankaBangla PriceFile table not found")
        rows = target.find_all("tr")
        if len(rows) < 2:
            raise RuntimeError("LankaBangla PriceFile contains no data rows")
        headers = [clean_text(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])]
        print("LankaBangla columns:", headers)
        positions = {norm(h): i for i, h in enumerate(headers)}
        def pos(*names):
            for name in names:
                if norm(name) in positions:
                    return positions[norm(name)]
            return None
        symbol_pos = pos("SYMBOL")
        date_pos = pos("DATE")
        open_pos = pos("OPEN")
        high_pos = pos("HIGH")
        low_pos = pos("LOW")
        close_pos = pos("CLOSE")
        volume_pos = pos("VOLUME")
        required = [symbol_pos, date_pos, open_pos, high_pos, low_pos, close_pos, volume_pos]
        if any(x is None for x in required):
            raise RuntimeError("Required LankaBangla columns missing")
        indices = {}
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) <= max(required):
                continue
            values = [clean_text(c.get_text(" ", strip=True)) for c in cells]
            symbol = values[symbol_pos].strip().upper()
            if symbol not in ("DSEX", "DSES", "DS30"):
                continue
            row_date = values[date_pos].strip()
            try:
                parsed_date = pd.to_datetime(row_date).strftime(DATE_FMT)
            except Exception:
                parsed_date = row_date
            if parsed_date != trading_date:
                continue
            indices[symbol] = {
                "Open": safe_float(values[open_pos]),
                "High": safe_float(values[high_pos]),
                "Low": safe_float(values[low_pos]),
                "Close": safe_float(values[close_pos]),
                "Volume": safe_int(values[volume_pos]),
            }
        for symbol in ("DSEX", "DSES", "DS30"):
            if symbol not in indices:
                raise RuntimeError(f"LankaBangla {symbol} not found for {trading_date}")
            for field in ("Open", "High", "Low", "Close"):
                if indices[symbol][field] <= 0:
                    raise RuntimeError(f"Invalid LankaBangla {symbol} {field}")
        market_volume = indices["DSEX"]["Volume"]
        if market_volume <= 0:
            raise RuntimeError("Invalid LankaBangla market volume")
        indices["DSEX"]["Volume"] = market_volume
        indices["DSES"]["Volume"] = 0
        indices["DS30"]["Volume"] = 0
        print("\nLankaBangla Index Summary:")
        for symbol in ("DSEX", "DSES", "DS30"):
            d = indices[symbol]
            print(f"{symbol}: O={d['Open']} H={d['High']} L={d['Low']} C={d['Close']} V={d['Volume']}")
        print("\nLankaBangla DSEX / DSES / DS30 validation PASSED.")
        return {"date": trading_date, "indices": indices}
    except Exception as exc:
        logging.exception("LankaBangla index collection failed")
        print("LANKABANGLA INDEX ERROR:", exc)
        return None


def build_final_dataset(stocks, lankabangla_indices, trading_date):
    if stocks is None or stocks.empty:
        raise RuntimeError("No DSE/SME stock data")
    if lankabangla_indices is None:
        raise RuntimeError("LankaBangla index data missing")
    if lankabangla_indices["date"] != trading_date:
        raise RuntimeError("LankaBangla index date does not match official trading date")
    expected = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
    if list(stocks.columns) != expected:
        raise RuntimeError(f"Unexpected stock columns: {list(stocks.columns)}")
    if stocks["Ticker"].duplicated().any():
        raise RuntimeError("Duplicate stock ticker found")
    if not (stocks["Date"].astype(str) == trading_date).all():
        raise RuntimeError("Stock date mismatch")
    if (stocks["Volume"] < 0).any():
        raise RuntimeError("Negative stock volume found")
    indices = lankabangla_indices["indices"]
    for symbol in ("DSEX", "DSES", "DS30"):
        if symbol not in indices:
            raise RuntimeError(f"Missing index: {symbol}")
        for field in ("Open", "High", "Low", "Close"):
            if indices[symbol][field] <= 0:
                raise RuntimeError(f"Invalid LankaBangla {symbol} {field}")
    if indices["DSEX"]["Volume"] <= 0:
        raise RuntimeError("Invalid DSEX market volume")
    index_rows = []
    for symbol in ("DSEX", "DSES", "DS30"):
        d = indices[symbol]
        index_rows.append([symbol, trading_date, d["Open"], d["High"], d["Low"], d["Close"], d["Volume"]])
    final_df = pd.concat([stocks, pd.DataFrame(index_rows, columns=expected)], ignore_index=True)
    if final_df["Ticker"].duplicated().any():
        raise RuntimeError("Duplicate ticker/index in final dataset")
    if len(final_df) <= len(stocks):
        raise RuntimeError("Index rows were not appended")
    return final_df


def publish_daily_csv(final_df, trading_date):
    directory = DATA_DIR / trading_date[:4] / trading_date[5:7]
    directory.mkdir(parents=True, exist_ok=True)
    daily_file = directory / f"DSE_EOD_{trading_date}.csv"
    temp_file = directory / f".DSE_EOD_{trading_date}.csv.tmp"
    final_df.to_csv(temp_file, index=False, header=False)
    temp_file.replace(daily_file)
    print("\n" + "=" * 70)
    print("CLOUD ARCHIVE PUBLISHED")
    print("=" * 70)
    print("Daily CSV:", daily_file)
    print("Rows:", len(final_df))
    print("\nINDEX SUMMARY")
    for symbol in ("DSEX", "DSES", "DS30"):
        row = final_df[final_df["Ticker"] == symbol].iloc[0]
        print(f"{symbol}: O={row['Open']} H={row['High']} L={row['Low']} C={row['Close']} V={row['Volume']}")
    logging.info("Daily EOD published: %s | rows=%d", daily_file, len(final_df))
    return daily_file


def collect_and_publish():
    current_date = today_bd()
    official_date = get_dse_official_date()
    if official_date is None:
        raise RuntimeError("Could not determine official DSE trading date")
    print("\nCurrent Bangladesh Date :", current_date)
    print("DSE Official Trading Date:", official_date)
    if current_date != official_date:
        print("\n" + "=" * 70)
        print("DATE GATE: STOP")
        print("=" * 70)
        print("Current date does not match DSE official date.")
        print("No market data will be collected.")
        print("No CSV will be written.")
        return False
    trading_date = official_date
    sme_df = scrape_sme_data(trading_date)
    if sme_df is None:
        raise RuntimeError("SME collection failed")
    dse_df = scrape_dse_day_end_archive(trading_date, sme_tickers=sme_df["Ticker"].tolist())
    if dse_df is None:
        raise RuntimeError("DSE Day End Archive collection failed")
    stocks = combine_dse_sme(dse_df, sme_df)
    if stocks is None:
        raise RuntimeError("DSE + SME combination failed")
    lankabangla_indices = get_lankabangla_indices(trading_date)
    if lankabangla_indices is None:
        raise RuntimeError("LankaBangla index collection failed")
    print("\n" + "=" * 70)
    print("FINAL DATASET VALIDATION")
    print("=" * 70)
    final_df = build_final_dataset(stocks, lankabangla_indices, trading_date)
    print("FINAL VALIDATION: PASSED")
    print("Stock Rows:", len(stocks))
    print("Index Rows:", len(final_df) - len(stocks))
    print("Final Rows:", len(final_df))
    print("DSEX Close:", lankabangla_indices["indices"]["DSEX"]["Close"])
    print("DSEX Volume:", lankabangla_indices["indices"]["DSEX"]["Volume"])
    daily_file = publish_daily_csv(final_df, trading_date)
    print("\n" + "=" * 70)
    print("DSE + SME + DSEX/DSES/DS30 COMPLETE")
    print("=" * 70)
    print("Official Trading Date:", trading_date)
    print("Daily CSV:", daily_file)
    print("DONE")
    return True


def seconds_until(time_value):
    current = now_bd()
    target = current.replace(hour=time_value.hour, minute=time_value.minute, second=0, microsecond=0)
    if target <= current:
        return 0
    return (target - current).total_seconds()


def run_normal():
    current_date = today_bd()
    official_date = get_dse_official_date()
    if official_date is None:
        print("Unable to determine DSE date. STOP.")
        return 1
    print("\nCurrent Bangladesh Date :", current_date)
    print("DSE Official Trading Date:", official_date)
    if current_date != official_date:
        print("\n" + "=" * 70)
        print("DATE GATE: STOP")
        print("=" * 70)
        print("DSE official date does not match today's date.")
        print("No collection.")
        return 0
    wait_seconds = seconds_until(FEED_TIME)
    if wait_seconds > 0:
        print("\n" + "=" * 70)
        print("DATE MATCHED - WAITING FOR 2:40 PM")
        print("=" * 70)
        print("Bangladesh Time:", now_bd().strftime("%I:%M:%S %p"))
        print("Scheduled feed : 02:40 PM")
        time.sleep(wait_seconds)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        print(f"\n========== EOD ATTEMPT {attempt}/{MAX_ATTEMPTS} ==========")
        try:
            current_date = today_bd()
            official_date = get_dse_official_date()
            if official_date is None:
                raise RuntimeError("Could not determine official DSE date")
            if current_date != official_date:
                print("DATE GATE CHANGED: STOP")
                return 0
            if collect_and_publish():
                return 0
        except Exception as exc:
            logging.exception("EOD attempt %d failed", attempt)
            print("\nATTEMPT FAILED:", exc)
        if attempt == 1:
            wait_seconds = seconds_until(RETRY_TIME)
            print("\n" + "=" * 70)
            print("FIRST ATTEMPT FAILED")
            print("=" * 70)
            if wait_seconds > 0:
                print("Retrying at 02:45 PM...")
                time.sleep(wait_seconds)
            else:
                print("2:45 PM has passed; performing the single retry now.")
    print("\n" + "=" * 70)
    print("CLOUD FEEDER FAILED")
    print("=" * 70)
    print("No new daily CSV was published.")
    return 1


def main():
    parser = argparse.ArgumentParser(description="DSE + SME + DSEX/DSES/DS30 daily EOD cloud feeder")
    parser.add_argument("--check-date", action="store_true", help="Check current Bangladesh date against DSE official date only.")
    args = parser.parse_args()
    if args.check_date:
        return date_gate_only()
    return run_normal()


if __name__ == "__main__":
    sys.exit(main())
