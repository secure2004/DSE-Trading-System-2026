# ================================================================
# DSE + SME + DSEX / DSES / DS30 DAILY EOD CLOUD DATA FEEDER
# ================================================================
#
# AGREED DESIGN
#
# START
#   -> Current Bangladesh local date
#   -> DSE official date
#       MATCH     -> wait until 2:40 PM -> collect
#       NO MATCH  -> STOP
#
# 2:40 PM
#   -> DSE Day End Archive: DSE stocks OHLC + Volume
#   -> Existing SME source: SME stocks
#   -> Combine DSE + SME
#   -> LankaBangla: DSEX / DSES / DS30
#   -> Final validation
#   -> data/YYYY/MM/DSE_EOD_YYYY-MM-DD.csv
#
# 2:40 failure -> retry once at 2:45 PM -> failure = STOP
#
# NO:
#   - intraday DSE stock source
#   - 09:30 reset block
#   - HTS/holiday lookup
#   - hard-coded weekday/weekend rule
#   - fixed ticker count
#   - Close=0 filter
#   - Volume=0 filter
#   - latest CSV
#
# FINAL CSV ORDER:
#   1. DSEX
#   2. DS30
#   3. DSES
#   4. DSE tickers - alphabetical
#   5. SME tickers - alphabetical
# ================================================================

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


# ================================================================
# CONFIGURATION
# ================================================================

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR.parent
LOG_FILE = PROJECT_DIR / "dse_data_feeder.log"

DSE_MARKET_API_URL = "https://www.dsebd.org/api/live/market"
DSE_ARCHIVE_API_URL = "https://dse.com.bd/api/live/data-archive/day-end"
SME_HTML_URL = "https://sme.dsebd.org/sme_latest_share_price_scroll_by_ltp.php"

LANKABANGLA_PRICEFILE_URL = (
    "https://www.lankabd.com/Home/PriceFile?dateinp={}"
)

BD_TZ = ZoneInfo("Asia/Dhaka")
DATE_FMT = "%Y-%m-%d"

FEED_TIME = dt_time(14, 40)
RETRY_TIME = dt_time(14, 45)

MAX_ATTEMPTS = 2

DSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.dsebd.org/",
    "Connection": "keep-alive",
}

SME_HEADERS = {
    **DSE_HEADERS,
    "Referer": "https://sme.dsebd.org/",
}

EXPECTED_COLUMNS = [
    "Ticker",
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
]

# Suppress only the urllib3 warning caused by verify=False.
urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)

DATA_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# ================================================================
# GENERAL HELPERS
# ================================================================

def safe_float(value):
    try:
        s = str(value).strip().replace(",", "")

        if s in (
            "",
            "--",
            "N/A",
            "NA",
            "None",
            "nan",
            "NaN",
        ):
            return 0.0

        return float(s)

    except Exception:
        return 0.0


def safe_int(value):
    try:
        s = str(value).strip().replace(",", "")

        if s in (
            "",
            "--",
            "N/A",
            "NA",
            "None",
            "nan",
            "NaN",
        ):
            return 0

        return int(float(s))

    except Exception:
        return 0


def clean_text(value):
    return re.sub(
        r"\s+",
        " ",
        str(value).strip(),
    )


def norm(value):
    return clean_text(value).upper()


def now_bd():
    return datetime.now(BD_TZ)


def today_bd():
    return now_bd().strftime(DATE_FMT)


def find_column(columns, candidates):
    normalized = {
        norm(c): c
        for c in columns
    }

    for candidate in candidates:
        key = norm(candidate)

        if key in normalized:
            return normalized[key]

    for col in columns:
        c = norm(col)

        for candidate in candidates:
            if norm(candidate) in c:
                return col

    return None


def valid_ticker(value):
    ticker = str(value).strip().upper()

    return bool(ticker)


# ================================================================
# 0. OFFICIAL DSE DATE
# ================================================================

def get_dse_official_date():
    print()
    print("=" * 70)
    print("0. GETTING OFFICIAL DSE TRADING DATE")
    print("=" * 70)

    try:
        response = requests.get(
            DSE_MARKET_API_URL,
            headers=DSE_HEADERS,
            timeout=30,
            verify=False,
        )

        response.raise_for_status()
        payload = response.json()

        session = payload.get("session", {})
        result = (
            session.get("date")
            or payload.get("date")
            or payload.get("tradingDate")
        )

        if not result:
            raise RuntimeError(
                "DSE market API did not return session date"
            )

        # Normalize ISO datetime/date to YYYY-MM-DD.
        result = str(result).strip()[:10]

        datetime.strptime(result, DATE_FMT)

        print("DSE Date API:", DSE_MARKET_API_URL)
        print("HTTP Status:", response.status_code)
        print("DSE Date Response:", len(response.content), "bytes")
        print("Official DSE Trading Date:", result)

        return result

    except Exception as exc:
        logging.exception("Official DSE date failed")
        print("DSE DATE ERROR:", exc)
        return None


def date_gate_only():
    current_date = today_bd()
    dse_date = get_dse_official_date()

    print()
    print("=" * 70)
    print("DATE GATE")
    print("=" * 70)

    print(
        "Current Bangladesh Date :",
        current_date,
    )

    print(
        "DSE Official Trading Date:",
        dse_date,
    )

    if dse_date is None:
        print("STATUS: FAILED")
        return 1

    if current_date != dse_date:
        print("STATUS: STOP")

        print(
            "Dates do not match. "
            "No market data will be collected."
        )

        return 0

    print("STATUS: MATCH")

    print(
        "DSE official date matches "
        "today's Bangladesh date."
    )

    return 0


# ================================================================
# 1. DSE STOCKS - DAY END ARCHIVE
# ================================================================

def scrape_dse_day_end_archive(
    trading_date,
    sme_tickers,
):
    print()
    print("=" * 70)
    print("1. DSE STOCK DATA - DAY END ARCHIVE API")
    print("=" * 70)

    def first_value(row, *names):
        """Return the first non-missing value from common API field names."""
        for name in names:
            if isinstance(row, dict) and name in row:
                value = row.get(name)
                if value is not None and str(value).strip() != "":
                    return value
        return None

    def extract_rows(payload):
        """Accept the common pagination envelope shapes used by the new API."""
        if isinstance(payload, list):
            return payload, {}

        if not isinstance(payload, dict):
            return [], {}

        for key in ("data", "rows", "items", "results", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return value, payload
            if isinstance(value, dict):
                for nested_key in ("data", "rows", "items", "results", "records"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return nested, value

        return [], payload

    def pagination_info(meta):
        """Read total/page metadata when supplied; otherwise use row-count discovery."""
        if not isinstance(meta, dict):
            return None, None

        candidates = [meta]
        for key in ("pagination", "meta", "pageInfo"):
            if isinstance(meta.get(key), dict):
                candidates.append(meta[key])

        total = pages = None
        for obj in candidates:
            if total is None:
                for k in ("total", "totalRows", "totalCount", "count"):
                    if obj.get(k) is not None:
                        try:
                            total = int(obj[k])
                            break
                        except (TypeError, ValueError):
                            pass
            if pages is None:
                for k in ("totalPages", "pages", "pageCount"):
                    if obj.get(k) is not None:
                        try:
                            pages = int(obj[k])
                            break
                        except (TypeError, ValueError):
                            pass
        return total, pages

    all_rows = []
    page = 1
    total_expected = None
    total_pages = None

    while True:
        params = {
            "from": trading_date,
            "to": trading_date,
            "page": page,
        }

        try:
            response = requests.get(
                DSE_ARCHIVE_API_URL,
                params=params,
                headers={
                    **DSE_HEADERS,
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.dsebd.org/data-archives",
                },
                timeout=45,
                verify=False,
            )
            response.raise_for_status()
            payload = response.json()

        except Exception as exc:
            logging.exception(
                "DSE Day End Archive API request failed; page=%d",
                page,
            )
            print("DSE ARCHIVE API ERROR:", repr(exc))
            return None

        rows_page, meta = extract_rows(payload)
        total, pages = pagination_info(meta)
        if total is not None:
            total_expected = total
        if pages is not None:
            total_pages = pages

        print(
            f"DSE Archive API Page {page}: HTTP {response.status_code}, "
            f"{len(response.content)} bytes, rows={len(rows_page)}"
        )

        if not rows_page:
            break

        all_rows.extend(rows_page)

        # Prefer explicit API pagination metadata. The page UI currently
        # reports 501-634 on page 2 of 2, so this also works if metadata is
        # absent by continuing until a short/empty page is returned.
        if total_pages is not None and page >= total_pages:
            break

        if total_expected is not None and len(all_rows) >= total_expected:
            break

        if len(rows_page) < 500:
            break

        page += 1

        if page > 100:
            raise RuntimeError("DSE archive pagination exceeded 100 pages")

    if not all_rows:
        print("ERROR: No DSE archive API rows returned")
        logging.error("DSE archive API returned no rows")
        return None

    print("DSE Archive API Total Raw Rows:", len(all_rows))
    if total_expected is not None:
        print("DSE Archive API Expected Rows:", total_expected)
    if total_pages is not None:
        print("DSE Archive API Total Pages:", total_pages)

    records = []

    for row in all_rows:
        if not isinstance(row, dict):
            continue

        ticker = first_value(
            row,
            "tradingCode", "trading_code", "TRADING CODE",
            "code", "symbol", "ticker",
        )
        ticker = str(ticker or "").strip().upper()

        if not valid_ticker(ticker):
            continue

        row_date = first_value(
            row, "date", "DATE", "tradingDate", "trading_date"
        )
        row_date = str(row_date or "").strip()[:10]
        if row_date and row_date != trading_date:
            continue

        # New archive field names follow the page columns. Keep fallbacks
        # for minor API naming variations.
        records.append({
            "Ticker": ticker,
            "Date": trading_date,
            "Open": safe_float(first_value(
                row, "openp", "openP", "OPENP*", "open", "Open"
            )),
            "High": safe_float(first_value(
                row, "high", "HIGH", "High"
            )),
            "Low": safe_float(first_value(
                row, "low", "LOW", "Low"
            )),
            "Close": safe_float(first_value(
                row, "closep", "closeP", "CLOSEP*", "close", "Close", "ltp", "LTP*"
            )),
            "Volume": safe_int(first_value(
                row, "volume", "VOLUME", "Volume"
            )),
        })

    if not records:
        print("ERROR: No valid DSE stock rows extracted")
        logging.error("DSE archive API contained no valid stock rows")
        return None

    result = pd.DataFrame(records)

    # SME remains authoritative.
    sme_set = {
        str(x).strip().upper()
        for x in sme_tickers
        if valid_ticker(x)
    }

    before_sme = len(result)
    if sme_set:
        result = result[~result["Ticker"].isin(sme_set)].copy()
    removed_sme = before_sme - len(result)

    result.drop_duplicates(subset=["Ticker"], keep="first", inplace=True)
    result.sort_values("Ticker", inplace=True)
    result.reset_index(drop=True, inplace=True)

    if result.empty:
        print("ERROR: DSE archive returned zero DSE stocks")
        return None

    print("DSE Archive Symbols:", len(result))
    print("SME Symbols Removed:", removed_sme)
    print("Zero Volume Retained:", int((result["Volume"] == 0).sum()))
    print("Zero Close Retained:", int((result["Close"] == 0).sum()))

    print()
    print("DSE ARCHIVE FIRST 5:")
    print(result.head(5).to_string(index=False))

    return result[EXPECTED_COLUMNS]


# ================================================================
# 2. SME - EXISTING SOURCE
# ================================================================

def scrape_sme_data(trading_date):
    print()
    print("=" * 70)
    print("2. SME DSE DATA")
    print("=" * 70)

    try:
        response = requests.get(
            SME_HTML_URL,
            headers=SME_HEADERS,
            timeout=30,
            verify=False,
        )

        response.raise_for_status()

        print(
            "SME URL:",
            SME_HTML_URL,
        )

        print(
            "HTTP Status:",
            response.status_code,
        )

        print(
            "Response Size:",
            len(response.content),
            "bytes",
        )

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        tables = soup.find_all("table")

        print(
            "SME Tables Found:",
            len(tables),
        )

        target = None

        for table in tables:

            text = norm(
                table.get_text(
                    " ",
                    strip=True,
                )
            )

            headers = [
                norm(
                    x.get_text(
                        " ",
                        strip=True,
                    )
                )
                for x in table.find_all("th")
            ]

            if (
                "TRADING CODE" in headers
                or "TRADINGCODE" in headers
                or "TRADING CODE" in text
            ):
                target = table
                break

        if target is None:
            raise RuntimeError(
                "SME table not found"
            )

        rows = target.find_all("tr")

        if len(rows) < 2:
            raise RuntimeError(
                "SME data rows not found"
            )

        headers = [
            x.get_text(
                " ",
                strip=True,
            ).upper()
            for x in rows[0].find_all(
                ["th", "td"]
            )
        ]

        records = []

        for row in rows[1:]:

            values = [
                x.get_text(
                    " ",
                    strip=True,
                )
                for x in row.find_all("td")
            ]

            if len(values) == len(headers):
                records.append(values)

        if not records:
            raise RuntimeError(
                "SME rows could not be parsed"
            )

        df = pd.DataFrame(
            records,
            columns=headers,
        )

        ticker_col = find_column(
            df.columns,
            [
                "TRADING CODE",
                "TRADINGCODE",
            ],
        )

        ltp_col = find_column(
            df.columns,
            [
                "LTP*",
                "LTP",
            ],
        )

        ycp_col = find_column(
            df.columns,
            [
                "YCP*",
                "YCP",
            ],
        )

        high_col = find_column(
            df.columns,
            ["HIGH"],
        )

        low_col = find_column(
            df.columns,
            ["LOW"],
        )

        close_col = find_column(
            df.columns,
            [
                "CLOSEP*",
                "CLOSEP",
                "CLOSE",
            ],
        )

        volume_col = find_column(
            df.columns,
            ["VOLUME"],
        )

        print()
        print("SME DETECTED COLUMNS:")

        print(
            "Ticker :",
            ticker_col,
        )

        print(
            "LTP    :",
            ltp_col,
        )

        print(
            "YCP    :",
            ycp_col,
        )

        print(
            "High   :",
            high_col,
        )

        print(
            "Low    :",
            low_col,
        )

        print(
            "Close  :",
            close_col,
        )

        print(
            "Volume :",
            volume_col,
        )

        if ticker_col is None:
            raise RuntimeError(
                "SME ticker column missing"
            )

        if (
            close_col is None
            and ltp_col is None
        ):
            raise RuntimeError(
                "SME close/LTP column missing"
            )

        # Prefer CLOSEP* when present;
        # otherwise LTP*.
        source_close = (
            close_col
            or ltp_col
        )

        result = pd.DataFrame()

        result["Ticker"] = (
            df[ticker_col]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        result["Date"] = trading_date

        # Preserve existing SME seven-column structure.
        if ycp_col:
            result["Open"] = (
                df[ycp_col]
                .apply(safe_float)
            )
        else:
            result["Open"] = (
                df[source_close]
                .apply(safe_float)
            )

        if high_col:
            result["High"] = (
                df[high_col]
                .apply(safe_float)
            )
        else:
            result["High"] = result["Open"]

        if low_col:
            result["Low"] = (
                df[low_col]
                .apply(safe_float)
            )
        else:
            result["Low"] = result["Open"]

        result["Close"] = (
            df[source_close]
            .apply(safe_float)
        )

        if volume_col:
            result["Volume"] = (
                df[volume_col]
                .apply(safe_int)
            )
        else:
            result["Volume"] = 0

        result = result[
            result["Ticker"].apply(
                valid_ticker
            )
        ].copy()

        # NO Close=0 filter.
        # NO Volume=0 filter.

        result.drop_duplicates(
            subset=["Ticker"],
            keep="first",
            inplace=True,
        )

        result.sort_values(
            "Ticker",
            inplace=True,
        )

        result.reset_index(
            drop=True,
            inplace=True,
        )

        if result.empty:
            raise RuntimeError(
                "SME returned zero "
                "valid symbols"
            )

        print()
        print(
            "SME Close Source:",
            source_close,
        )

        print(
            "SME Symbols:",
            len(result),
        )

        print(
            "SME Trading Date:",
            trading_date,
        )

        return result[EXPECTED_COLUMNS]

    except Exception as exc:

        logging.exception(
            "SME collection failed"
        )

        print(
            "SME ERROR:",
            exc,
        )

        return None


# ================================================================
# 3. COMBINE DSE + SME
# ================================================================

def combine_dse_sme(
    dse_df,
    sme_df,
):
    print()
    print("=" * 70)
    print("3. COMBINING DSE + SME")
    print("=" * 70)

    if (
        dse_df is None
        or sme_df is None
    ):
        return None

    # Keep DSE and SME groups separate.
    dse = dse_df.copy()
    sme = sme_df.copy()

    # Normalize ticker names.
    dse["Ticker"] = (
        dse["Ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    sme["Ticker"] = (
        sme["Ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # SME remains authoritative if a ticker
    # appears in both sources.
    sme_tickers = set(
        sme["Ticker"]
    )

    dse_before = len(dse)

    dse = dse[
        ~dse["Ticker"].isin(
            sme_tickers
        )
    ].copy()

    removed_overlap = (
        dse_before - len(dse)
    )

    # Remove duplicate tickers within
    # each source.
    dse.drop_duplicates(
        subset=["Ticker"],
        keep="first",
        inplace=True,
    )

    sme.drop_duplicates(
        subset=["Ticker"],
        keep="first",
        inplace=True,
    )

    # Sort each group independently.
    dse.sort_values(
        by="Ticker",
        ascending=True,
        inplace=True,
    )

    sme.sort_values(
        by="Ticker",
        ascending=True,
        inplace=True,
    )

    dse.reset_index(
        drop=True,
        inplace=True,
    )

    sme.reset_index(
        drop=True,
        inplace=True,
    )

    # DSE FIRST, SME SECOND.
    combined = pd.concat(
        [dse, sme],
        ignore_index=True,
    )

    print(
        "DSE Symbols       :",
        len(dse),
    )

    print(