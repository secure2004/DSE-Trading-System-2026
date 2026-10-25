# ================================================================
# DSE + SME + ATB + DSEX / DSES / DS30 DAILY EOD CLOUD DATA FEEDER
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
#   -> ATB source: ATB stocks
#   -> Combine DSE + SME + ATB
#   -> LankaBangla: DSEX / DSES / DS30
#   -> Final validation
#   -> YYYY/MM/DSE_EOD_YYYY-MM-DD.csv
#
# 2:40 failure -> retry at 2:45 PM -> final retry at 2:50 PM -> failure = STOP
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
#   6. ATB tickers - alphabetical
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
DATA_DIR = PROJECT_DIR
LOG_FILE = PROJECT_DIR / "dse_data_feeder.log"

DSE_MARKET_API_URL = "https://www.dsebd.org/api/live/market"
DSE_ARCHIVE_API_URL = "https://dse.com.bd/api/live/data-archive/day-end"
SME_HTML_URL = "https://sme.dsebd.org/sme_latest_share_price_scroll_by_ltp.php"
ATB_HTML_URL = "https://atb.dsebd.org/latest_share_price_scroll_l.php"

LANKABANGLA_PRICEFILE_URL = (
    "https://www.lankabd.com/Home/PriceFile?dateinp={}"
)

BD_TZ = ZoneInfo("Asia/Dhaka")
DATE_FMT = "%Y-%m-%d"

FEED_TIME = dt_time(14, 40)
RETRY_TIME = dt_time(14, 45)
FINAL_RETRY_TIME = dt_time(14, 50)

MAX_ATTEMPTS = 3

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

ATB_HEADERS = {
    **DSE_HEADERS,
    "Referer": "https://atb.dsebd.org/",
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
# 3. ATB DSE DATA
# ================================================================

def scrape_atb_data(trading_date):
    print()
    print("=" * 70)
    print("3. ATB DSE DATA")
    print("=" * 70)

    try:
        response = requests.get(
            ATB_HTML_URL,
            headers=ATB_HEADERS,
            timeout=30,
            verify=False,
        )

        response.raise_for_status()

        print("ATB URL:", ATB_HTML_URL)
        print("HTTP Status:", response.status_code)
        print("Response Size:", len(response.content), "bytes")

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        tables = soup.find_all("table")

        print("ATB Tables Found:", len(tables))

        target = None

        # Find the ATB table by its actual column names rather than
        # relying on a fixed table number.
        for table in tables:
            headers = [
                norm(x.get_text(" ", strip=True))
                for x in table.find_all("th")
            ]

            header_set = set(headers)

            if (
                "TRADING CODE" in header_set
                and "LTP*" in header_set
                and "HIGH" in header_set
                and "LOW" in header_set
                and "CLOSEP*" in header_set
                and "YCP*" in header_set
                and "VOLUME" in header_set
            ):
                target = table
                break

        if target is None:
            raise RuntimeError(
                "ATB table with expected columns was not found"
            )

        rows = target.find_all("tr")

        if len(rows) < 2:
            raise RuntimeError(
                "ATB data rows not found"
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
                "ATB rows could not be parsed"
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

        ycp_col = find_column(
            df.columns,
            [
                "YCP*",
                "YCP",
            ],
        )

        volume_col = find_column(
            df.columns,
            ["VOLUME"],
        )

        print()
        print("ATB DETECTED COLUMNS:")
        print("Ticker :", ticker_col)
        print("LTP    :", ltp_col)
        print("YCP    :", ycp_col)
        print("High   :", high_col)
        print("Low    :", low_col)
        print("Close  :", close_col)
        print("Volume :", volume_col)

        if ticker_col is None:
            raise RuntimeError(
                "ATB ticker column missing"
            )

        if (
            ycp_col is None
            or high_col is None
            or low_col is None
            or close_col is None
            or volume_col is None
        ):
            raise RuntimeError(
                "ATB required OHLC/Volume columns missing"
            )

        result = pd.DataFrame()

        result["Ticker"] = (
            df[ticker_col]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        result["Date"] = trading_date

        # OPTION A:
        # ATB does not publish a separate today's OPEN column.
        # Use YCP* (previous close) in the unified Open field
        # so the archive keeps the existing 7-column structure.
        result["Open"] = (
            df[ycp_col]
            .apply(safe_float)
        )

        result["High"] = (
            df[high_col]
            .apply(safe_float)
        )

        result["Low"] = (
            df[low_col]
            .apply(safe_float)
        )

        result["Close"] = (
            df[close_col]
            .apply(safe_float)
        )

        result["Volume"] = (
            df[volume_col]
            .apply(safe_int)
        )

        result = result[
            result["Ticker"].apply(
                valid_ticker
            )
        ].copy()

        # Keep zero-volume and zero-price rows, consistent with
        # the existing DSE/SME design.
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
                "ATB returned zero valid symbols"
            )

        print()
        print(
            "ATB Close Source:",
            close_col,
        )

        print(
            "ATB Open Source:",
            ycp_col,
            "(Option A)",
        )

        print(
            "ATB Symbols:",
            len(result),
        )

        print(
            "ATB Trading Date:",
            trading_date,
        )

        print()
        print("ATB FIRST 5:")
        print(
            result.head(5).to_string(
                index=False
            )
        )

        return result[EXPECTED_COLUMNS]

    except Exception as exc:
        logging.exception(
            "ATB collection failed"
        )

        print(
            "ATB ERROR:",
            exc,
        )

        return None


# ================================================================
# 4. COMBINE DSE + SME + ATB
# ================================================================

def combine_dse_sme_atb(
    dse_df,
    sme_df,
    atb_df,
):
    print()
    print("=" * 70)
    print("4. COMBINING DSE + SME + ATB")
    print("=" * 70)

    if (
        dse_df is None
        or sme_df is None
        or atb_df is None
    ):
        return None

    # Keep DSE and SME groups separate.
    dse = dse_df.copy()
    sme = sme_df.copy()
    atb = atb_df.copy()

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

    atb["Ticker"] = (
        atb["Ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # SME remains authoritative if a ticker
    # appears in both DSE and SME.
    sme_tickers = set(sme["Ticker"])

    dse_before = len(dse)

    dse = dse[
        ~dse["Ticker"].isin(
            sme_tickers
        )
    ].copy()

    removed_dse_sme_overlap = (
        dse_before - len(dse)
    )

    # ATB is appended as its own source group.
    # Do not silently overwrite a DSE/SME ticker.
    dse_sme_tickers = set(dse["Ticker"]) | set(sme["Ticker"])

    atb_overlap = (
        dse_sme_tickers
        & set(atb["Ticker"])
    )

    if atb_overlap:
        raise RuntimeError(
            "ATB ticker collision with DSE/SME: "
            + ", ".join(sorted(atb_overlap))
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

    atb.drop_duplicates(
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

    # DSE FIRST, SME SECOND, ATB THIRD.
    combined = pd.concat(
        [dse, sme, atb],
        ignore_index=True,
    )

    print(
        "DSE Symbols       :",
        len(dse),
    )

    print(
        "SME Symbols       :",
        len(sme),
    )

    print(
        "ATB Symbols       :",
        len(atb),
    )

    print(
        "DSE/SME Duplicates Removed:",
        removed_dse_sme_overlap,
    )

    print(
        "ATB Collisions     :",
        len(atb_overlap),
    )

    print(
        "Combined Symbols  :",
        len(combined),
    )

    if combined.empty:
        return None

    print()
    print("DSE FIRST TICKERS:")
    print(
        dse["Ticker"]
        .head(5)
        .tolist()
    )

    print()
    print("DSE LAST TICKERS:")
    print(
        dse["Ticker"]
        .tail(5)
        .tolist()
    )

    print()
    print("SME FIRST TICKERS:")
    print(
        sme["Ticker"]
        .head(5)
        .tolist()
    )

    print()
    print("SME LAST TICKERS:")
    print(
        sme["Ticker"]
        .tail(5)
        .tolist()
    )

    print()
    print("ATB FIRST TICKERS:")
    print(
        atb["Ticker"]
        .head(5)
        .tolist()
    )

    print()
    print("ATB LAST TICKERS:")
    print(
        atb["Ticker"]
        .tail(5)
        .tolist()
    )

    return combined[EXPECTED_COLUMNS]


# ================================================================
# 5. LANKABANGLA DSEX / DSES / DS30 INDEX DATA
# ================================================================

def get_lankabangla_indices(
    trading_date,
):
    print()
    print("=" * 70)
    print("4. LANKABANGLA DSEX / DSES / DS30")
    print("=" * 70)

    url = LANKABANGLA_PRICEFILE_URL.format(
        trading_date
    )

    try:
        print(
            "LankaBangla URL:",
            url,
        )

        response = requests.get(
            url,
            headers=DSE_HEADERS,
            timeout=45,
            verify=False,
        )

        response.raise_for_status()

        print(
            "LankaBangla HTTP Status:",
            response.status_code,
        )

        print(
            "LankaBangla Response Size:",
            len(response.content),
            "bytes",
        )

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        table = soup.find(
            "table",
            id="pricefiletable",
        )

        if table is None:
            raise RuntimeError(
                "LankaBangla pricefiletable "
                "not found"
            )

        rows = table.find_all("tr")

        if len(rows) < 2:
            raise RuntimeError(
                "LankaBangla PriceFile "
                "contains no data rows"
            )

        # --------------------------------------------------------
        # Detect columns from PriceFile header.
        # --------------------------------------------------------

        header_cells = rows[0].find_all(
            ["th", "td"]
        )

        headers = [
            clean_text(
                cell.get_text(
                    " ",
                    strip=True,
                )
            )
            for cell in header_cells
        ]

        print(
            "LankaBangla columns:",
            headers,
        )

        normalized_headers = [
            norm(x)
            for x in headers
        ]

        def get_position(*names):
            for name in names:
                key = norm(name)

                if key in normalized_headers:
                    return normalized_headers.index(
                        key
                    )

            return None

        symbol_pos = get_position(
            "SYMBOL"
        )

        date_pos = get_position(
            "DATE"
        )

        open_pos = get_position(
            "OPEN"
        )

        high_pos = get_position(
            "HIGH"
        )

        low_pos = get_position(
            "LOW"
        )

        close_pos = get_position(
            "CLOSE"
        )

        volume_pos = get_position(
            "VOLUME"
        )

        required_positions = [
            symbol_pos,
            date_pos,
            open_pos,
            high_pos,
            low_pos,
            close_pos,
            volume_pos,
        ]

        if any(
            pos is None
            for pos in required_positions
        ):
            raise RuntimeError(
                "Required LankaBangla "
                "columns are missing"
            )

        # --------------------------------------------------------
        # Extract only DSEX / DSES / DS30.
        # --------------------------------------------------------

        wanted = {
            "DSEX",
            "DSES",
            "DS30",
        }

        records = []

        for row in rows[1:]:

            cells = row.find_all("td")

            if len(cells) <= max(
                required_positions
            ):
                continue

            values = [
                clean_text(
                    cell.get_text(
                        " ",
                        strip=True,
                    )
                )
                for cell in cells
            ]

            symbol = (
                values[symbol_pos]
                .strip()
                .upper()
            )

            if symbol not in wanted:
                continue

            source_date = clean_text(
                values[date_pos]
            )

            parsed_source_date = pd.to_datetime(
                source_date,
                errors="coerce",
                dayfirst=False,
            )

            if pd.isna(
                parsed_source_date
            ):
                parsed_source_date = pd.to_datetime(
                    source_date,
                    errors="coerce",
                    dayfirst=True,
                )

            if pd.isna(
                parsed_source_date
            ):
                raise RuntimeError(
                    f"Invalid LankaBangla date "
                    f"for {symbol}: "
                    f"{source_date}"
                )

            parsed_date = (
                parsed_source_date
                .strftime(DATE_FMT)
            )

            if parsed_date != trading_date:
                raise RuntimeError(
                    f"LankaBangla date mismatch "
                    f"for {symbol}: "
                    f"expected {trading_date}, "
                    f"got {parsed_date}"
                )

            records.append(
                {
                    "Ticker": symbol,
                    "Date": parsed_date,
                    "Open": safe_float(
                        values[open_pos]
                    ),
                    "High": safe_float(
                        values[high_pos]
                    ),
                    "Low": safe_float(
                        values[low_pos]
                    ),
                    "Close": safe_float(
                        values[close_pos]
                    ),
                    "Volume": safe_int(
                        values[volume_pos]
                    ),
                }
            )

        if not records:
            raise RuntimeError(
                "DSEX/DSES/DS30 not found "
                "in LankaBangla PriceFile"
            )

        result = pd.DataFrame(
            records
        )

        # --------------------------------------------------------
        # Exactly one row per index.
        # --------------------------------------------------------

        if result["Ticker"].duplicated().any():

            duplicates = result.loc[
                result["Ticker"].duplicated(),
                "Ticker",
            ].tolist()

            raise RuntimeError(
                "Duplicate LankaBangla "
                "index rows found: "
                + ", ".join(duplicates)
            )

        missing = (
            wanted
            - set(result["Ticker"])
        )

        if missing:
            raise RuntimeError(
                "Missing LankaBangla indexes: "
                + ", ".join(
                    sorted(missing)
                )
            )

        if len(result) != 3:
            raise RuntimeError(
                "Expected 3 LankaBangla "
                f"index rows, got {len(result)}"
            )

        # --------------------------------------------------------
        # Validate OHLC and volume.
        # --------------------------------------------------------

        for _, row in result.iterrows():

            symbol = row["Ticker"]

            for field in (
                "Open",
                "High",
                "Low",
                "Close",
            ):

                if (
                    pd.isna(row[field])
                    or row[field] <= 0
                ):
                    raise RuntimeError(
                        f"Invalid {symbol} "
                        f"{field}: "
                        f"{row[field]}"
                    )

            if row["High"] < row["Low"]:
                raise RuntimeError(
                    f"Invalid {symbol}: "
                    f"High < Low"
                )

            if not (
                row["Low"]
                <= row["Open"]
                <= row["High"]
            ):
                raise RuntimeError(
                    f"Invalid {symbol}: "
                    f"Open outside "
                    f"High/Low range"
                )

            if not (
                row["Low"]
                <= row["Close"]
                <= row["High"]
            ):
                raise RuntimeError(
                    f"Invalid {symbol}: "
                    f"Close outside "
                    f"High/Low range"
                )

            if row["Volume"] < 0:
                raise RuntimeError(
                    f"Invalid {symbol}: "
                    f"negative volume"
                )

        # --------------------------------------------------------
        # Volume convention.
        #
        # LankaBangla PriceFile provides the same
        # market volume for DSEX, DSES and DS30.
        #
        # Keep market volume only on DSEX.
        # DSES and DS30 receive Volume = 0.
        # --------------------------------------------------------

        result.loc[
            result["Ticker"].isin(
                ["DSES", "DS30"]
            ),
            "Volume",
        ] = 0

        result["Volume"] = (
            result["Volume"]
            .fillna(0)
            .astype("int64")
        )

        # --------------------------------------------------------
        # Standard index order.
        #
        # IMPORTANT:
        # Final CSV requirement is:
        # DSEX, DS30, DSES
        # --------------------------------------------------------

        index_order = [
            "DSEX",
            "DS30",
            "DSES",
        ]

        result["Ticker"] = pd.Categorical(
            result["Ticker"],
            categories=index_order,
            ordered=True,
        )

        result = (
            result
            .sort_values("Ticker")
            .reset_index(drop=True)
        )

        result["Ticker"] = (
            result["Ticker"]
            .astype(str)
        )

        result = result[
            EXPECTED_COLUMNS
        ]

        print()
        print(
            "LankaBangla Index Summary:"
        )

        for _, row in result.iterrows():

            print(
                f"{row['Ticker']}: "
                f"O={row['Open']:.5f} "
                f"H={row['High']:.5f} "
                f"L={row['Low']:.5f} "
                f"C={row['Close']:.5f} "
                f"V={int(row['Volume'])}"
            )

        print()
        print(
            "LankaBangla DSEX / DSES / "
            "DS30 validation PASSED."
        )

        return result

    except Exception as exc:

        logging.exception(
            "LankaBangla index collection failed"
        )

        print(
            "LANKABANGLA INDEX ERROR:",
            exc,
        )

        return None


# ================================================================
# 6. FINAL DATASET
# ================================================================

def build_final_dataset(
    stocks,
    index_df,
    trading_date,
):
    if stocks is None or stocks.empty:
        raise RuntimeError(
            "No DSE/SME/ATB stock data"
        )

    if index_df is None or index_df.empty:
        raise RuntimeError(
            "LankaBangla index data missing"
        )

    expected = EXPECTED_COLUMNS

    if list(stocks.columns) != expected:
        raise RuntimeError(
            "Unexpected stock columns: "
            f"{list(stocks.columns)}"
        )

    if list(index_df.columns) != expected:
        raise RuntimeError(
            "Unexpected index columns: "
            f"{list(index_df.columns)}"
        )

    # ------------------------------------------------------------
    # STOCK VALIDATION
    # ------------------------------------------------------------

    if stocks["Ticker"].duplicated().any():
        raise RuntimeError(
            "Duplicate stock ticker found"
        )

    if not (
        stocks["Date"].astype(str)
        == trading_date
    ).all():
        raise RuntimeError(
            "Stock date mismatch"
        )

    # ------------------------------------------------------------
    # INDEX VALIDATION
    # ------------------------------------------------------------

    if index_df["Ticker"].duplicated().any():
        raise RuntimeError(
            "Duplicate index ticker found"
        )

    if not (
        index_df["Date"].astype(str)
        == trading_date
    ).all():
        raise RuntimeError(
            "Index date mismatch"
        )

    expected_indexes = {
        "DSEX",
        "DS30",
        "DSES",
    }

    actual_indexes = set(
        index_df["Ticker"]
    )

    if actual_indexes != expected_indexes:
        raise RuntimeError(
            "Index set mismatch: "
            f"expected {expected_indexes}, "
            f"got {actual_indexes}"
        )

    if len(index_df) != 3:
        raise RuntimeError(
            "Expected exactly 3 index rows, "
            f"got {len(index_df)}"
        )

    # ------------------------------------------------------------
    # STOCK VOLUME VALIDATION
    # ------------------------------------------------------------

    if (stocks["Volume"] < 0).any():
        raise RuntimeError(
            "Negative stock volume found"
        )

    # ------------------------------------------------------------
    # INDEX VOLUME VALIDATION
    # ------------------------------------------------------------

    if (index_df["Volume"] < 0).any():
        raise RuntimeError(
            "Negative index volume found"
        )

    # ------------------------------------------------------------
    # VALIDATE INDEX OHLC
    # ------------------------------------------------------------

    for _, row in index_df.iterrows():

        symbol = row["Ticker"]

        for field in (
            "Open",
            "High",
            "Low",
            "Close",
        ):

            if (
                pd.isna(row[field])
                or row[field] <= 0
            ):
                raise RuntimeError(
                    f"Invalid final index "
                    f"{symbol} {field}"
                )

        if row["High"] < row["Low"]:
            raise RuntimeError(
                f"Invalid final index "
                f"{symbol}: High < Low"
            )

        if not (
            row["Low"]
            <= row["Open"]
            <= row["High"]
        ):
            raise RuntimeError(
                f"Invalid final index "
                f"{symbol}: "
                f"Open outside range"
            )

        if not (
            row["Low"]
            <= row["Close"]
            <= row["High"]
        ):
            raise RuntimeError(
                f"Invalid final index "
                f"{symbol}: "
                f"Close outside range"
            )

    # ------------------------------------------------------------
    # PREVENT STOCK / INDEX COLLISION
    # ------------------------------------------------------------

    stock_index_overlap = (
        set(stocks["Ticker"])
        & expected_indexes
    )

    if stock_index_overlap:
        raise RuntimeError(
            "Stock/index ticker collision: "
            + ", ".join(
                sorted(
                    stock_index_overlap
                )
            )
        )

    # ------------------------------------------------------------
    # FORCE EXACT INDEX ORDER
    #
    # DSEX
    # DS30
    # DSES
    # ------------------------------------------------------------

    index_order = [
        "DSEX",
        "DS30",
        "DSES",
    ]

    index_df = (
        index_df
        .set_index("Ticker")
        .loc[index_order]
        .reset_index()
    )

    # ------------------------------------------------------------
    # STOCK ORDER
    #
    # combine_dse_sme_atb() already creates:
    #
    # DSE alphabetical
    # SME alphabetical
    #
    # Do NOT globally sort here because that would mix
    # DSE and SME groups.
    # ------------------------------------------------------------

    stocks = stocks.copy()

    # ------------------------------------------------------------
    # CREATE INDEX ROWS IN EXACT REQUIRED ORDER
    # ------------------------------------------------------------

    index_rows = []

    for symbol in (
        "DSEX",
        "DS30",
        "DSES",
    ):

        data = index_df[
            index_df["Ticker"] == symbol
        ].iloc[0]

        index_rows.append(
            [
                symbol,
                trading_date,
                data["Open"],
                data["High"],
                data["Low"],
                data["Close"],
                data["Volume"],
            ]
        )

    index_rows_df = pd.DataFrame(
        index_rows,
        columns=expected,
    )

    # ------------------------------------------------------------
    # FINAL ORDER:
    #
    # 1. DSEX
    # 2. DS30
    # 3. DSES
    # 4. DSE tickers
    # 5. SME tickers
    # ------------------------------------------------------------

    final_df = pd.concat(
        [
            index_rows_df,
            stocks,
        ],
        ignore_index=True,
    )

    # ------------------------------------------------------------
    # FINAL DUPLICATE VALIDATION
    # ------------------------------------------------------------

    if final_df["Ticker"].duplicated().any():
        raise RuntimeError(
            "Duplicate ticker/index "
            "in final dataset"
        )

    expected_rows = len(stocks) + 3

    if len(final_df) != expected_rows:
        raise RuntimeError(
            "Final row-count validation failed: "
            f"expected {expected_rows}, "
            f"got {len(final_df)}"
        )

    if (
        final_df["Ticker"].nunique()
        != len(final_df)
    ):
        raise RuntimeError(
            "Final dataset contains "
            "duplicate tickers"
        )

    # ------------------------------------------------------------
    # VERIFY EXACT TOP 3 ORDER
    # ------------------------------------------------------------

    actual_top_three = (
        final_df["Ticker"]
        .head(3)
        .tolist()
    )

    expected_top_three = [
        "DSEX",
        "DS30",
        "DSES",
    ]

    if actual_top_three != expected_top_three:
        raise RuntimeError(
            "Final index order validation failed: "
            f"expected {expected_top_three}, "
            f"got {actual_top_three}"
        )

    # ------------------------------------------------------------
    # FINAL ORDER CHECK
    #
    # Do NOT globally sort or validate the combined DSE + SME
    # ticker list. The required structure is intentionally:
    #
    # DSE tickers - alphabetical
    # SME tickers - alphabetical
    #
    # Therefore the transition from the last DSE ticker to the
    # first SME ticker is not expected to be globally alphabetical.
    #
    # combine_dse_sme_atb() already sorts each group independently.
    # ------------------------------------------------------------

    print()
    print("FINAL CSV ORDER VALIDATION: PASSED")
    print("1. DSEX")
    print("2. DS30")
    print("3. DSES")
    print("4. DSE tickers - alphabetical")
    print("5. SME tickers - alphabetical")
    print("6. ATB tickers - alphabetical")

    print()
    print("FIRST 10 FINAL ROWS:")

    print(
        final_df.head(10).to_string(
            index=False
        )
    )

    print()
    print("LAST 10 FINAL ROWS:")

    print(
        final_df.tail(10).to_string(
            index=False
        )
    )

    return final_df[expected]


# ================================================================
# 7. PUBLISH DAILY CSV ONLY
# ================================================================

def publish_daily_csv(
    final_df,
    trading_date,
):
    year = trading_date[:4]
    month = trading_date[5:7]

    directory = (
        DATA_DIR
        / year
        / month
    )

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    daily_file = (
        directory
        / f"DSE_EOD_{trading_date}.csv"
    )

    temp_file = (
        directory
        / f".DSE_EOD_{trading_date}.csv.tmp"
    )

    final_df.to_csv(
        temp_file,
        index=False,
        header=False,
    )

    temp_file.replace(
        daily_file
    )

    print()
    print("=" * 70)
    print("CLOUD ARCHIVE PUBLISHED")
    print("=" * 70)

    print(
        "Daily CSV:",
        daily_file,
    )

    print(
        "Rows:",
        len(final_df),
    )

    print()
    print("INDEX SUMMARY")

    for symbol in (
        "DSEX",
        "DS30",
        "DSES",
    ):

        row = final_df[
            final_df["Ticker"] == symbol
        ].iloc[0]

        print(
            f"{symbol}: "
            f"O={row['Open']} "
            f"H={row['High']} "
            f"L={row['Low']} "
            f"C={row['Close']} "
            f"V={row['Volume']}"
        )

    logging.info(
        "Daily EOD published: %s | rows=%d",
        daily_file,
        len(final_df),
    )

    return daily_file


# ================================================================
# COMPLETE COLLECTION
# ================================================================

def collect_and_publish():
    # ------------------------------------------------------------
    # Date gate
    # ------------------------------------------------------------

    current_date = today_bd()
    official_date = (
        get_dse_official_date()
    )

    if official_date is None:
        raise RuntimeError(
            "Could not determine official "
            "DSE trading date"
        )

    print()
    print(
        "Current Bangladesh Date :",
        current_date,
    )

    print(
        "DSE Official Trading Date:",
        official_date,
    )

    if current_date != official_date:

        print()
        print("=" * 70)
        print("DATE GATE: STOP")
        print("=" * 70)

        print(
            "Current date does not match "
            "DSE official date."
        )

        print(            "No market data will be collected."
        )

        print(
            "No CSV will be written."
        )

        return False

    trading_date = official_date

    # ------------------------------------------------------------
    # SME source
    # ------------------------------------------------------------

    sme_df = scrape_sme_data(
        trading_date
    )

    if sme_df is None:
        raise RuntimeError(
            "SME collection failed"
        )

    # ------------------------------------------------------------
    # DSE stocks from Day End Archive
    # ------------------------------------------------------------

    dse_df = scrape_dse_day_end_archive(
        trading_date,
        sme_tickers=sme_df["Ticker"].tolist(),
    )
    # ------------------------------------------------------------
    # ATB source
    # ------------------------------------------------------------

    atb_df = scrape_atb_data(
        trading_date
    )

    if atb_df is None:
        raise RuntimeError(
            "ATB collection failed"
        )


    if dse_df is None:
        raise RuntimeError(
            "DSE Day End Archive "
            "collection failed"
        )

    # ------------------------------------------------------------
    # Combine DSE + SME + ATB
    # ------------------------------------------------------------

    stocks = combine_dse_sme_atb(
        dse_df,
        sme_df,
        atb_df,
    )

    if stocks is None:
        raise RuntimeError(
            "DSE + SME + ATB combination failed"
        )

    # ------------------------------------------------------------
    # LankaBangla DSEX / DSES / DS30
    # ------------------------------------------------------------

    index_df = get_lankabangla_indices(
        trading_date
    )

    if index_df is None:
        raise RuntimeError(
            "LankaBangla index collection failed"
        )

    # ------------------------------------------------------------
    # Final validation
    # ------------------------------------------------------------

    print()
    print("=" * 70)
    print("FINAL DATASET VALIDATION")
    print("=" * 70)

    final_df = build_final_dataset(
        stocks,
        index_df,
        trading_date,
    )

    print(
        "FINAL VALIDATION: PASSED"
    )

    print(
        "Stock Rows:",
        len(stocks),
    )

    print(
        "Index Rows:",
        len(index_df),
    )

    print(
        "Final Rows:",
        len(final_df),
    )

    # ------------------------------------------------------------
    # Publish only after all validation passes.
    # ------------------------------------------------------------

    daily_file = publish_daily_csv(
        final_df,
        trading_date,
    )

    print()
    print("=" * 70)
    print(
        "DSE + SME + ATB + DSEX/DSES/DS30 COMPLETE"
    )
    print("=" * 70)

    print(
        "Official Trading Date:",
        trading_date,
    )

    print(
        "Daily CSV:",
        daily_file,
    )

    print("DONE")

    return True


# ================================================================
# WAIT / RETRY CONTROL
# ================================================================

def seconds_until(time_value):
    current = now_bd()

    target = current.replace(
        hour=time_value.hour,
        minute=time_value.minute,
        second=0,
        microsecond=0,
    )

    if target <= current:
        return 0

    return (
        target - current
    ).total_seconds()


def run_normal():
    # ------------------------------------------------------------
    # Wait until the collection window BEFORE checking the official
    # DSE date. The official date may not be updated early in the
    # morning for special Friday/Saturday trading sessions.
    # ------------------------------------------------------------

    wait_seconds = seconds_until(
        FEED_TIME
    )

    if wait_seconds > 0:

        print()
        print("=" * 70)
        print(
            "WAITING FOR 2:40 PM BEFORE DATE CHECK"
        )
        print("=" * 70)

        print(
            "Bangladesh Time:",
            now_bd().strftime(
                "%I:%M:%S %p"
            ),
        )

        print(
            "Scheduled feed : 02:40 PM"
        )

        time.sleep(
            wait_seconds
        )

    # Check the current date against the official DSE date only
    # after the scheduled collection window has started.

    current_date = today_bd()

    official_date = (
        get_dse_official_date()
    )

    if official_date is None:

        print(
            "Unable to determine "
            "DSE date at collection time. STOP."
        )

        return 1

    print()
    print(
        "Current Bangladesh Date :",
        current_date,
    )

    print(
        "DSE Official Trading Date:",
        official_date,
    )

    # This is the ONLY initial trading-day gate.

    if current_date != official_date:

        print()
        print("=" * 70)
        print("DATE GATE: STOP")
        print("=" * 70)

        print(
            "DSE official date does not "
            "match today's date."
        )

        print(
            "No collection."
        )

        return 0

    # ------------------------------------------------------------
    # Attempt 1: 2:40 PM
    # Attempt 2: 2:45 PM if needed
    # Attempt 3: 2:50 PM if needed
    # ------------------------------------------------------------

    for attempt in range(
        1,
        MAX_ATTEMPTS + 1,
    ):

        print()
        print(
            f"========== EOD ATTEMPT "
            f"{attempt}/{MAX_ATTEMPTS} =========="
        )

        try:

            # Re-check date immediately
            # before collection.

            current_date = today_bd()

            official_date = (
                get_dse_official_date()
            )

            if official_date is None:
                raise RuntimeError(
                    "Could not determine "
                    "DSE official date"
                )

            if (
                current_date
                != official_date
            ):

                print(
                    "DATE GATE CHANGED: STOP"
                )

                return 0

            if collect_and_publish():
                return 0

        except Exception as exc:

            logging.exception(
                "EOD attempt %d failed",
                attempt,
            )

            print()
            print(
                "ATTEMPT FAILED:",
                exc,
            )

        if attempt == 1:

            wait_seconds = seconds_until(
                RETRY_TIME
            )

            print()
            print("=" * 70)
            print("FIRST ATTEMPT FAILED")
            print("=" * 70)

            if wait_seconds > 0:

                print(
                    "Retrying at 02:45 PM..."
                )

                time.sleep(
                    wait_seconds
                )

            else:

                print(
                    "2:45 PM has passed; "
                    "performing the retry now."
                )

        elif attempt == 2:

            wait_seconds = seconds_until(
                FINAL_RETRY_TIME
            )

            print()
            print("=" * 70)
            print("SECOND ATTEMPT FAILED")
            print("=" * 70)

            if wait_seconds > 0:

                print(
                    "Final retrying at 02:50 PM..."
                )

                time.sleep(
                    wait_seconds
                )

            else:

                print(
                    "2:50 PM has passed; "
                    "performing the final retry now."
                )

    print()
    print("=" * 70)
    print("CLOUD FEEDER FAILED")
    print("=" * 70)

    print(
        "No new daily CSV was published."
    )

    return 1


# ================================================================
# MAIN
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "DSE + SME + ATB + DSEX/DSES/DS30 "
            "daily EOD cloud feeder"
        )
    )

    parser.add_argument(
        "--check-date",
        action="store_true",
        help=(
            "Check current Bangladesh date "
            "against DSE official date only."
        ),
    )

    args = parser.parse_args()

    if args.check_date:
        return date_gate_only()

    return run_normal()


if __name__ == "__main__":
    sys.exit(main())
