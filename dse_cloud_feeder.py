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

DSE_INDEX_URL = "https://www.dsebd.org/index.php"
DSE_ARCHIVE_URL = "https://www.dsebd.org/day_end_archive.php"
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
            DSE_INDEX_URL,
            headers=DSE_HEADERS,
            timeout=30,
            verify=False,
        )

        response.raise_for_status()

        print(
            "DSE Date URL:",
            DSE_INDEX_URL,
        )

        print(
            "DSE Date Response:",
            len(response.content),
            "bytes",
        )

        match = re.search(
            r"Last\s+update\s+on\s+"
            r"([A-Za-z]+)\s+"
            r"(\d{1,2}),\s+"
            r"(\d{4})",
            response.text,
            re.IGNORECASE,
        )

        if not match:
            raise RuntimeError(
                "DSE official trading date not found"
            )

        month_name, day, year = match.groups()

        parsed = None

        for fmt in (
            "%B %d %Y",
            "%b %d %Y",
        ):
            try:
                parsed = datetime.strptime(
                    f"{month_name} {day} {year}",
                    fmt,
                )

                break

            except ValueError:
                pass

        if parsed is None:
            raise RuntimeError(
                "Could not parse DSE official date"
            )

        result = parsed.strftime(DATE_FMT)

        print(
            "Official DSE Trading Date:",
            result,
        )

        return result

    except Exception as exc:
        logging.exception(
            "Official DSE date failed"
        )

        print(
            "DSE DATE ERROR:",
            exc,
        )

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
    print("1. DSE STOCK DATA - DAY END ARCHIVE")
    print("=" * 70)

    params = {
        "startDate": trading_date,
        "endDate": trading_date,
        "inst": "All Instrument",
        "archive": "data",
    }

    try:
        response = requests.get(
            DSE_ARCHIVE_URL,
            params=params,
            headers=DSE_HEADERS,
            timeout=45,
            verify=False,
        )

        response.raise_for_status()

        print(
            "DSE Archive URL:",
            response.url,
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

    except Exception as exc:
        logging.exception(
            "DSE Day End Archive request failed"
        )

        print(
            "DSE ARCHIVE ERROR:",
            repr(exc),
        )

        return None

    # ------------------------------------------------------------
    # Find the actual EOD stock table.
    #
    # Do NOT depend on a fixed table number.
    # ------------------------------------------------------------

    target = None
    target_index = None

    tables = soup.find_all("table")

    print(
        "HTML Tables Found:",
        len(tables),
    )

    for i, table in enumerate(tables):

        rows = table.find_all("tr")

        if not rows:
            continue

        header_cells = rows[0].find_all(
            ["th", "td"]
        )

        headers = [
            norm(
                cell.get_text(
                    " ",
                    strip=True,
                )
            )
            for cell in header_cells
        ]

        required_headers = {
            "TRADING CODE",
            "HIGH",
            "LOW",
            "OPENP*",
            "CLOSEP*",
            "VOLUME",
        }

        if required_headers.issubset(
            set(headers)
        ):
            target = table
            target_index = i
            break

    if target is None:
        print(
            "ERROR: DSE Day End Archive "
            "stock table not found"
        )

        logging.error(
            "DSE archive stock table not found; "
            "html_tables=%d",
            len(tables),
        )

        return None

    print(
        "DSE Stock Table Found: TABLE",
        target_index,
    )

    rows = target.find_all("tr")

    if len(rows) < 2:
        print(
            "ERROR: DSE stock table "
            "contains no data rows"
        )

        return None

    header_cells = rows[0].find_all(
        ["th", "td"]
    )

    headers = [
        norm(
            cell.get_text(
                " ",
                strip=True,
            )
        )
        for cell in header_cells
    ]

    header_positions = {
        header: i
        for i, header in enumerate(headers)
    }

    def header_position(*names):
        for name in names:
            key = norm(name)

            if key in header_positions:
                return header_positions[key]

        return None

    ticker_pos = header_position(
        "TRADING CODE",
        "TRADINGCODE",
    )

    date_pos = header_position(
        "DATE",
    )

    open_pos = header_position(
        "OPENP*",
        "OPENP",
        "OPEN",
    )

    high_pos = header_position(
        "HIGH",
    )

    low_pos = header_position(
        "LOW",
    )

    close_pos = header_position(
        "CLOSEP*",
        "CLOSEP",
        "CLOSE",
    )

    volume_pos = header_position(
        "VOLUME",
    )

    print()
    print("DSE Archive Detected Columns:")

    print(
        "Ticker :",
        "TRADING CODE"
        if ticker_pos is not None
        else None,
    )

    print(
        "Date   :",
        "DATE"
        if date_pos is not None
        else None,
    )

    print(
        "Open   :",
        "OPENP*"
        if open_pos is not None
        else None,
    )

    print(
        "High   :",
        "HIGH"
        if high_pos is not None
        else None,
    )

    print(
        "Low    :",
        "LOW"
        if low_pos is not None
        else None,
    )

    print(
        "Close  :",
        "CLOSEP*"
        if close_pos is not None
        else None,
    )

    print(
        "Volume :",
        "VOLUME"
        if volume_pos is not None
        else None,
    )

    required_positions = [
        ticker_pos,
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
        print(
            "ERROR: Required DSE archive "
            "columns missing"
        )

        return None

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

        ticker = (
            values[ticker_pos]
            .strip()
            .upper()
        )

        if not valid_ticker(ticker):

            logging.warning(
                "Skipped invalid DSE ticker: %r",
                ticker,
            )

            print(
                "WARNING: Skipped invalid "
                "DSE ticker:",
                repr(ticker),
            )

            continue

        # If the page supplies a date, verify it.
        if date_pos is not None:

            row_date = (
                values[date_pos]
                .strip()
            )

            if (
                row_date
                and row_date != trading_date
            ):
                continue

        records.append(
            {
                "Ticker": ticker,
                "Date": trading_date,
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
        print(
            "ERROR: No valid DSE "
            "stock rows extracted"
        )

        logging.error(
            "DSE archive contained "
            "no valid stock rows"
        )

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
        result = result[
            ~result["Ticker"].isin(
                sme_set
            )
        ].copy()

    removed_sme = (
        before_sme - len(result)
    )

    # No fixed ticker count.
    # No Close=0 filter.
    # No Volume=0 filter.
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
        print(
            "ERROR: DSE archive returned "
            "zero DSE stocks"
        )

        return None

    print(
        "DSE Stock Table Rows:",
        len(rows) - 1,
    )

    print(
        "DSE Archive Symbols:",
        len(result),
    )

    print(
        "SME Symbols Removed:",
        removed_sme,
    )

    print(
        "Zero Volume Retained:",
        int(
            (result["Volume"] == 0).sum()
        ),
    )

    print(
        "Zero Close Retained:",
        int(
            (result["Close"] == 0).sum()
        ),
    )

    print()
    print("DSE ARCHIVE FIRST 5:")

    print(
        result.head(5).to_string(
            index=False
        )
    )

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
        "SME Symbols       :",
        len(sme),
    )

    print(
        "Duplicates Removed:",
        removed_overlap,
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

    return combined[EXPECTED_COLUMNS]


# ================================================================
# 4. LANKABANGLA DSEX / DSES / DS30 INDEX DATA
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
            "No DSE/SME stock data"
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
        "LBS",
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
        "LBS",
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
    # combine_dse_sme() already creates:
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
    # combine_dse_sme() already sorts each group independently.
    # ------------------------------------------------------------

    print()
    print("FINAL CSV ORDER VALIDATION: PASSED")
    print("1. DSEX")
    print("2. DS30")
    print("3. DSES")
    print("4. DSE tickers - alphabetical")
    print("5. SME tickers - alphabetical")

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

        print(
            "No market data will be collected."
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

    if dse_df is None:
        raise RuntimeError(
            "DSE Day End Archive "
            "collection failed"
        )

    # ------------------------------------------------------------
    # Combine DSE + SME
    # ------------------------------------------------------------

    stocks = combine_dse_sme(
        dse_df,
        sme_df,
    )

    if stocks is None:
        raise RuntimeError(
            "DSE + SME combination failed"
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
        "DSE + SME + DSEX/DSES/DS30 COMPLETE"
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
    # Date is checked immediately.

    current_date = today_bd()

    official_date = (
        get_dse_official_date()
    )

    if official_date is None:

        print(
            "Unable to determine "
            "DSE date. STOP."
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

    # This is the ONLY trading-day gate.

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

    # If launched before 2:40 PM,
    # wait until 2:40 PM.

    wait_seconds = seconds_until(
        FEED_TIME
    )

    if wait_seconds > 0:

        print()
        print("=" * 70)
        print(
            "DATE MATCHED - "
            "WAITING FOR 2:40 PM"
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

    # ------------------------------------------------------------
    # Attempt 1: 2:40 PM
    # Attempt 2: 2:45 PM if needed
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
                    "performing the single "
                    "retry now."
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
            "DSE + SME + DSEX/DSES/DS30 "
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
