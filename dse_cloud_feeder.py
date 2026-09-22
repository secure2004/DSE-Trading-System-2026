from pathlib import Path

content = r'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DSE Cloud Feeder
================

Daily Bangladesh DSE/SME EOD data collector.

Workflow:
1. Start with current Bangladesh local date.
2. Get the official DSE trading date from DSE index page.
3. If local date != official DSE date, stop.
4. Wait until 2:40 PM Bangladesh time if started earlier.
5. Collect:
   - DSE Day End Archive
   - DSE SME latest price source
   - LankaBangla PriceFile for DSEX / DS30 / DSES
6. Combine and validate.
7. Publish:
   data/YYYY/MM/DSE_EOD_YYYY-MM-DD.csv

Retry:
- Attempt 1 at 2:40 PM
- Attempt 2 at 2:45 PM if attempt 1 fails
- Stop after the second failure.

Important:
- No TradingView.
- No tradingview_ta.
- No lxml dependency.
- LankaBangla HTML is parsed with BeautifulSoup.
- No hard-coded weekday/weekend or holiday rules.
- No Close=0 or Volume=0 stock filtering.
- DSES/DS30 market volume from LankaBangla is forced to 0 because
  the same market-wide volume is returned for all three indices.
- Final CSV order:
    DSEX
    DS30
    DSES
    DSE tickers
    SME tickers
"""

import argparse
import logging
import re
import sys
import time
import urllib3

from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup


# ============================================================
# PATHS
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent

DATA_DIR = PROJECT_DIR / "data"

LOG_FILE = PROJECT_DIR / "dse_data_feeder.log"


# ============================================================
# URLs
# ============================================================

DSE_INDEX_URL = (
    "https://www.dsebd.org/index.php"
)

DSE_ARCHIVE_URL = (
    "https://www.dsebd.org/day_end_archive.php"
)

SME_HTML_URL = (
    "https://sme.dsebd.org/"
    "sme_latest_share_price_scroll_by_ltp.php"
)

LANKABANGLA_PRICEFILE_URL = (
    "https://www.lankabd.com/Home/PriceFile"
)


# ============================================================
# CONSTANTS
# ============================================================

BD_TZ = ZoneInfo(
    "Asia/Dhaka"
)

DATE_FMT = "%Y-%m-%d"

FEED_TIME = dt_time(
    14,
    40,
)

RETRY_TIME = dt_time(
    14,
    45,
)

MAX_ATTEMPTS = 2


EXPECTED_COLUMNS = [
    "Ticker",
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
]


# ============================================================
# HTTP HEADERS
# ============================================================

DSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/153.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/avif,image/webp,"
        "image/apng,*/*;q=0.8"
    ),
    "Accept-Language": (
        "en-US,en;q=0.9"
    ),
    "Connection": "keep-alive",
}


SME_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/153.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/avif,image/webp,"
        "image/apng,*/*;q=0.8"
    ),
    "Accept-Language": (
        "en-US,en;q=0.9"
    ),
    "Connection": "keep-alive",
}


LANKABANGLA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/153.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/avif,image/webp,"
        "image/apng,*/*;q=0.8"
    ),
    "Accept-Language": (
        "en-US,en;q=0.9"
    ),
    "Connection": "keep-alive",
}


# ============================================================
# SSL WARNING SUPPRESSION
# ============================================================

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)


# ============================================================
# LOGGING
# ============================================================

def setup_logging():
    """
    Configure console + file logging.
    """

    LOG_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
        handlers=[
            logging.FileHandler(
                LOG_FILE,
                encoding="utf-8",
            ),
            logging.StreamHandler(
                sys.stdout,
            ),
        ],
    )


logger = logging.getLogger(
    __name__
)


# ============================================================
# BASIC HELPERS
# ============================================================

def clean_text(value):
    """
    Convert any value to a normalized string.
    """

    if value is None:
        return ""

    return (
        str(value)
        .replace("\xa0", " ")
        .strip()
    )


def norm(value):
    """
    Normalize header text for comparison.
    """

    text = clean_text(
        value
    ).upper()

    text = re.sub(
        r"[^A-Z0-9]+",
        "",
        text,
    )

    return text


def safe_float(value):
    """
    Safely convert text to float.

    Handles:
    - commas
    - whitespace
    - parentheses
    - empty strings
    """

    text = clean_text(
        value
    )

    if not text:
        return None

    text = text.replace(
        ",",
        "",
    )

    text = text.replace(
        "\u2212",
        "-",
    )

    if (
        text.startswith("(")
        and text.endswith(")")
    ):
        text = "-" + text[1:-1]

    try:
        return float(text)
    except (
        TypeError,
        ValueError,
    ):
        return None


def safe_int(value):
    """
    Safely convert value to integer.
    """

    number = safe_float(
        value
    )

    if number is None:
        return None

    return int(
        round(number)
    )


def now_bd():
    """
    Current Bangladesh local datetime.
    """

    return datetime.now(
        BD_TZ
    )


def today_bd():
    """
    Current Bangladesh local date.
    """

    return now_bd().date()


def find_column(
    headers,
    candidates,
):
    """
    Find a column index from normalized header candidates.

    Exact normalized match is preferred.
    """

    normalized_headers = [
        norm(header)
        for header in headers
    ]

    normalized_candidates = [
        norm(candidate)
        for candidate in candidates
    ]

    for candidate in normalized_candidates:
        for index, header in enumerate(
            normalized_headers
        ):
            if header == candidate:
                return index

    return None


def valid_ticker(value):
    """
    Basic ticker validation.
    """

    ticker = clean_text(
        value
    ).upper()

    if not ticker:
        return False

    if ticker in {
        "TRADING CODE",
        "TRADINGCODE",
        "SYMBOL",
        "TOTAL",
        "TOTALS",
        "N/A",
        "NA",
    }:
        return False

    return bool(
        re.fullmatch(
            r"[A-Z0-9&._-]+",
            ticker,
        )
    )


def parse_date_value(value):
    """
    Convert common date representations to YYYY-MM-DD.
    """

    text = clean_text(
        value
    )

    if not text:
        return None

    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%b-%Y",
        "%d/%b/%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(
                text,
                fmt,
            ).date()
        except ValueError:
            pass

    parsed = pd.to_datetime(
        text,
        errors="coerce",
        dayfirst=False,
    )

    if pd.isna(parsed):
        return None

    return parsed.date()


def table_rows(table):
    """
    Return non-empty table rows as lists of text.
    """

    rows = []

    for tr in table.find_all("tr"):
        cells = tr.find_all(
            ["th", "td"]
        )

        values = [
            clean_text(
                cell.get_text(
                    " ",
                    strip=True,
                )
            )
            for cell in cells
        ]

        if any(values):
            rows.append(values)

    return rows


def locate_header_row(
    rows,
    required_headers,
    max_scan=15,
):
    """
    Locate the first row containing all required headers.
    """

    required = {
        norm(item)
        for item in required_headers
    }

    for row_index, row in enumerate(
        rows[:max_scan]
    ):
        row_norm = {
            norm(item)
            for item in row
        }

        if required.issubset(
            row_norm
        ):
            return row_index

    return None


# ============================================================
# DSE OFFICIAL DATE
# ============================================================

def get_dse_official_date():
    """
    Read official DSE last-update date.

    Expected page text contains:
        Last update on September 20, 2026
    """

    print()
    print(
        "=" * 70
    )
    print(
        "CHECKING OFFICIAL DSE DATE"
    )
    print(
        "=" * 70
    )

    response = requests.get(
        DSE_INDEX_URL,
        headers=DSE_HEADERS,
        timeout=30,
        verify=False,
    )

    response.raise_for_status()

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
            "Could not find official DSE date "
            "on DSE index page."
        )

    month_name = match.group(
        1
    )
    day = int(
        match.group(2)
    )
    year = int(
        match.group(3)
    )

    official_date = datetime.strptime(
        f"{month_name} {day} {year}",
        "%B %d %Y",
    ).date()

    print(
        "Official DSE Date:",
        official_date.strftime(
            DATE_FMT
        ),
    )

    return official_date


def date_gate_only():
    """
    Check local Bangladesh date against official DSE date.

    Returns:
        trading_date if equal
        None if not equal
    """

    local_date = today_bd()

    print()
    print(
        "Bangladesh Local Date:",
        local_date.strftime(
            DATE_FMT
        ),
    )

    try:
        official_date = (
            get_dse_official_date()
        )
    except Exception as exc:
        print(
            "DSE date check failed:",
            exc,
        )
        return None

    if local_date != official_date:
        print()
        print(
            "DATE GATE: STOP"
        )
        print(
            "Local date and official DSE date "
            "do not match."
        )
        print(
            "No collection will be performed."
        )
        return None

    print()
    print(
        "DATE GATE: PASS"
    )

    return official_date


# ============================================================
# DSE DAY END ARCHIVE
# ============================================================

def scrape_dse_day_end_archive(
    trading_date,
):
    """
    Collect DSE stock OHLC + Volume from Day End Archive.

    The table is discovered dynamically from headers.
    No fixed table number is used.
    """

    print()
    print(
        "=" * 70
    )
    print(
        "1. DSE DAY END ARCHIVE"
    )
    print(
        "=" * 70
    )

    date_string = trading_date.strftime(
        DATE_FMT
    )

    params = {
        "startDate": date_string,
        "endDate": date_string,
        "inst": "All",
        "archive": "1",
    }

    response = requests.get(
        DSE_ARCHIVE_URL,
        params=params,
        headers=DSE_HEADERS,
        timeout=60,
        verify=False,
    )

    response.raise_for_status()

    soup = BeautifulSoup(
        response.text,
        "html.parser",
    )

    target_table = None
    target_rows = None
    header_row_index = None

    required = [
        "TRADING CODE",
        "HIGH",
        "LOW",
        "VOLUME",
    ]

    for table in soup.find_all("table"):
        rows = table_rows(
            table
        )

        if not rows:
            continue

        header_index = locate_header_row(
            rows,
            required,
        )

        if header_index is None:
            continue

        headers = rows[
            header_index
        ]

        if (
            find_column(
                headers,
                ["OPENP*", "OPEN"],
            )
            is None
        ):
            continue

        if (
            find_column(
                headers,
                ["CLOSEP*", "CLOSE"],
            )
            is None
        ):
            continue

        target_table = table
        target_rows = rows
        header_row_index = header_index
        break

    if target_table is None:
        raise RuntimeError(
            "Could not locate DSE Day End "
            "Archive stock table."
        )

    headers = target_rows[
        header_row_index
    ]

    ticker_index = find_column(
        headers,
        [
            "TRADING CODE",
            "TRADINGCODE",
        ],
    )

    date_index = find_column(
        headers,
        [
            "DATE",
            "DATE*",
        ],
    )

    open_index = find_column(
        headers,
        [
            "OPENP*",
            "OPEN",
            "OPENP",
        ],
    )

    high_index = find_column(
        headers,
        [
            "HIGH",
            "HIGH*",
        ],
    )

    low_index = find_column(
        headers,
        [
            "LOW",
            "LOW*",
        ],
    )

    close_index = find_column(
        headers,
        [
            "CLOSEP*",
            "CLOSE",
            "CLOSEP",
        ],
    )

    volume_index = find_column(
        headers,
        [
            "VOLUME",
            "VOLUME*",
        ],
    )

    required_indices = {
        "ticker": ticker_index,
        "open": open_index,
        "high": high_index,
        "low": low_index,
        "close": close_index,
        "volume": volume_index,
    }

    missing = [
        name
        for name, index in required_indices.items()
        if index is None
    ]

    if missing:
        raise RuntimeError(
            "DSE archive missing columns: "
            + ", ".join(missing)
        )

    records = []

    for row in target_rows[
        header_row_index + 1:
    ]:
        max_index = max(
            index
            for index in required_indices.values()
            if index is not None
        )

        if len(row) <= max_index:
            continue

        ticker = clean_text(
            row[ticker_index]
        ).upper()

        if not valid_ticker(
            ticker
        ):
            continue

        if date_index is not None:
            row_date = parse_date_value(
                row[date_index]
            )

            if (
                row_date is not None
                and row_date != trading_date
            ):
                continue

        open_value = safe_float(
            row[open_index]
        )

        high_value = safe_float(
            row[high_index]
        )

        low_value = safe_float(
            row[low_index]
        )

        close_value = safe_float(
            row[close_index]
        )

        volume_value = safe_int(
            row[volume_index]
        )

        if (
            open_value is None
            or high_value is None
            or low_value is None
            or close_value is None
            or volume_value is None
        ):
            continue

        records.append(
            [
                ticker,
                date_string,
                open_value,
                high_value,
                low_value,
                close_value,
                volume_value,
            ]
        )

    df = pd.DataFrame(
        records,
        columns=EXPECTED_COLUMNS,
    )

    if df.empty:
        raise RuntimeError(
            "DSE Day End Archive returned "
            "no valid stock records."
        )

    df.drop_duplicates(
        subset=["Ticker"],
        keep="last",
        inplace=True,
    )

    df.sort_values(
        "Ticker",
        inplace=True,
    )

    df.reset_index(
        drop=True,
        inplace=True,
    )

    print(
        "DSE Archive Symbols:",
        len(df),
    )

    return df


# ============================================================
# SME
# ============================================================

def scrape_sme_data(
    trading_date,
):
    """
    Collect SME stock data.

    Table and columns are identified dynamically.
    """

    print()
    print(
        "=" * 70
    )
    print(
        "2. SME MARKET"
    )
    print(
        "=" * 70
    )

    response = requests.get(
        SME_HTML_URL,
        headers=SME_HEADERS,
        timeout=60,
        verify=False,
    )

    response.raise_for_status()

    soup = BeautifulSoup(
        response.text,
        "html.parser",
    )

    target_rows = None
    header_row_index = None

    required = [
        "TRADING CODE",
        "HIGH",
        "LOW",
        "VOLUME",
    ]

    for table in soup.find_all("table"):
        rows = table_rows(
            table
        )

        if not rows:
            continue

        header_index = locate_header_row(
            rows,
            required,
        )

        if header_index is None:
            continue

        headers = rows[
            header_index
        ]

        if (
            find_column(
                headers,
                [
                    "LTP*",
                    "LTP",
                    "CLOSEP*",
                    "CLOSE",
                ],
            )
            is None
        ):
            continue

        target_rows = rows
        header_row_index = header_index
        break

    if target_rows is None:
        raise RuntimeError(
            "Could not locate SME stock table."
        )

    headers = target_rows[
        header_row_index
    ]

    ticker_index = find_column(
        headers,
        [
            "TRADING CODE",
            "TRADINGCODE",
        ],
    )

    date_index = find_column(
        headers,
        [
            "DATE",
            "DATE*",
        ],
    )

    open_index = find_column(
        headers,
        [
            "YCP*",
            "YCP",
            "OPENP*",
            "OPEN",
        ],
    )

    high_index = find_column(
        headers,
        [
            "HIGH",
            "HIGH*",
        ],
    )

    low_index = find_column(
        headers,
        [
            "LOW",
            "LOW*",
        ],
    )

    close_index = find_column(
        headers,
        [
            "CLOSEP*",
            "CLOSE",
            "LTP*",
            "LTP",
        ],
    )

    volume_index = find_column(
        headers,
        [
            "VOLUME",
            "VOLUME*",
        ],
    )

    required_indices = {
        "ticker": ticker_index,
        "high": high_index,
        "low": low_index,
        "close": close_index,
        "volume": volume_index,
    }

    missing = [
        name
        for name, index in required_indices.items()
        if index is None
    ]

    if missing:
        raise RuntimeError(
            "SME table missing columns: "
            + ", ".join(missing)
        )

    records = []

    for row in target_rows[
        header_row_index + 1:
    ]:
        max_index = max(
            index
            for index in required_indices.values()
            if index is not None
        )

        if len(row) <= max_index:
            continue

        ticker = clean_text(
            row[ticker_index]
        ).upper()

        if not valid_ticker(
            ticker
        ):
            continue

        if date_index is not None:
            row_date = parse_date_value(
                row[date_index]
            )

            if (
                row_date is not None
                and row_date != trading_date
            ):
                continue

        high_value = safe_float(
            row[high_index]
        )

        low_value = safe_float(
            row[low_index]
        )

        close_value = safe_float(
            row[close_index]
        )

        volume_value = safe_int(
            row[volume_index]
        )

        if (
            high_value is None
            or low_value is None
            or close_value is None
            or volume_value is None
        ):
            continue

        open_value = None

        if open_index is not None:
            open_value = safe_float(
                row[open_index]
            )

        if open_value is None:
            open_value = close_value

        records.append(
            [
                ticker,
                trading_date.strftime(
                    DATE_FMT
                ),
                open_value,
                high_value,
                low_value,
                close_value,
                volume_value,
            ]
        )

    df = pd.DataFrame(
        records,
        columns=EXPECTED_COLUMNS,
    )

    if df.empty:
        raise RuntimeError(
            "SME source returned no valid "
            "stock records."
        )

    df.drop_duplicates(
        subset=["Ticker"],
        keep="last",
        inplace=True,
    )

    df.sort_values(
        "Ticker",
        inplace=True,
    )

    df.reset_index(
        drop=True,
        inplace=True,
    )

    print(
        "SME Symbols:",
        len(df),
    )

    return df


# ============================================================
# COMBINE DSE + SME
# ============================================================

def combine_dse_sme(
    dse_df,
    sme_df,
):
    """
    Keep DSE and SME groups separate.

    Final stock ordering:
        DSE tickers
        SME tickers

    SME remains authoritative if a ticker appears
    in both datasets.
    """

    print()
    print(
        "=" * 70
    )
    print(
        "3. COMBINING DSE + SME"
    )
    print(
        "=" * 70
    )

    if (
        dse_df is None
        or sme_df is None
    ):
        return None

    # Keep DSE and SME groups separate.
    dse = dse_df.copy()
    sme = sme_df.copy()

    # Remove any accidental overlap.
    # SME remains authoritative if a ticker appears
    # in both datasets.
    dse_before = len(
        dse
    )

    sme_tickers = set(
        sme["Ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    dse = dse[
        ~dse["Ticker"].isin(
            sme_tickers
        )
    ].copy()

    removed_overlap = (
        dse_before
        - len(dse)
    )

    # Sort each group alphabetically.
    dse.sort_values(
        "Ticker",
        inplace=True,
    )

    sme.sort_values(
        "Ticker",
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

    # DSE first, SME second.
    combined = pd.concat(
        [
            dse,
            sme,
        ],
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

    return combined[
        EXPECTED_COLUMNS
    ]


# ============================================================
# LANKABANGLA PRICEFILE
# ============================================================

def get_lankabangla_indices(
    trading_date,
):
    """
    Get DSEX / DS30 / DSES OHLC + Volume
    from LankaBangla PriceFile.

    BeautifulSoup is used instead of pandas.read_html()
    so lxml is not required.

    LankaBangla returns the same market-wide volume
    for DSEX, DSES and DS30. Therefore:
        DSEX = actual market volume
        DSES = 0
        DS30 = 0
    """

    print()
    print(
        "=" * 70
    )
    print(
        "4. LANKABANGLA INDEX DATA"
    )
    print(
        "=" * 70
    )

    date_string = trading_date.strftime(
        DATE_FMT
    )

    url = (
        f"{LANKABANGLA_PRICEFILE_URL}"
        f"?dateinp={date_string}"
    )

    print(
        "URL:",
        url,
    )

    response = requests.get(
        url,
        headers=LANKABANGLA_HEADERS,
        timeout=60,
        verify=False,
    )

    print(
        "HTTP Status:",
        response.status_code,
    )

    response.raise_for_status()

    soup = BeautifulSoup(
        response.text,
        "html.parser",
    )

    target_rows = None
    header_row_index = None

    required = [
        "SYMBOL",
        "DATE",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
    ]

    for table in soup.find_all("table"):
        rows = table_rows(
            table
        )

        if not rows:
            continue

        header_index = locate_header_row(
            rows,
            required,
        )

        if header_index is None:
            continue

        target_rows = rows
        header_row_index = header_index
        break

    if target_rows is None:
        raise RuntimeError(
            "Could not locate LankaBangla "
            "PriceFile table."
        )

    headers = target_rows[
        header_row_index
    ]

    symbol_index = find_column(
        headers,
        [
            "SYMBOL",
            "TRADING CODE",
            "TRADINGCODE",
        ],
    )

    date_index = find_column(
        headers,
        [
            "DATE",
        ],
    )

    open_index = find_column(
        headers,
        [
            "OPEN",
        ],
    )

    high_index = find_column(
        headers,
        [
            "HIGH",
        ],
    )

    low_index = find_column(
        headers,
        [
            "LOW",
        ],
    )

    close_index = find_column(
        headers,
        [
            "CLOSE",
        ],
    )

    volume_index = find_column(
        headers,
        [
            "VOLUME",
        ],
    )

    required_indices = {
        "symbol": symbol_index,
        "date": date_index,
        "open": open_index,
        "high": high_index,
        "low": low_index,
        "close": close_index,
        "volume": volume_index,
    }

    missing = [
        name
        for name, index in required_indices.items()
        if index is None
    ]

    if missing:
        raise RuntimeError(
            "LankaBangla table missing columns: "
            + ", ".join(missing)
        )

    wanted_symbols = {
        "DSEX",
        "DSES",
        "DS30",
    }

    found = {}

    for row in target_rows[
        header_row_index + 1:
    ]:
        max_index = max(
            index
            for index in required_indices.values()
            if index is not None
        )

        if len(row) <= max_index:
            continue

        symbol = clean_text(
            row[symbol_index]
        ).upper()

        if symbol not in wanted_symbols:
            continue

        row_date = parse_date_value(
            row[date_index]
        )

        if (
            row_date is not None
            and row_date != trading_date
        ):
            continue

        open_value = safe_float(
            row[open_index]
        )

        high_value = safe_float(
            row[high_index]
        )

        low_value = safe_float(
            row[low_index]
        )

        close_value = safe_float(
            row[close_index]
        )

        volume_value = safe_int(
            row[volume_index]
        )

        if (
            open_value is None
            or high_value is None
            or low_value is None
            or close_value is None
            or volume_value is None
        ):
            continue

        if (
            open_value <= 0
            or high_value <= 0
            or low_value <= 0
            or close_value <= 0
        ):
            continue

        found[symbol] = {
            "Open": open_value,
            "High": high_value,
            "Low": low_value,
            "Close": close_value,
            "Volume": volume_value,
        }

    missing_symbols = [
        symbol
        for symbol in (
            "DSEX",
            "DSES",
            "DS30",
        )
        if symbol not in found
    ]

    if missing_symbols:
        raise RuntimeError(
            "LankaBangla missing index data: "
            + ", ".join(
                missing_symbols
            )
        )

    if found["DSEX"]["Volume"] <= 0:
        raise RuntimeError(
            "LankaBangla DSEX volume is not positive."
        )

    # DSEX keeps the actual market volume.
    # DSES and DS30 are set to zero because
    # LankaBangla returns the same market-wide volume.
    found["DSES"]["Volume"] = 0
    found["DS30"]["Volume"] = 0

    print()
    print(
        "LankaBangla Index Values:"
    )

    for symbol in (
        "DSEX",
        "DS30",
        "DSES",
    ):
        data = found[symbol]

        print(
            f"{symbol}: "
            f"O={data['Open']} "
            f"H={data['High']} "
            f"L={data['Low']} "
            f"C={data['Close']} "
            f"V={data['Volume']}"
        )

    return {
        "date": trading_date,
        "indices": found,
    }


# ============================================================
# FINAL DATASET
# ============================================================

def build_final_dataset(
    stocks,
    index_data,
    trading_date,
):
    """
    Build final CSV dataset.

    Required order:
        1. DSEX
        2. DS30
        3. DSES
        4. DSE tickers
        5. SME tickers
    """

    print()
    print(
        "=" * 70
    )
    print(
        "5. FINAL VALIDATION"
    )
    print(
        "=" * 70
    )

    if stocks is None:
        raise RuntimeError(
            "Stock dataset is None."
        )

    if stocks.empty:
        raise RuntimeError(
            "Stock dataset is empty."
        )

    if index_data is None:
        raise RuntimeError(
            "Index data is None."
        )

    if index_data.get(
        "date"
    ) != trading_date:
        raise RuntimeError(
            "LankaBangla date does not match "
            "DSE trading date."
        )

    stocks = stocks.copy()

    if list(
        stocks.columns
    ) != EXPECTED_COLUMNS:
        raise RuntimeError(
            "Stock columns are incorrect: "
            f"{list(stocks.columns)}"
        )

    # Ensure exact trading date.
    stocks["Date"] = (
        stocks["Date"]
        .astype(str)
    )

    expected_date_string = (
        trading_date.strftime(
            DATE_FMT
        )
    )

    if not (
        stocks["Date"]
        == expected_date_string
    ).all():
        raise RuntimeError(
            "One or more stock records have "
            "an incorrect date."
        )

    # No duplicate stock tickers.
    if stocks["Ticker"].duplicated().any():
        duplicates = (
            stocks.loc[
                stocks["Ticker"].duplicated(),
                "Ticker",
            ]
            .tolist()
        )

        raise RuntimeError(
            "Duplicate stock tickers found: "
            + ", ".join(
                duplicates[:20]
            )
        )

    # No negative volume.
    if (
        stocks["Volume"] < 0
    ).any():
        raise RuntimeError(
            "Negative stock volume found."
        )

    # Required indices.
    required_symbols = [
        "DSEX",
        "DS30",
        "DSES",
    ]

    for symbol in required_symbols:
        if symbol not in index_data[
            "indices"
        ]:
            raise RuntimeError(
                f"Missing index: {symbol}"
            )

    # Validate index OHLC.
    for symbol in required_symbols:
        data = index_data[
            "indices"
        ][symbol]

        for field in (
            "Open",
            "High",
            "Low",
            "Close",
        ):
            value = data.get(
                field
            )

            if (
                value is None
                or value <= 0
            ):
                raise RuntimeError(
                    f"{symbol} {field} is invalid."
                )

    # DSEX market volume must be positive.
    if (
        index_data["indices"]["DSEX"]["Volume"]
        <= 0
    ):
        raise RuntimeError(
            "DSEX market volume must be positive."
        )

    # --------------------------------------------------------
    # Required final CSV order:
    #   1. DSEX
    #   2. DS30
    #   3. DSES
    #   4. DSE tickers
    #   5. SME tickers
    # --------------------------------------------------------

    index_rows = []

    for symbol in (
        "DSEX",
        "DS30",
        "DSES",
    ):

        data = index_data[
            "indices"
        ][symbol]

        index_rows.append(
            [
                symbol,
                trading_date.strftime(
                    DATE_FMT
                ),
                data["Open"],
                data["High"],
                data["Low"],
                data["Close"],
                data["Volume"],
            ]
        )

    index_df = pd.DataFrame(
        index_rows,
        columns=EXPECTED_COLUMNS,
    )

    final_df = pd.concat(
        [
            index_df,
            stocks,
        ],
        ignore_index=True,
    )

    # Final duplicate check.
    if final_df["Ticker"].duplicated().any():
        duplicates = (
            final_df.loc[
                final_df["Ticker"].duplicated(),
                "Ticker",
            ]
            .tolist()
        )

        raise RuntimeError(
            "Duplicate ticker/index symbols in "
            "final dataset: "
            + ", ".join(
                duplicates[:20]
            )
        )

    if len(final_df) <= len(stocks):
        raise RuntimeError(
            "Final dataset does not contain "
            "the three index rows."
        )

    print(
        "Index Rows        :",
        len(index_df),
    )

    print(
        "Stock Rows        :",
        len(stocks),
    )

    print(
        "Final Rows        :",
        len(final_df),
    )

    print()
    print(
        "FIRST 10 FINAL ROWS:"
    )

    print(
        final_df.head(
            10
        ).to_string(
            index=False
        )
    )

    return final_df


# ============================================================
# PUBLISH CSV
# ============================================================

def publish_daily_csv(
    final_df,
    trading_date,
):
    """
    Publish:
        data/YYYY/MM/DSE_EOD_YYYY-MM-DD.csv

    Uses a temporary file and atomic replace.
    """

    print()
    print(
        "=" * 70
    )
    print(
        "6. PUBLISHING CSV"
    )
    print(
        "=" * 70
    )

    year = trading_date.strftime(
        "%Y"
    )

    month = trading_date.strftime(
        "%m"
    )

    filename = (
        f"DSE_EOD_"
        f"{trading_date.strftime(DATE_FMT)}"
        f".csv"
    )

    output_dir = (
        DATA_DIR
        / year
        / month
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / filename
    )

    temp_path = (
        output_dir
        / f".{filename}.tmp"
    )

    final_df.to_csv(
        temp_path,
        index=False,
        header=False,
        encoding="utf-8",
        lineterminator="\n",
    )

    temp_path.replace(
        output_path
    )

    print(
        "Published:",
        output_path,
    )

    print(
        "Rows:",
        len(final_df),
    )

    print()
    print(
        "CSV ORDER:"
    )

    print(
        "1. DSEX"
    )
    print(
        "2. DS30"
    )
    print(
        "3. DSES"
    )
    print(
        "4. DSE tickers"
    )
    print(
        "5. SME tickers"
    )

    return output_path


# ============================================================
# COMPLETE COLLECTION
# ============================================================

def collect_and_publish(
    trading_date,
):
    """
    Run the complete data collection and publication.
    """

    print()
    print(
        "#" * 70
    )
    print(
        "DSE DAILY DATA COLLECTION"
    )
    print(
        f"Trading Date: "
        f"{trading_date.strftime(DATE_FMT)}"
    )
    print(
        "#" * 70
    )

    # Re-check official date immediately before collection.
    official_date = (
        get_dse_official_date()
    )

    if official_date != trading_date:
        raise RuntimeError(
            "Official DSE date changed before "
            "collection."
        )

    dse_df = (
        scrape_dse_day_end_archive(
            trading_date
        )
    )

    sme_df = (
        scrape_sme_data(
            trading_date
        )
    )

    stocks = (
        combine_dse_sme(
            dse_df,
            sme_df,
        )
    )

    if stocks is None or stocks.empty:
        raise RuntimeError(
            "Combined stock dataset is empty."
        )

    index_data = (
        get_lankabangla_indices(
            trading_date
        )
    )

    final_df = (
        build_final_dataset(
            stocks,
            index_data,
            trading_date,
        )
    )

    output_path = (
        publish_daily_csv(
            final_df,
            trading_date,
        )
    )

    print()
    print(
        "=" * 70
    )
    print(
        "COLLECTION SUCCESS"
    )
    print(
        "=" * 70
    )

    print(
        "Trading Date:",
        trading_date.strftime(
            DATE_FMT
        ),
    )

    print(
        "Final Rows:",
        len(final_df),
    )

    print(
        "CSV:",
        output_path,
    )

    return output_path


# ============================================================
# TIME HELPERS
# ============================================================

def seconds_until(
    target_time,
):
    """
    Seconds from now until today's target time
    in Bangladesh timezone.

    Returns 0 if target time has already passed.
    """

    current = now_bd()

    target = datetime.combine(
        current.date(),
        target_time,
        tzinfo=BD_TZ,
    )

    delta = (
        target
        - current
    ).total_seconds()

    return max(
        0,
        delta,
    )


def wait_until(
    target_time,
):
    """
    Wait until Bangladesh local target time.
    """

    while True:
        current = now_bd()

        target = datetime.combine(
            current.date(),
            target_time,
            tzinfo=BD_TZ,
        )

        remaining = (
            target
            - current
        ).total_seconds()

        if remaining <= 0:
            return

        print(
            f"Waiting until "
            f"{target_time.strftime('%H:%M')} "
            f"BD time. "
            f"Remaining: "
            f"{int(remaining)} seconds"
        )

        time.sleep(
            min(
                remaining,
                30,
            )
        )


# ============================================================
# NORMAL RUN
# ============================================================

def run_normal():
    """
    Normal production workflow.

    1. Date gate.
    2. Wait until 14:40 if needed.
    3. Attempt collection.
    4. Retry once at 14:45.
    """

    print()
    print(
        "=" * 70
    )
    print(
        "DSE CLOUD FEEDER"
    )
    print(
        "=" * 70
    )

    current = now_bd()

    print(
        "Bangladesh Time:",
        current.strftime(
            "%Y-%m-%d %H:%M:%S %Z"
        ),
    )

    # --------------------------------------------------------
    # DATE GATE
    # --------------------------------------------------------

    trading_date = (
        date_gate_only()
    )

    if trading_date is None:
        return 0

    # --------------------------------------------------------
    # WAIT UNTIL 14:40
    # --------------------------------------------------------

    if now_bd().time() < FEED_TIME:
        print()
        print(
            "Before 2:40 PM."
        )

        wait_until(
            FEED_TIME
        )

    # --------------------------------------------------------
    # ATTEMPT 1
    # --------------------------------------------------------

    for attempt in range(
        1,
        MAX_ATTEMPTS + 1,
    ):

        print()
        print(
            "=" * 70
        )
        print(
            f"COLLECTION ATTEMPT "
            f"{attempt}/{MAX_ATTEMPTS}"
        )
        print(
            "=" * 70
        )

        try:
            # Re-check local date.
            current_date = today_bd()

            if current_date != trading_date:
                print(
                    "Date changed. Stopping."
                )
                return 0

            # Re-check official DSE date.
            official_date = (
                get_dse_official_date()
            )

            if official_date != trading_date:
                print(
                    "Official DSE date no longer "
                    "matches target date. Stopping."
                )
                return 0

            collect_and_publish(
                trading_date
            )

            return 0

        except Exception as exc:
            logger.exception(
                "Collection attempt %s failed.",
                attempt,
            )

            print()
            print(
                f"ATTEMPT {attempt} FAILED:"
            )

            print(
                str(exc)
            )

            if attempt >= MAX_ATTEMPTS:
                print()
                print(
                    "SECOND ATTEMPT FAILED."
                )

                print(
                    "Stopping without publishing "
                    "another CSV."
                )

                return 1

            # ------------------------------------------------
            # WAIT UNTIL 14:45 FOR RETRY
            # ------------------------------------------------

            print()
            print(
                "Retry scheduled for 2:45 PM."
            )

            if now_bd().time() < RETRY_TIME:
                wait_until(
                    RETRY_TIME
                )

    return 1


# ============================================================
# MAIN
# ============================================================

def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "DSE daily EOD cloud feeder"
        )
    )

    parser.add_argument(
        "--now",
        action="store_true",
        help=(
            "Run normal production workflow."
        ),
    )

    args = parser.parse_args()

    # Default behavior is normal run.
    return run_normal()


if __name__ == "__main__":
    sys.exit(
        main()
    )
'''

path = Path("/mnt/data/dse_cloud_feeder.py")
path.write_text(content, encoding="utf-8")

print(f"Created: {path}")
print(f"Size: {path.stat().st_size:,} bytes")
