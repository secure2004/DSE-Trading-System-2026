# =========================================================
# DSE + SME + DSEX Data Feeder for AmiBroker
# FINAL COMBINED PROFESSIONAL VERSION
#
# DATA:
#   DSE stocks       -> DSE website
#   SME stocks       -> SME DSE website
#   DSEX Open/High/Low -> TradingView
#   DSEX Close      -> DSE website
#   DSEX Volume     -> DSE website Total Volume
#   DSEX Date       -> DSE website "Last update on"
#
# SAFETY:
#   09:30:00 AM - 10:00:00 AM BST
#   DSE/SME download completely blocked.
#
#   Empty DSE/SME data cannot overwrite CSV.
# =========================================================


# =========================================================
# IMPORTS
# =========================================================

import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import shutil
import os
import re

import urllib3

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)

from tradingview_ta import TA_Handler, Interval



# =========================================================
# CLOUD ARCHIVE CONFIGURATION
# =========================================================

from pathlib import Path
import time
import sys

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
LATEST_DIR = DATA_DIR / "latest"
LOG_FILE = PROJECT_DIR / "dse_data_feeder.log"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LATEST_DIR.mkdir(parents=True, exist_ok=True)

DSE_HTML_URL = "https://www.dsebd.org/latest_share_price_scroll_l.php"
DSE_INDEX_URL = "https://www.dsebd.org/index.php"
SME_HTML_URL = "https://sme.dsebd.org/sme_latest_share_price_scroll_by_ltp.php"

DATE_FMT = "%Y-%m-%d"
MARKET_CLOSE_HOUR = 14
MARKET_CLOSE_MINUTE = 30

# DSE new-day reset protection: 09:30 <= time < 10:00 BST.
DSE_RESET_START = "09:30:00"
DSE_RESET_END = "10:00:00"

# Cloud retry protection.
MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 300  # 5 minutes

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# =========================================================
# HTTP HEADERS
# =========================================================

DSE_HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36",

    "Accept":
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/webp,*/*;q=0.8",

    "Accept-Language":
        "en-US,en;q=0.9",

    "Connection":
        "keep-alive"
}


SME_HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36",

    "Accept":
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/webp,*/*;q=0.8",

    "Accept-Language":
        "en-US,en;q=0.9",

    "Connection":
        "keep-alive",

    "Referer":
        "https://sme.dsebd.org/"
}
# =========================================================
# SAFE FLOAT CONVERSION
# =========================================================

def safe_float(value):

    try:

        value = str(value).strip()

        value = value.replace(",", "")

        value = value.replace("--", "0")

        value = value.replace("N/A", "0")

        if value == "":
            return 0.0

        return round(
            float(value),
            2
        )

    except:

        return 0.0


# =========================================================
# SAFE INT CONVERSION
# =========================================================

def safe_int(value):

    try:

        value = str(value).strip()

        value = value.replace(",", "")

        value = value.replace("--", "0")

        value = value.replace("N/A", "0")

        if value == "":
            return 0

        return int(
            float(value)
        )

    except:

        return 0


# =========================================================
# MARKET CLOSE CHECK
# =========================================================

def market_is_closed():

    current_time = datetime.now().time()

    return (
        current_time.hour > MARKET_CLOSE_HOUR
        or
        (
            current_time.hour == MARKET_CLOSE_HOUR
            and
            current_time.minute >= MARKET_CLOSE_MINUTE
        )
    )


# =========================================================
# DSE DOWNLOAD PROTECTION
#
# BLOCK:
#
# 09:30:00 AM <= TIME < 10:00:00 AM
#
# No DSE request
# No SME request
# No CSV overwrite
# No DSEX append
# =========================================================

def dse_download_allowed():

    current_time = datetime.now().time()

    block_start = datetime.strptime(
        DSE_RESET_START,
        "%H:%M:%S"
    ).time()

    block_end = datetime.strptime(
        DSE_RESET_END,
        "%H:%M:%S"
    ).time()


    # =====================================================
    # BLOCK RESET WINDOW
    # =====================================================

    if (
        current_time >= block_start
        and
        current_time < block_end
    ):

        print()
        print("=" * 70)
        print("DSE DATA DOWNLOAD BLOCKED")
        print("=" * 70)

        print()
        print(
            "Current Time:",
            current_time.strftime("%I:%M:%S %p")
        )

        print(
            "Blocked Window:",
            "09:30:00 AM - 10:00:00 AM BST"
        )

        print()
        print(
            "Reason:"
        )

        print(
            "DSE website is in the new-day reset period."
        )

        print(
            "Market data may temporarily appear empty/zero."
        )

        print()
        print(
            "NO DSE DATA WILL BE DOWNLOADED."
        )

        print(
            "NO SME DATA WILL BE DOWNLOADED."
        )

        print(
            "EXISTING CSV WILL NOT BE CHANGED."
        )

        logging.warning(
            "DSE download blocked during "
            "09:30-10:00 BST reset window | "
            f"Current={current_time}"
        )

        return False


    return True


# =========================================================
# FIND COLUMN
# =========================================================

def find_column(
    df,
    possible_names
):

    for name in possible_names:

        if name in df.columns:

            return name

    return None


# =========================================================
# GET OFFICIAL DSE TRADING DATE
#
# SOURCE:
# https://www.dsebd.org/index.php
#
# Example:
#
# Last update on Aug 23, 2026 at 2:40 PM
#
# Result:
#
# 2026-08-23
#
# IMPORTANT:
# This is the master date used by DSE + SME.
# =========================================================

def get_dse_trading_date():

    print()
    print("=" * 70)
    print("0. GETTING OFFICIAL DSE TRADING DATE")
    print("=" * 70)


    try:

        session = requests.Session()

        response = session.get(
            DSE_INDEX_URL,
            headers=DSE_HEADERS,
            timeout=30,
            verify=False
        )

        response.raise_for_status()

        html = response.text


        print()
        print(
            "DSE Date URL:",
            DSE_INDEX_URL
        )

        print(
            "DSE Date Response:",
            len(response.content),
            "bytes"
        )


        # =================================================
        # FIND:
        #
        # Last update on Aug 23, 2026 at 2:40 PM
        # =================================================

        match = re.search(

            r"Last\s+update\s+on\s+"
            r"([A-Za-z]+)\s+"
            r"(\d{1,2}),\s+"
            r"(\d{4})",

            html,

            re.IGNORECASE
        )


        if match is None:

            logging.error(
                "DSE official trading date not found"
            )

            print()
            print(
                "ERROR: DSE OFFICIAL TRADING DATE NOT FOUND"
            )

            return False


        month_name = match.group(1)

        day_number = match.group(2)

        year_number = match.group(3)


        # =================================================
        # CONVERT DATE
        # =================================================

        try:

            trading_date = datetime.strptime(

                f"{month_name} "
                f"{day_number} "
                f"{year_number}",

                "%B %d %Y"

            ).strftime(
                DATE_FMT
            )


        except ValueError:

            trading_date = datetime.strptime(

                f"{month_name} "
                f"{day_number} "
                f"{year_number}",

                "%b %d %Y"

            ).strftime(
                DATE_FMT
            )


        print()
        print(
            "Official DSE Trading Date:",
            trading_date
        )


        logging.info(
            f"Official DSE trading date: {trading_date}"
        )


        return trading_date


    except Exception as e:

        logging.error(
            f"DSE trading date fetch failed: {e}"
        )

        print(
            "DSE DATE ERROR:",
            e
        )

        return False


# =========================================================
# PART 1: SCRAPE MAIN DSE STOCKS
# =========================================================

def scrape_dse_latest_prices(
    trading_date
):

    print()
    print("=" * 70)
    print("1. MAIN DSE DATA")
    print("=" * 70)


    # =====================================================
    # REQUEST
    # =====================================================

    try:

        session = requests.Session()

        response = session.get(
            DSE_HTML_URL,
            headers=DSE_HEADERS,
            timeout=30,
            verify=False
        )

        response.raise_for_status()


        print(
            "HTTP Status:",
            response.status_code
        )

        print(
            "Response Size:",
            len(response.content),
            "bytes"
        )


    except Exception as e:

        logging.error(
            f"DSE fetch failed: {e}"
        )

        print(
            "DSE FETCH FAILED:",
            e
        )

        return False


    # =====================================================
    # PARSE HTML
    # =====================================================

    soup = BeautifulSoup(
        response.text,
        "html.parser"
    )


    # =====================================================
    # FIND DSE TABLE
    # =====================================================

    tables = soup.find_all(
        "table"
    )

    table = None


    for t in tables:

        headers_test = [

            th.get_text(
                strip=True
            ).upper()

            for th in t.find_all("th")
        ]


        if "TRADING CODE" in headers_test:

            table = t

            break


    if table is None:

        logging.error(
            "DSE table not found"
        )

        print(
            "DSE TABLE PARSE FAILED"
        )

        return False


    rows = table.find_all(
        "tr"
    )


    if len(rows) <= 1:

        logging.error(
            "No DSE rows found"
        )

        print(
            "NO DSE ROWS FOUND"
        )

        return False


    # =====================================================
    # HEADERS
    # =====================================================

    headers = [

        th.get_text(
            strip=True
        )

        for th in rows[0].find_all("th")
    ]


    data = []


    for row in rows[1:]:

        cols = [

            td.get_text(
                strip=True
            )

            for td in row.find_all("td")
        ]


        if len(cols) == len(headers):

            data.append(cols)


    if not data:

        logging.error(
            "No DSE stock data parsed"
        )

        print(
            "NO DSE STOCK DATA"
        )

        return False


    # =====================================================
    # DATAFRAME
    # =====================================================

    df = pd.DataFrame(
        data,
        columns=headers
    )


    # =====================================================
    # REMOVE SERIAL COLUMN
    # =====================================================

    first_col = df.columns[0].upper()


    if (
        "SL" in first_col
        or
        first_col == "#"
    ):

        df.drop(
            columns=df.columns[0],
            inplace=True
        )


    # =====================================================
    # MARKET TIME
    # =====================================================

    after_market = market_is_closed()


    # =====================================================
    # CLOSE SOURCE
    # =====================================================

    close_source = "LTP*"


    if (
        after_market
        and
        "CLOSEP*" in df.columns
    ):

        close_source = "CLOSEP*"


    logging.info(
        f"DSE close source: {close_source}"
    )


    print(
        "DSE Close Source:",
        close_source
    )


    # =====================================================
    # COLUMN RENAME
    # =====================================================

    rename_map = {

        "TRADING CODE":
            "Ticker",

        "YCP*":
            "Open",

        "HIGH":
            "High",

        "LOW":
            "Low",

        close_source:
            "Close",

        "VOLUME":
            "Volume"
    }


    df.rename(
        columns=rename_map,
        inplace=True
    )


    # =====================================================
    # REQUIRED COLUMNS
    # =====================================================

    required_cols = [

        "Ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume"
    ]


    for col in required_cols:

        if col not in df.columns:

            print(
                "DSE MISSING COLUMN:",
                col
            )

            logging.error(
                f"DSE missing column: {col}"
            )

            return False


    # =====================================================
    # CLEAN DATA
    # =====================================================

    for col in [
        "Open",
        "High",
        "Low",
        "Close"
    ]:

        df[col] = df[col].apply(
            safe_float
        )


    df["Volume"] = df["Volume"].apply(
        safe_int
    )


    # =====================================================
    # REMOVE INVALID CLOSE
    # =====================================================

    df = df[
        df["Close"] > 0
    ]


    # =====================================================
    # VALID SYMBOLS
    # =====================================================

    df = df[
        df["Ticker"]
        .astype(str)
        .str.match(
            r"^[A-Z0-9]+$"
        )
    ]


    # =====================================================
    # CRITICAL EMPTY-DATA PROTECTION
    # =====================================================

    if df.empty:

        logging.error(
            "DSE returned zero valid symbols"
        )

        print()
        print(
            "ERROR: DSE RETURNED ZERO VALID SYMBOLS"
        )

        print(
            "DSE DATA WILL NOT BE WRITTEN."
        )

        return False


    # =====================================================
    # DATE
    #
    # OFFICIAL DSE DATE
    # =====================================================

    df["Date"] = trading_date


    # =====================================================
    # FINAL COLUMN ORDER
    # =====================================================

    df = df[
        [
            "Ticker",
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume"
        ]
    ]


    # =====================================================
    # SORT
    # =====================================================

    df.sort_values(
        "Ticker",
        inplace=True
    )


    print()
    print(
        "DSE Symbols:",
        len(df)
    )

    print(
        "DSE Trading Date:",
        trading_date
    )


    logging.info(
        f"DSE stocks scraped: {len(df)}"
    )


    return df


# =========================================================
# PART 2: SCRAPE SME DSE STOCKS
# =========================================================

def scrape_sme_latest_prices(
    trading_date
):

    print()
    print("=" * 70)
    print("2. SME DSE DATA")
    print("=" * 70)


    # =====================================================
    # REQUEST
    # =====================================================

    try:

        session = requests.Session()

        response = session.get(
            SME_HTML_URL,
            headers=SME_HEADERS,
            timeout=30,
            verify=False
        )

        response.raise_for_status()


        print(
            "HTTP Status:",
            response.status_code
        )

        print(
            "Response Size:",
            len(response.content),
            "bytes"
        )


    except Exception as e:

        logging.error(
            f"SME fetch failed: {e}"
        )

        print(
            "SME FETCH FAILED:",
            e
        )

        return False


    # =====================================================
    # PARSE HTML
    # =====================================================

    soup = BeautifulSoup(
        response.text,
        "html.parser"
    )


    tables = soup.find_all(
        "table"
    )


    print(
        "SME Tables Found:",
        len(tables)
    )


    if not tables:

        logging.error(
            "SME table not found"
        )

        print(
            "SME TABLE NOT FOUND"
        )

        return False


    # =====================================================
    # FIND SME TABLE
    # =====================================================

    table = None


    for t in tables:

        headers_test = [

            th.get_text(
                strip=True
            ).upper()

            for th in t.find_all("th")
        ]


        table_text = t.get_text(
            " ",
            strip=True
        ).upper()


        if (
            "TRADING CODE" in headers_test
            or
            "TRADINGCODE" in headers_test
            or
            "TRADING CODE" in table_text
        ):

            table = t

            break


    # =====================================================
    # FALLBACK
    # =====================================================

    if table is None:

        print(
            "WARNING: SME Trading Code table "
            "not automatically detected."
        )

        print(
            "Using first table."
        )

        table = tables[0]


    # =====================================================
    # ROWS
    # =====================================================

    rows = table.find_all(
        "tr"
    )


    print(
        "SME Rows Found:",
        len(rows)
    )


    if len(rows) <= 1:

        logging.error(
            "No SME rows found"
        )

        print(
            "NO SME DATA ROWS"
        )

        return False


    # =====================================================
    # HEADERS
    # =====================================================

    headers = [

        th.get_text(
            strip=True
        )

        for th in rows[0].find_all("th")
    ]


    # =====================================================
    # FALLBACK HEADER
    # =====================================================

    if not headers:

        headers = [

            td.get_text(
                strip=True
            )

            for td in rows[0].find_all("td")
        ]


    # =====================================================
    # NORMALIZE HEADERS
    # =====================================================

    headers = [

        str(x)
        .strip()
        .upper()

        for x in headers
    ]


    print()
    print(
        "SME COLUMNS:"
    )


    for i, col in enumerate(
        headers,
        start=1
    ):

        print(
            f"{i:2}. {col}"
        )


    # =====================================================
    # DATA
    # =====================================================

    data = []


    for row in rows[1:]:

        cols = [

            td.get_text(
                strip=True
            )

            for td in row.find_all("td")
        ]


        if len(cols) == len(headers):

            data.append(cols)


    if not data:

        logging.error(
            "No SME data parsed"
        )

        print(
            "NO SME DATA PARSED"
        )

        return False


    # =====================================================
    # DATAFRAME
    # =====================================================

    df = pd.DataFrame(
        data,
        columns=headers
    )


    # =====================================================
    # FIND COLUMNS
    # =====================================================

    ticker_col = find_column(
        df,
        [
            "TRADING CODE",
            "TRADINGCODE",
            "CODE"
        ]
    )


    ltp_col = find_column(
        df,
        [
            "LTP",
            "LTP*",
            "LAST TRADE PRICE"
        ]
    )


    ycp_col = find_column(
        df,
        [
            "YCP*",
            "YCP"
        ]
    )


    high_col = find_column(
        df,
        [
            "HIGH"
        ]
    )


    low_col = find_column(
        df,
        [
            "LOW"
        ]
    )


    close_col = find_column(
        df,
        [
            "CLOSEP*",
            "CLOSEP",
            "CLOSE"
        ]
    )


    volume_col = find_column(
        df,
        [
            "VOLUME"
        ]
    )


    # =====================================================
    # SHOW DETECTED
    # =====================================================

    print()
    print(
        "SME DETECTED COLUMNS:"
    )

    print(
        "Ticker :",
        ticker_col
    )

    print(
        "LTP    :",
        ltp_col
    )

    print(
        "YCP    :",
        ycp_col
    )

    print(
        "High   :",
        high_col
    )

    print(
        "Low    :",
        low_col
    )

    print(
        "Close  :",
        close_col
    )

    print(
        "Volume :",
        volume_col
    )


    # =====================================================
    # TICKER REQUIRED
    # =====================================================

    if ticker_col is None:

        logging.error(
            "SME trading code column not found"
        )

        print()
        print(
            "ERROR: SME TRADING CODE COLUMN NOT FOUND"
        )

        return False


    # =====================================================
    # CLOSE SOURCE
    # =====================================================

    if market_is_closed() and close_col:

        close_source = close_col

    elif ltp_col:

        close_source = ltp_col

    elif close_col:

        close_source = close_col

    else:

        logging.error(
            "SME LTP/CLOSE column not found"
        )

        print()
        print(
            "ERROR: SME LTP/CLOSE COLUMN NOT FOUND"
        )

        return False


    print()
    print(
        "SME Close Source:",
        close_source
    )


    # =====================================================
    # CREATE STANDARD DATAFRAME
    # =====================================================

    result = pd.DataFrame()


    result["Ticker"] = (
        df[ticker_col]
        .astype(str)
        .str.strip()
    )


    # =====================================================
    # OPEN
    # =====================================================

    if ycp_col:

        result["Open"] = (
            df[ycp_col]
            .apply(safe_float)
        )

    else:

        result["Open"] = (
            df[close_source]
            .apply(safe_float)
        )


    # =====================================================
    # HIGH
    # =====================================================

    if high_col:

        result["High"] = (
            df[high_col]
            .apply(safe_float)
        )

    else:

        result["High"] = result["Open"]


    # =====================================================
    # LOW
    # =====================================================

    if low_col:

        result["Low"] = (
            df[low_col]
            .apply(safe_float)
        )

    else:

        result["Low"] = result["Open"]


    # =====================================================
    # CLOSE
    # =====================================================

    result["Close"] = (
        df[close_source]
        .apply(safe_float)
    )


    # =====================================================
    # VOLUME
    # =====================================================

    if volume_col:

        result["Volume"] = (
            df[volume_col]
            .apply(safe_int)
        )

    else:

        result["Volume"] = 0


    # =====================================================
    # REMOVE INVALID
    # =====================================================

    result = result[
        result["Close"] > 0
    ]


    # =====================================================
    # VALID SYMBOLS
    # =====================================================

    result = result[
        result["Ticker"]
        .astype(str)
        .str.match(
            r"^[A-Z0-9]+$"
        )
    ]


    # =====================================================
    # EMPTY-DATA PROTECTION
    # =====================================================

    if result.empty:

        logging.error(
            "SME returned zero valid symbols"
        )

        print()
        print(
            "ERROR: SME RETURNED ZERO VALID SYMBOLS"
        )

        print(
            "SME DATA WILL NOT BE WRITTEN."
        )

        return False


    # =====================================================
    # DATE
    # =====================================================

    result["Date"] = trading_date


    # =====================================================
    # FINAL FORMAT
    # =====================================================

    result = result[
        [
            "Ticker",
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume"
        ]
    ]


    # =====================================================
    # SORT
    # =====================================================

    result.sort_values(
        "Ticker",
        inplace=True
    )


    print()
    print(
        "SME Symbols:",
        len(result)
    )

    print(
        "SME Trading Date:",
        trading_date
    )


    logging.info(
        f"SME stocks scraped: {len(result)}"
    )


    return result


# =========================================================
# PART 3: COMBINE DSE + SME
# =========================================================

def combine_dse_sme(
    dse_df,
    sme_df
):

    print()
    print("=" * 70)
    print("3. COMBINING DSE + SME")
    print("=" * 70)


    try:

        # =================================================
        # VALIDATE INPUT
        # =================================================

        if (
            dse_df is None
            or
            sme_df is None
            or
            dse_df.empty
            or
            sme_df.empty
        ):

            logging.error(
                "DSE or SME dataframe is empty"
            )

            print()
            print(
                "ERROR: DSE OR SME DATA IS EMPTY"
            )

            print(
                "COMBINATION ABORTED."
            )

            return False


        # =================================================
        # COMBINE
        # =================================================

        combined_df = pd.concat(
            [
                dse_df,
                sme_df
            ],
            ignore_index=True
        )


        # =================================================
        # COUNT BEFORE DUPLICATE REMOVAL
        # =================================================

        total_before = len(
            combined_df
        )


        # =================================================
        # REMOVE DUPLICATE TICKERS
        # =================================================

        combined_df.drop_duplicates(
            subset=[
                "Ticker"
            ],
            keep="first",
            inplace=True
        )


        total_after = len(
            combined_df
        )


        duplicates = (
            total_before -
            total_after
        )


        # =================================================
        # FINAL EMPTY CHECK
        # =================================================

        if combined_df.empty:

            logging.error(
                "Combined DSE + SME dataframe is empty"
            )

            print()
            print(
                "ERROR: COMBINED DATA IS EMPTY"
            )

            return False


        # =================================================
        # SORT
        # =================================================

        combined_df.sort_values(
            "Ticker",
            inplace=True
        )


        print()
        print(
            "DSE Symbols      :",
            len(dse_df)
        )

        print(
            "SME Symbols      :",
            len(sme_df)
        )

        print(
            "Duplicates Removed:",
            duplicates
        )

        print(
            "Combined Symbols  :",
            len(combined_df)
        )


        return combined_df


    except Exception as e:

        logging.error(
            f"DSE + SME combine failed: {e}"
        )

        print(
            "COMBINE FAILED:",
            e
        )

        return False


# =========================================================
# PART 4: WRITE COMBINED CSV
# =========================================================

def write_combined_csv(
    combined_df
):

    print()
    print("=" * 70)
    print("4. WRITING INTRADAY EOD CSV")
    print("=" * 70)


    # =====================================================
    # CRITICAL EMPTY DATA PROTECTION
    # =====================================================

    if (
        combined_df is None
        or
        combined_df.empty
    ):

        logging.error(
            "Refusing to write empty DSE + SME CSV"
        )

        print()
        print(
            "ERROR: REFUSING TO WRITE EMPTY CSV"
        )

        print(
            "Existing CSV will NOT be changed."
        )

        return False


    # =====================================================
    # BACKUP EXISTING CSV
    # =====================================================

    try:

        if os.path.exists(
            CSV_FILE
        ):

            backup_folder = (
                rf"{BASE_DIR}\BACKUP"
            )


            os.makedirs(
                backup_folder,
                exist_ok=True
            )


            backup_file = (
                rf"{backup_folder}"
                r"\Intraday_EOD_backup.csv"
            )


            shutil.copy(
                CSV_FILE,
                backup_file
            )


            print(
                "Backup created:"
            )

            print(
                backup_file
            )


    except Exception as e:

        logging.warning(
            f"Backup failed: {e}"
        )


    # =====================================================
    # TEMP FILE
    # =====================================================

    temp_csv = (
        CSV_FILE + ".tmp"
    )


    try:

        combined_df.to_csv(
            temp_csv,
            index=False,
            header=False
        )


        shutil.move(
            temp_csv,
            CSV_FILE
        )


    except PermissionError:

        print()
        print(
            "ERROR: FILE LOCKED"
        )

        print(
            "Close Excel / AmiBroker / Notepad"
        )


        logging.error(
            "CSV file locked"
        )


        return False


    except Exception as e:

        print(
            "WRITE FAILED:",
            e
        )


        logging.error(
            f"Write failed: {e}"
        )


        return False


    print()
    print(
        "SUCCESS:"
    )


    print(
        len(combined_df),
        "DSE + SME symbols written"
    )


    print()
    print(
        "CSV:"
    )


    print(
        CSV_FILE
    )


    logging.info(
        f"Combined DSE + SME written: "
        f"{len(combined_df)} symbols | "
        f"Date={combined_df['Date'].iloc[0]}"
    )


    return True


```python
# =========================================================
# PART 5: APPEND DSEX
#
# DSEX OPEN   -> TradingView
# DSEX HIGH   -> TradingView
# DSEX LOW    -> TradingView
# DSEX CLOSE  -> DSE Website
# DSEX VOLUME -> DSE Website Total Volume
# DSEX DATE   -> DSE Website "Last update on"
#
# IMPORTANT:
# DSE website HTML structure can change.
# Therefore DSEX CLOSE and VOLUME use multiple
# extraction methods.
# =========================================================


def append_dsex_index():

    print()
    print("=" * 70)
    print("5. APPENDING DSEX")
    print("=" * 70)

    # =====================================================
    # 1. GET DSEX OHLC FROM TRADINGVIEW
    # =====================================================

    try:

        handler = TA_Handler(
            symbol="DSEX",
            exchange="DSEBD",
            screener="bangladesh",
            interval=Interval.INTERVAL_1_DAY
        )

        analysis = handler.get_analysis()

        ind = analysis.indicators

        tv_open = round(
            float(ind.get("open", 0)),
            2
        )

        tv_high = round(
            float(ind.get("high", 0)),
            2
        )

        tv_low = round(
            float(ind.get("low", 0)),
            2
        )

        print()
        print("TradingView DSEX OHLC:")

        print(
            "Open :",
            tv_open
        )

        print(
            "High :",
            tv_high
        )

        print(
            "Low  :",
            tv_low
        )

        # -------------------------------------------------
        # VALIDATE TRADINGVIEW DATA
        # -------------------------------------------------

        if (
            tv_open <= 0
            or
            tv_high <= 0
            or
            tv_low <= 0
        ):

            logging.error(
                "Invalid TradingView DSEX OHLC"
            )

            print()
            print(
                "ERROR: INVALID TRADINGVIEW DSEX OHLC"
            )

            return False

    except Exception as e:

        logging.error(
            f"TradingView DSEX fetch failed: {e}"
        )

        print(
            "TRADINGVIEW DSEX ERROR:",
            e
        )

        return False


    # =====================================================
    # 2. GET DSEX DATA FROM DSE WEBSITE
    # =====================================================

    try:

        session = requests.Session()

        response = session.get(
            DSE_INDEX_URL,
            headers=DSE_HEADERS,
            timeout=30,
            verify=False
        )

        response.raise_for_status()

        html = response.text

        print()
        print(
            "DSE URL:",
            DSE_INDEX_URL
        )

        print(
            "DSE Final URL:",
            response.url
        )

        print(
            "DSE HTTP Status:",
            response.status_code
        )

        print(
            "DSE Response Size:",
            len(response.content),
            "bytes"
        )


        # =================================================
        # BEAUTIFULSOUP
        # =================================================

        soup = BeautifulSoup(
            html,
            "html.parser"
        )


        # =================================================
        # 2A. GET OFFICIAL DSE DATE
        #
        # SOURCE:
        #
        # Last update on Aug 25, 2026 at 2:40 PM
        #
        # IMPORTANT:
        # This date is the master DSE trading date.
        # =================================================

        date_match = re.search(

            r"Last\s+update\s+on\s+"
            r"([A-Za-z]+)\s+"
            r"(\d{1,2}),\s+"
            r"(\d{4})",

            html,

            re.IGNORECASE
        )


        if date_match is None:

            logging.error(
                "DSE Last update date not found"
            )

            print()
            print(
                "ERROR: DSE LAST UPDATE DATE NOT FOUND"
            )

            return False


        month_name = date_match.group(1)

        day_number = date_match.group(2)

        year_number = date_match.group(3)


        try:

            dse_date = datetime.strptime(

                f"{month_name} "
                f"{day_number} "
                f"{year_number}",

                "%B %d %Y"

            ).strftime(
                DATE_FMT
            )

        except ValueError:

            dse_date = datetime.strptime(

                f"{month_name} "
                f"{day_number} "
                f"{year_number}",

                "%b %d %Y"

            ).strftime(
                DATE_FMT
            )


        print()
        print(
            "DSE Official Date:",
            dse_date
        )


        # =================================================
        # 2B. DSEX CLOSE
        #
        # IMPORTANT:
        #
        # Current DSE HTML may not expose the DSEX row
        # through BeautifulSoup's expected structure.
        #
        # Therefore several methods are attempted.
        # =================================================

        dse_close = 0.0


        # =================================================
        # METHOD 1
        #
        # Search HTML for:
        #
        # DSE X Index
        #
        # and inspect nearby HTML.
        # =================================================

        try:

            dsex_pattern = re.compile(
                r"DSE\s*"
                r"(?:<[^>]+>\s*)?"
                r*X
                r"(?:</[^>]+>\s*)?"
                r"Index",
                re.IGNORECASE
            )

            dsex_match = dsex_pattern.search(
                html
            )

        except Exception:

            dsex_match = None


        if dsex_match:

            start_pos = max(
                0,
                dsex_match.start() - 3000
            )

            end_pos = min(
                len(html),
                dsex_match.end() + 5000
            )

            dsex_context = html[
                start_pos:end_pos
            ]

            # ---------------------------------------------
            # Look for m_col-2 values
            # ---------------------------------------------

            value_matches = re.findall(

                r'<div\s+class=["\']'
                r'[^"\']*m_col-2[^"\']*'
                r'["\'][^>]*>'
                r'\s*'
                r'([0-9][0-9,\.\s]*)'
                r'\s*'
                r'</div>',

                dsex_context,

                re.IGNORECASE
            )


            for value_text in value_matches:

                candidate = safe_float(
                    value_text
                )

                # DSEX index is normally in this range.
                if (
                    candidate > 1000
                    and
                    candidate < 20000
                ):

                    dse_close = candidate

                    break


        # =================================================
        # METHOD 2
        #
        # Find "DSEX Index" using BeautifulSoup and
        # inspect parent elements.
        # =================================================

        if dse_close <= 0:

            label_divs = soup.find_all(
                "div",
                class_="m_col-1"
            )

            for label_div in label_divs:

                label_text = label_div.get_text(
                    " ",
                    strip=True
                )

                label_text = " ".join(
                    label_text.split()
                )

                if (
                    label_text.upper()
                    !=
                    "DSEX INDEX"
                ):

                    continue


                # -----------------------------------------
                # Try nearest midrow
                # -----------------------------------------

                parent_row = label_div.find_parent(
                    "div",
                    class_="midrow"
                )


                if parent_row is not None:

                    value_div = parent_row.find(
                        "div",
                        class_="m_col-2"
                    )

                    if value_div is not None:

                        candidate = safe_float(
                            value_div.get_text(
                                " ",
                                strip=True
                            )
                        )

                        if (
                            candidate > 1000
                            and
                            candidate < 20000
                        ):

                            dse_close = candidate

                            break


        # =================================================
        # METHOD 3
        #
        # Generic HTML text extraction.
        #
        # Search text around "DSEX Index".
        # =================================================

        if dse_close <= 0:

            try:

                page_text = soup.get_text(
                    " ",
                    strip=True
                )

                page_text = " ".join(
                    page_text.split()
                )

                dsex_text_match = re.search(

                    r"DSEX\s+Index"
                    r".{0,500}?"
                    r"([0-9]{3,5}\.[0-9]{1,5})",

                    page_text,

                    re.IGNORECASE
                )


                if dsex_text_match:

                    candidate = safe_float(
                        dsex_text_match.group(1)
                    )

                    if (
                        candidate > 1000
                        and
                        candidate < 20000
                    ):

                        dse_close = candidate


            except Exception as e:

                logging.warning(
                    f"DSEX text extraction failed: {e}"
                )


        # =================================================
        # METHOD 4
        #
        # Search known DSEX index page.
        #
        # The main DSE page can contain navigation
        # references instead of the actual index data.
        # =================================================

        if dse_close <= 0:

            try:

                dsex_response = session.get(

                    "https://www.dsebd.org/dseX_share.php",

                    headers=DSE_HEADERS,

                    timeout=30,

                    verify=False
                )

                if dsex_response.status_code == 200:

                    dsex_html = (
                        dsex_response.text
                    )

                    dsex_soup = BeautifulSoup(
                        dsex_html,
                        "html.parser"
                    )


                    # -------------------------------------
                    # Search m_col-2
                    # -------------------------------------

                    value_divs = dsex_soup.find_all(
                        "div",
                        class_="m_col-2"
                    )


                    for value_div in value_divs:

                        candidate = safe_float(
                            value_div.get_text(
                                " ",
                                strip=True
                            )
                        )

                        if (
                            candidate > 1000
                            and
                            candidate < 20000
                        ):

                            dse_close = candidate

                            break


                    # -------------------------------------
                    # Raw HTML fallback
                    # -------------------------------------

                    if dse_close <= 0:

                        value_matches = re.findall(

                            r'<div\s+class=["\']'
                            r'[^"\']*m_col-2[^"\']*'
                            r'["\'][^>]*>'
                            r'\s*'
                            r'([0-9][0-9,\.\s]*)'
                            r'\s*'
                            r'</div>',

                            dsex_html,

                            re.IGNORECASE
                        )


                        for value_text in value_matches:

                            candidate = safe_float(
                                value_text
                            )

                            if (
                                candidate > 1000
                                and
                                candidate < 20000
                            ):

                                dse_close = candidate

                                break


            except Exception as e:

                logging.warning(
                    f"DSEX dedicated page extraction failed: {e}"
                )


        # =================================================
        # FINAL DSEX CLOSE VALIDATION
        # =================================================

        if dse_close <= 0:

            print()
            print(
                "ERROR: DSE DSEX CLOSE NOT FOUND"
            )

            logging.error(
                "DSE DSEX close not found"
            )

            return False


        print()
        print(
            "DSE DSEX Close:",
            dse_close
        )


        # =================================================
        # 2C. GET TOTAL VOLUME
        #
        # PRIMARY:
        #
        # m_col-wid1 colorlight
        #
        # FALLBACK:
        # Raw HTML.
        # =================================================

        dse_volume = 0


        # -------------------------------------------------
        # METHOD 1: BeautifulSoup
        # -------------------------------------------------

        volume_div = soup.find(
            "div",
            class_="m_col-wid1 colorlight"
        )


        if volume_div is not None:

            dse_volume = safe_int(

                volume_div.get_text(
                    " ",
                    strip=True
                )
            )


        # -------------------------------------------------
        # METHOD 2: Raw HTML
        # -------------------------------------------------

        if dse_volume <= 0:

            volume_matches = re.findall(

                r'<div\s+class=["\']'
                r'[^"\']*m_col-wid1[^"\']*'
                r'colorlight'
                r'[^"\']*'
                r'["\'][^>]*>'
                r'\s*'
                r'([0-9][0-9,\.\s]*)'
                r'\s*'
                r'</div>',

                html,

                re.IGNORECASE
            )


            for value_text in volume_matches:

                candidate = safe_int(
                    value_text
                )

                if candidate > 0:

                    dse_volume = candidate

                    break


        # -------------------------------------------------
        # METHOD 3: Search Total Volume text
        # -------------------------------------------------

        if dse_volume <= 0:

            try:

                volume_text_match = re.search(

                    r"Total\s+Volume"
                    r".{0,500}?"
                    r"([0-9][0-9,\.\s]{5,})",

                    soup.get_text(
                        " ",
                        strip=True
                    ),

                    re.IGNORECASE
                )


                if volume_text_match:

                    candidate = safe_int(
                        volume_text_match.group(1)
                    )

                    if candidate > 0:

                        dse_volume = candidate


            except Exception as e:

                logging.warning(
                    f"Total volume text extraction failed: {e}"
                )


        # =================================================
        # FINAL VOLUME VALIDATION
        # =================================================

        if dse_volume <= 0:

            logging.error(
                "DSE Total Volume not found"
            )

            print()
            print(
                "ERROR: DSE TOTAL VOLUME NOT FOUND"
            )

            return False


        print(
            "DSE Total Volume:",
            dse_volume
        )


    except Exception as e:

        logging.error(
            f"DSE DSEX data fetch failed: {e}"
        )

        print()
        print(
            "DSE DSEX DATA ERROR:",
            e
        )

        return False


    # =====================================================
    # 3. CREATE DSEX ROW
    # =====================================================

    row = [

        "DSEX",

        dse_date,

        tv_open,

        tv_high,

        tv_low,

        dse_close,

        dse_volume

    ]


    # =====================================================
    # 4. DISPLAY FINAL DSEX ROW
    # =====================================================

    print()
    print(
        "DSEX data collected successfully"
    )

    print()
    print(
        "Final DSEX Row:"
    )

    print(
        "Ticker :",
        row[0]
    )

    print(
        "Date   :",
        row[1]
    )

    print(
        "Open   :",
        row[2]
    )

    print(
        "High   :",
        row[3]
    )

    print(
        "Low    :",
        row[4]
    )

    print(
        "Close  :",
        row[5]
    )

    print(
        "Volume :",
        row[6]
    )


    logging.info(
        "DSEX collected successfully | "
        f"Date={dse_date}, "
        f"Open={tv_open}, "
        f"High={tv_high}, "
        f"Low={tv_low}, "
        f"Close={dse_close}, "
        f"Volume={dse_volume}"
    )


    return row
```



# =========================================================
# CLOUD HELPERS
# =========================================================

def normalize_date(value):
    return str(value).strip()


def validate_final_dataset(combined_df, dsex_row, trading_date):
    """Final safety gate before publishing a daily archive."""
    if combined_df is None or combined_df.empty:
        raise ValueError("Combined DSE + SME data is empty")

    if dsex_row is None or len(dsex_row) != 7:
        raise ValueError("DSEX row is missing or malformed")

    expected_columns = [
        "Ticker", "Date", "Open", "High", "Low", "Close", "Volume"
    ]

    if list(combined_df.columns) != expected_columns:
        raise ValueError(
            f"Unexpected columns: {list(combined_df.columns)}"
        )

    if combined_df["Ticker"].duplicated().any():
        raise ValueError("Duplicate ticker found in DSE + SME data")

    if not (combined_df["Date"].astype(str) == trading_date).all():
        raise ValueError("DSE/SME date mismatch")

    if dsex_row[0] != "DSEX":
        raise ValueError("DSEX ticker is invalid")

    if normalize_date(dsex_row[1]) != trading_date:
        raise ValueError(
            f"DSEX date mismatch: DSEX={dsex_row[1]} "
            f"official={trading_date}"
        )

    if float(dsex_row[2]) <= 0 or float(dsex_row[3]) <= 0:
        raise ValueError("Invalid DSEX Open/High")

    if float(dsex_row[4]) <= 0 or float(dsex_row[5]) <= 0:
        raise ValueError("Invalid DSEX Low/Close")

    if int(dsex_row[6]) <= 0:
        raise ValueError("Invalid DSEX Volume")

    if (combined_df["Close"] <= 0).any():
        raise ValueError("Invalid stock Close <= 0")

    if (combined_df["Volume"] < 0).any():
        raise ValueError("Invalid stock Volume < 0")

    return True


def write_daily_archive(combined_df, dsex_row, trading_date):
    """Write a dated CSV and latest CSV only after all validation passes."""
    final_df = pd.concat(
        [
            combined_df,
            pd.DataFrame(
                [dsex_row],
                columns=combined_df.columns
            )
        ],
        ignore_index=True
    )

    if final_df["Ticker"].duplicated().any():
        raise ValueError("DSEX or stock ticker duplicate detected")

    validate_final_dataset(combined_df, dsex_row, trading_date)

    year = trading_date[:4]
    month = trading_date[5:7]

    month_dir = DATA_DIR / year / month
    month_dir.mkdir(parents=True, exist_ok=True)

    daily_file = month_dir / f"DSE_EOD_{trading_date}.csv"
    latest_file = LATEST_DIR / "DSE_EOD_latest.csv"

    temp_daily = daily_file.with_suffix(".csv.tmp")
    temp_latest = latest_file.with_suffix(".csv.tmp")

    # Atomic replacement: never leave a half-written CSV.
    final_df.to_csv(temp_daily, index=False, header=False)
    temp_daily.replace(daily_file)

    final_df.to_csv(temp_latest, index=False, header=False)
    temp_latest.replace(latest_file)

    print()
    print("=" * 70)
    print("CLOUD ARCHIVE PUBLISHED")
    print("=" * 70)
    print("Daily CSV:")
    print(daily_file)
    print("Latest CSV:")
    print(latest_file)
    print("Rows:", len(final_df))

    logging.info(
        f"Published {daily_file} | rows={len(final_df)} | date={trading_date}"
    )

    return daily_file


def run_once():
    """One complete DSE + SME + DSEX collection attempt."""
    print()
    print("=" * 70)
    print("DSE + SME + DSEX CLOUD DATA FEEDER")
    print("=" * 70)

    if not dse_download_allowed():
        print()
        print("FEEDER STOPPED SAFELY.")
        return False

    trading_date = get_dse_trading_date()

    if trading_date is False:
        raise RuntimeError("Could not determine official DSE trading date")

    dse_df = scrape_dse_latest_prices(trading_date)

    if dse_df is False:
        raise RuntimeError("Main DSE scraping failed")

    sme_df = scrape_sme_latest_prices(trading_date)

    if sme_df is False:
        raise RuntimeError("SME scraping failed")

    combined_df = combine_dse_sme(dse_df, sme_df)

    if combined_df is False:
        raise RuntimeError("DSE + SME combination failed")

    # DSEX is collected before ANY archive is published.
    dsex_row = append_dsex_index()

    if dsex_row is False:
        raise RuntimeError("DSEX collection failed")

    daily_file = write_daily_archive(
        combined_df,
        dsex_row,
        trading_date
    )

    print()
    print("=" * 70)
    print("DSE + SME + DSEX COMPLETE")
    print("=" * 70)
    print("Official Trading Date:", trading_date)
    print("Daily CSV:", daily_file)
    print("DONE")

    return True


def main():
    for attempt in range(1, MAX_ATTEMPTS + 1):
        print()
        print(
            f"========== CLOUD ATTEMPT {attempt}/{MAX_ATTEMPTS} =========="
        )

        try:
            if run_once():
                return 0

        except Exception as e:
            logging.exception(f"Attempt {attempt} failed")
            print()
            print("ATTEMPT FAILED:", e)

        if attempt < MAX_ATTEMPTS:
            print()
            print(
                f"Retrying in {RETRY_DELAY_SECONDS // 60} minutes..."
            )
            time.sleep(RETRY_DELAY_SECONDS)

    print()
    print("=" * 70)
    print("CLOUD FEEDER FAILED")
    print("=" * 70)
    print("No new daily archive was published.")

    return 1


if __name__ == "__main__":
    sys.exit(main())
