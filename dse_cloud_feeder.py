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


# =============================================================
# 5. APPENDING DSEX
# =============================================================

def append_dsex_index():
    """
    Collect the DSEX EOD row.

    SOURCE RULES
    ------------
    TradingView:
        Open / High / Low

    DSE homepage:
        FIRST DSEX row  = latest trading day close
        SECOND DSEX row = preceding trade date close

        FIRST Total Volume block  = latest trading day volume
        SECOND Total Volume block = preceding trade date volume

    IMPORTANT
    ---------
    The DSE homepage is deliberately read by MIDROW position/content.
    We do NOT search arbitrary DSEX text.

    For the example supplied by the user:

        Latest trading day (2026-08-27)
            DSEX   = 5655.68313
            Volume = 177465534

        Preceding trade date (2026-08-25)
            DSEX   = 5640.08828
            Volume = 191683061

    Therefore the EOD row MUST use:

        Close  = 5655.68313
        Volume = 177465534
    """

    print()
    print("=" * 70)
    print("5. APPENDING DSEX")
    print("=" * 70)

    # =============================================================
    # 5A. TRADINGVIEW DSEX OHLC
    # =============================================================

    # TradingView identifies the DSE Broad Index as DSEBD:DSEX.
    # tradingview_ta expects:
    #   symbol   = DSEX
    #   exchange = DSEBD
    #   screener = bangladesh

    try:

        print()
        print("Getting DSEX OHLC from TradingView...")

        handler = TA_Handler(
            symbol="DSEX",
            exchange="DSEBD",
            screener="bangladesh",
            interval=Interval.INTERVAL_1_DAY,
            timeout=30
        )

        analysis = handler.get_analysis()

        indicators = analysis.indicators

        dsex_open = float(indicators["open"])
        dsex_high = float(indicators["high"])
        dsex_low = float(indicators["low"])

        print()
        print("TradingView DSEX OHLC:")

        print(
            "Open :",
            dsex_open
        )

        print(
            "High :",
            dsex_high
        )

        print(
            "Low  :",
            dsex_low
        )

    except Exception as e:

        logging.error(
            f"TradingView DSEX OHLC unavailable: {e}"
        )

        print()
        print(
            "ERROR: TradingView DSEX OHLC unavailable:",
            e
        )

        return False

    # =============================================================
    # 5B. DOWNLOAD DSE HOMEPAGE
    # =============================================================

    dse_url = DSE_INDEX_URL

    try:

        dse_session = requests.Session()

        dse_response = dse_session.get(
            dse_url,
            headers=DSE_HEADERS,
            timeout=30,
            verify=False
        )

        print()
        print(
            "DSE URL:",
            dse_url
        )

        print(
            "DSE Final URL:",
            dse_response.url
        )

        print(
            "DSE HTTP Status:",
            dse_response.status_code
        )

        print(
            "DSE Response Size:",
            len(dse_response.content),
            "bytes"
        )

        if dse_response.status_code != 200:

            logging.error(
                f"DSE homepage HTTP error: "
                f"{dse_response.status_code}"
            )

            print(
                "ERROR: DSE homepage download failed"
            )

            return False

        dse_html = dse_response.text

    except Exception as e:

        logging.error(
            f"DSE homepage request failed: {e}"
        )

        print(
            "ERROR: DSE homepage request failed:",
            e
        )

        return False

    # =============================================================
    # 5C. PARSE DSE HOMEPAGE
    # =============================================================

    try:

        dse_soup = BeautifulSoup(
            dse_html,
            "html.parser"
        )

    except Exception as e:

        logging.error(
            f"DSE HTML parsing failed: {e}"
        )

        print(
            "ERROR: DSE HTML parsing failed:",
            e
        )

        return False

    # =============================================================
    # 5D. OFFICIAL DSE DATE
    # =============================================================

    official_date = get_dse_trading_date()

    if official_date is False:

        logging.error(
            "Official DSE trading date unavailable"
        )

        print(
            "ERROR: Official DSE date unavailable"
        )

        return False

    print()
    print(
        "DSE Official Date:",
        official_date
    )

    # =============================================================
    # 5E. FIND ALL MIDROW SECTIONS
    # =============================================================

    midrows = dse_soup.find_all(
        "div",
        class_="midrow"
    )

    print()
    print(
        "DSE Midrow Sections Found:",
        len(midrows)
    )

    if not midrows:

        logging.error(
            "No DSE midrow sections found"
        )

        print(
            "ERROR: No DSE Midrow sections found"
        )

        return False

    for row_number, row_div in enumerate(
        midrows,
        start=1
    ):

        row_text = row_div.get_text(
            " ",
            strip=True
        )

        row_text = " ".join(
            row_text.split()
        )

        print(
            f"DSE Midrow {row_number}:",
            row_text[:200]
        )

    # =============================================================
    # 5F. COLLECT DSEX ROWS
    # =============================================================

    dsex_rows = []

    for row_number, row_div in enumerate(
        midrows,
        start=1
    ):

        row_text = row_div.get_text(
            " ",
            strip=True
        )

        row_text = " ".join(
            row_text.split()
        )

        normalized_upper = re.sub(
            r"\s+",
            " ",
            row_text
        ).strip().upper()

        # BeautifulSoup may render:
        #
        #   DSE X Index
        #
        # or:
        #
        #   DSEX Index
        #
        # Accept both.

        is_dsex_row = (
            "DSEX INDEX" in normalized_upper
            or
            "DSE X INDEX" in normalized_upper
        )

        if not is_dsex_row:

            continue

        print()
        print(
            "DSEX ROW DETECTED:",
            row_text
        )

        # IMPORTANT:
        # Get the value from m_col-2 inside THIS row.
        # This prevents navigation/other DSEX text from being used.

        value_div = row_div.find(
            "div",
            class_="m_col-2"
        )

        if value_div is None:

            print(
                "DSEX m_col-2 NOT FOUND"
            )

            continue

        value_text = value_div.get_text(
            " ",
            strip=True
        )

        print(
            "DSEX VALUE TEXT:",
            repr(value_text)
        )

        # DO NOT use safe_float().
        # safe_float() intentionally rounds to 2 decimals.
        # DSEX official precision must be preserved.

        try:

            candidate = float(
                value_text
                .replace(",", "")
                .strip()
            )

        except Exception:

            candidate = 0.0

        print(
            "DSEX CANDIDATE:",
            candidate
        )

        if (
            candidate >= 1000
            and
            candidate <= 20000
        ):

            dsex_rows.append(
                {
                    "row_number":
                        row_number,

                    "value":
                        candidate
                }
            )

            print(
                "DSEX VALUE ACCEPTED:",
                candidate
            )

    # =============================================================
    # 5G. DSEX VALIDATION
    # =============================================================

    print()
    print(
        "DSEX Rows Found:",
        len(dsex_rows)
    )

    print(
        "DSEX Rows:",
        dsex_rows
    )

    if len(dsex_rows) == 0:

        logging.error(
            "No DSE X Index values found"
        )

        print()
        print(
            "ERROR: No DSE X Index values found"
        )

        return False

    # =============================================================
    # 5H. SELECT LATEST DSEX CLOSE
    # =============================================================

    # FIRST DSEX row = latest trading day.
    #
    # SECOND DSEX row = preceding trade date.
    #
    # Example:
    #
    # FIRST:
    #   5655.68313
    #
    # SECOND:
    #   5640.08828
    #
    # Therefore:
    #
    # DSEX CLOSE = 5655.68313
    #
    # NOT 5640.08828.

    dse_close = dsex_rows[0]["value"]

    print()
    print(
        "DSEX Close Source:",
        "FIRST DSE X INDEX - LATEST TRADING DAY"
    )

    print(
        "DSEX Latest/Close:",
        f"{dse_close:.5f}"
    )

    # =============================================================
    # 5I. COLLECT TOTAL VOLUME BLOCKS
    # =============================================================

    volume_rows = []

    for row_number, row_div in enumerate(
        midrows,
        start=1
    ):

        row_text = row_div.get_text(
            " ",
            strip=True
        )

        row_text = " ".join(
            row_text.split()
        )

        normalized_upper = row_text.upper()

        if not (
            "TOTAL TRADE" in normalized_upper
            and
            "TOTAL VOLUME" in normalized_upper
            and
            "TOTAL VALUE" in normalized_upper
        ):

            continue

        print()
        print(
            "DSE TOTAL HEADER FOUND:",
            row_number
        )

        print(
            row_text
        )

        # The numeric data is the NEXT midrow.

        next_row_number = row_number + 1

        if (
            next_row_number < 1
            or
            next_row_number > len(midrows)
        ):

            continue

        data_div = midrows[
            next_row_number - 1
        ]

        data_text = data_div.get_text(
            " ",
            strip=True
        )

        data_text = " ".join(
            data_text.split()
        )

        print(
            "DSE TOTAL DATA:",
            data_text
        )

        numbers = re.findall(
            r"\d+(?:,\d{3})*(?:\.\d+)?",
            data_text
        )

        numeric_values = []

        for value_text in numbers:

            try:

                numeric_values.append(
                    float(
                        value_text
                        .replace(",", "")
                        .strip()
                    )
                )

            except Exception:

                continue

        print(
            "DSE TOTAL NUMBERS:",
            numeric_values
        )

        if len(numeric_values) >= 3:

            volume_rows.append(
                {
                    "row_number":
                        row_number,

                    "trade":
                        numeric_values[0],

                    "volume":
                        numeric_values[1],

                    "value_mn":
                        numeric_values[2]
                }
            )

    # =============================================================
    # 5J. VOLUME VALIDATION
    # =============================================================

    print()
    print(
        "DSE Volume Rows Found:",
        len(volume_rows)
    )

    print(
        "DSE Volume Rows:",
        volume_rows
    )

    if len(volume_rows) == 0:

        logging.error(
            "DSE total volume block not found"
        )

        print()
        print(
            "ERROR: DSE TOTAL VOLUME NOT FOUND"
        )

        return False

    # =============================================================
    # 5K. SELECT LATEST TOTAL VOLUME
    # =============================================================

    # FIRST total block = latest trading day.
    #
    # SECOND total block = preceding trade date.
    #
    # Example:
    #
    # FIRST:
    #   Trade   = 147461
    #   Volume  = 177465534
    #   Value   = 4670.148
    #
    # SECOND:
    #   Trade   = 154602
    #   Volume  = 191683061
    #   Value   = 5067.175
    #
    # Therefore the EOD DSEX row MUST use the FIRST block.

    latest_volume = volume_rows[0]

    dse_total_trade = (
        latest_volume["trade"]
    )

    dse_total_volume = (
        latest_volume["volume"]
    )

    dse_total_value_mn = (
        latest_volume["value_mn"]
    )

    print()
    print(
        "DSE Total Volume Source:",
        "FIRST TOTAL VOLUME BLOCK - LATEST TRADING DAY"
    )

    print(
        "DSE Final Total Trade:",
        int(dse_total_trade)
    )

    print(
        "DSE Final Total Volume:",
        int(dse_total_volume)
    )

    print(
        "DSE Final Total Value (MN):",
        dse_total_value_mn
    )

    # =============================================================
    # 5L. FINAL DSEX VALIDATION
    # =============================================================

    if dse_close <= 0:

        logging.error(
            "DSE DSEX close not found"
        )

        print()
        print(
            "ERROR: DSE DSEX CLOSE NOT FOUND"
        )

        return False

    if dse_total_volume <= 0:

        logging.error(
            "DSE total volume not found"
        )

        print()
        print(
            "ERROR: DSE TOTAL VOLUME NOT FOUND"
        )

        return False

    print()
    print(
        "DSE DSEX Close:",
        f"{dse_close:.5f}"
    )

    print(
        "DSE Total Volume:",
        int(dse_total_volume)
    )

    # =============================================================
    # 5M. DSEX DATA COMPLETE
    # =============================================================

    print()
    print(
        "DSEX data collected successfully"
    )

    # =============================================================
    # 5N. FINAL DSEX ROW
    # =============================================================

    dsex_row = {
        "Ticker":
            "DSEX",

        "Date":
            official_date,

        "Open":
            round(
                dsex_open,
                2
            ),

        "High":
            round(
                dsex_high,
                2
            ),

        "Low":
            round(
                dsex_low,
                2
            ),

        # Preserve official DSE precision.
        "Close":
            round(
                dse_close,
                5
            ),

        "Volume":
            int(
                dse_total_volume
            )
    }

    print()
    print(
        "Final DSEX Row:"
    )

    print(
        "Ticker :",
        dsex_row["Ticker"]
    )

    print(
        "Date   :",
        dsex_row["Date"]
    )

    print(
        "Open   :",
        dsex_row["Open"]
    )

    print(
        "High   :",
        dsex_row["High"]
    )

    print(
        "Low    :",
        dsex_row["Low"]
    )

    print(
        "Close  :",
        f"{dsex_row['Close']:.5f}"
    )

    print(
        "Volume :",
        dsex_row["Volume"]
    )

    # =============================================================
    # 5O. RETURN DSEX ROW
    # =============================================================

    return dsex_row



# =========================================================
# CLOUD HELPERS
# =========================================================


def normalize_date(value):
    return str(value).strip()


# =========================================================
# VALIDATE FINAL DATASET
# =========================================================

def validate_final_dataset(
    combined_df,
    dsex_row,
    trading_date
):
    """
    Final safety gate before publishing a daily archive.

    DSEX row must be a dictionary containing:

        Ticker
        Date
        Open
        High
        Low
        Close
        Volume
    """

    # =========================================================
    # 1. VALIDATE COMBINED DSE + SME DATA
    # =========================================================

    if (
        combined_df is None
        or
        combined_df.empty
    ):

        raise ValueError(
            "Combined DSE + SME data is empty"
        )


    # =========================================================
    # 2. VALIDATE DSEX ROW
    # =========================================================

    if (
        dsex_row is None
        or
        not isinstance(
            dsex_row,
            dict
        )
    ):

        raise ValueError(
            "DSEX row is missing or malformed"
        )


    # =========================================================
    # 3. EXPECTED DSEX KEYS
    # =========================================================

    expected_dsex_keys = [
        "Ticker",
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume"
    ]


    for key in expected_dsex_keys:

        if key not in dsex_row:

            raise ValueError(
                f"DSEX row missing key: {key}"
            )


    # =========================================================
    # 4. VALIDATE DSE + SME COLUMNS
    # =========================================================

    expected_columns = [
        "Ticker",
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume"
    ]


    if list(
        combined_df.columns
    ) != expected_columns:

        raise ValueError(
            "Unexpected columns: "
            f"{list(combined_df.columns)}"
        )


    # =========================================================
    # 5. DUPLICATE STOCK TICKERS
    # =========================================================

    if (
        combined_df["Ticker"]
        .duplicated()
        .any()
    ):

        raise ValueError(
            "Duplicate ticker found in DSE + SME data"
        )


    # =========================================================
    # 6. STOCK DATE VALIDATION
    # =========================================================

    if not (
        combined_df["Date"]
        .astype(str)
        ==
        trading_date
    ).all():

        raise ValueError(
            "DSE/SME date mismatch"
        )


    # =========================================================
    # 7. DSEX TICKER VALIDATION
    # =========================================================

    if (
        dsex_row["Ticker"]
        !=
        "DSEX"
    ):

        raise ValueError(
            "DSEX ticker is invalid"
        )


    # =========================================================
    # 8. DSEX DATE VALIDATION
    # =========================================================

    if (
        normalize_date(
            dsex_row["Date"]
        )
        !=
        trading_date
    ):

        raise ValueError(
            "DSEX date mismatch: "
            f"DSEX={dsex_row['Date']} "
            f"official={trading_date}"
        )


    # =========================================================
    # 9. DSEX OHLC VALIDATION
    # =========================================================

    if (
        float(
            dsex_row["Open"]
        )
        <=
        0
        or
        float(
            dsex_row["High"]
        )
        <=
        0
    ):

        raise ValueError(
            "Invalid DSEX Open/High"
        )


    if (
        float(
            dsex_row["Low"]
        )
        <=
        0
        or
        float(
            dsex_row["Close"]
        )
        <=
        0
    ):

        raise ValueError(
            "Invalid DSEX Low/Close"
        )


    # =========================================================
    # 10. DSEX VOLUME VALIDATION
    # =========================================================

    if (
        int(
            dsex_row["Volume"]
        )
        <=
        0
    ):

        raise ValueError(
            "Invalid DSEX Volume"
        )


    # =========================================================
    # 11. STOCK CLOSE VALIDATION
    # =========================================================

    if (
        combined_df["Close"]
        <=
        0
    ).any():

        raise ValueError(
            "Invalid stock Close <= 0"
        )


    # =========================================================
    # 12. STOCK VOLUME VALIDATION
    # =========================================================

    if (
        combined_df["Volume"]
        <
        0
    ).any():

        raise ValueError(
            "Invalid stock Volume < 0"
        )


    # =========================================================
    # 13. VALIDATION SUCCESS
    # =========================================================

    print()
    print(
        "FINAL DATASET VALIDATION: PASSED"
    )

    print(
        "DSEX Close:",
        dsex_row["Close"]
    )

    print(
        "DSEX Volume:",
        dsex_row["Volume"]
    )

    return True


# =========================================================
# WRITE DAILY ARCHIVE
# =========================================================

def write_daily_archive(
    combined_df,
    dsex_row,
    trading_date
):
    """
    Write the dated CSV and latest CSV only after
    the complete dataset passes validation.
    """

    # =========================================================
    # 1. VALIDATE BEFORE BUILDING ARCHIVE
    # =========================================================

    validate_final_dataset(
        combined_df,
        dsex_row,
        trading_date
    )


    # =========================================================
    # 2. BUILD FINAL DATAFRAME
    # =========================================================

    dsex_df = pd.DataFrame(
        [
            dsex_row
        ],
        columns=combined_df.columns
    )


    final_df = pd.concat(
        [
            combined_df,
            dsex_df
        ],
        ignore_index=True
    )


    # =========================================================
    # 3. FINAL DUPLICATE CHECK
    # =========================================================

    if (
        final_df["Ticker"]
        .duplicated()
        .any()
    ):

        raise ValueError(
            "DSEX or stock ticker duplicate detected"
        )


    # =========================================================
    # 4. FINAL ROW COUNT CHECK
    # =========================================================

    if len(final_df) != (
        len(combined_df) + 1
    ):

        raise ValueError(
            "Final dataset row count mismatch"
        )


    # =========================================================
    # 5. PREPARE DIRECTORIES
    # =========================================================

    year = trading_date[:4]

    month = trading_date[5:7]

    month_dir = (
        DATA_DIR
        /
        year
        /
        month
    )

    month_dir.mkdir(
        parents=True,
        exist_ok=True
    )


    # =========================================================
    # 6. OUTPUT FILES
    # =========================================================

    daily_file = (
        month_dir
        /
        f"DSE_EOD_{trading_date}.csv"
    )

    latest_file = (
        LATEST_DIR
        /
        "DSE_EOD_latest.csv"
    )


    # =========================================================
    # 7. TEMPORARY FILES
    # =========================================================

    temp_daily = (
        daily_file.with_suffix(
            ".csv.tmp"
        )
    )

    temp_latest = (
        latest_file.with_suffix(
            ".csv.tmp"
        )
    )


    # =========================================================
    # 8. WRITE DAILY ARCHIVE
    # =========================================================

    # Atomic replacement:
    # never leave a half-written CSV.

    final_df.to_csv(
        temp_daily,
        index=False,
        header=False
    )

    temp_daily.replace(
        daily_file
    )


    # =========================================================
    # 9. WRITE LATEST ARCHIVE
    # =========================================================

    final_df.to_csv(
        temp_latest,
        index=False,
        header=False
    )

    temp_latest.replace(
        latest_file
    )


    # =========================================================
    # 10. SUCCESS MESSAGE
    # =========================================================

    print()
    print(
        "=" * 70
    )

    print(
        "CLOUD ARCHIVE PUBLISHED"
    )

    print(
        "=" * 70
    )

    print(
        "Daily CSV:"
    )

    print(
        daily_file
    )

    print(
        "Latest CSV:"
    )

    print(
        latest_file
    )

    print(
        "Rows:",
        len(final_df)
    )


    print()
    print(
        "Published DSEX:"
    )

    print(
        "Date   :",
        dsex_row["Date"]
    )

    print(
        "Close  :",
        dsex_row["Close"]
    )

    print(
        "Volume :",
        dsex_row["Volume"]
    )


    logging.info(
        f"Published {daily_file} | "
        f"rows={len(final_df)} | "
        f"date={trading_date}"
    )


    return daily_file


# =========================================================
# RUN ONCE
# =========================================================

def run_once():
    """
    One complete DSE + SME + DSEX collection attempt.
    """

    print()
    print(
        "=" * 70
    )

    print(
        "DSE + SME + DSEX CLOUD DATA FEEDER"
    )

    print(
        "=" * 70
    )


    # =========================================================
    # 1. DOWNLOAD PERMISSION
    # =========================================================

    if not dse_download_allowed():

        print()

        print(
            "FEEDER STOPPED SAFELY."
        )

        return False


    # =========================================================
    # 2. OFFICIAL TRADING DATE
    # =========================================================

    trading_date = (
        get_dse_trading_date()
    )


    if trading_date is False:

        raise RuntimeError(
            "Could not determine official DSE trading date"
        )


    # =========================================================
    # 3. MAIN DSE DATA
    # =========================================================

    dse_df = (
        scrape_dse_latest_prices(
            trading_date
        )
    )


    if dse_df is False:

        raise RuntimeError(
            "Main DSE scraping failed"
        )


    # =========================================================
    # 4. SME DATA
    # =========================================================

    sme_df = (
        scrape_sme_latest_prices(
            trading_date
        )
    )


    if sme_df is False:

        raise RuntimeError(
            "SME scraping failed"
        )


    # =========================================================
    # 5. COMBINE DSE + SME
    # =========================================================

    combined_df = (
        combine_dse_sme(
            dse_df,
            sme_df
        )
    )


    if combined_df is False:

        raise RuntimeError(
            "DSE + SME combination failed"
        )


    # =========================================================
    # 6. DSEX
    # =========================================================

    # DSEX is collected before ANY archive
    # is published.

    dsex_row = (
        append_dsex_index()
    )


    if dsex_row is False:

        raise RuntimeError(
            "DSEX collection failed"
        )


    # =========================================================
    # 7. WRITE ARCHIVE
    # =========================================================

    daily_file = (
        write_daily_archive(
            combined_df,
            dsex_row,
            trading_date
        )
    )


    # =========================================================
    # 8. COMPLETE
    # =========================================================

    print()
    print(
        "=" * 70
    )

    print(
        "DSE + SME + DSEX COMPLETE"
    )

    print(
        "=" * 70
    )

    print(
        "Official Trading Date:",
        trading_date
    )

    print(
        "Daily CSV:",
        daily_file
    )

    print(
        "DONE"
    )


    return True


# =========================================================
# MAIN
# =========================================================

def main():

    for attempt in range(
        1,
        MAX_ATTEMPTS + 1
    ):

        print()

        print(
            f"========== "
            f"CLOUD ATTEMPT "
            f"{attempt}/"
            f"{MAX_ATTEMPTS}"
            f" =========="
        )


        try:

            if run_once():

                return 0


        except Exception as e:

            logging.exception(
                f"Attempt {attempt} failed"
            )

            print()

            print(
                "ATTEMPT FAILED:",
                e
            )


        if (
            attempt
            <
            MAX_ATTEMPTS
        ):

            print()

            print(
                f"Retrying in "
                f"{RETRY_DELAY_SECONDS // 60} "
                f"minutes..."
            )

            time.sleep(
                RETRY_DELAY_SECONDS
            )


    print()

    print(
        "=" * 70
    )

    print(
        "CLOUD FEEDER FAILED"
    )

    print(
        "=" * 70
    )

    print(
        "No new daily archive was published."
    )


    return 1


# =========================================================
# PROGRAM ENTRY POINT
# =========================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )

