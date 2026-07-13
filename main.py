"""
Kite Daily Data Loader
=======================

Downloads one trading day's minute-level OHLC(V) history from the Zerodha
Kite Connect REST API and persists it into PostgreSQL, covering:

  * Index spot prices        -> `spot` table
  * Equity cash prices       -> `equity` table
  * Options (near 2 expiries)-> `fut_n_opt` table (segment=2)
  * Futures (near 2 expiries)-> `fut_n_opt` table (segment=3)

The option strike range for each index is derived from that day's observed
spot high/low, rounded to the index's usual strike step (see
`KiteDataLoader.STRIKE_ROUNDING`).

Run directly (`python main.py`) to execute the full pipeline for today.
"""

import io
from datetime import date, datetime, time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
import requests
from psycopg2.extensions import connection as PGConnection

from config.config import db_config, equity, index, inst_url, logger, mapper
from utils.helpers import create_fut_n_opt_table, create_table, create_table_eq, fetch_kite_token


class KiteDataLoader:
    """Fetches a single trading day's OHLC(V) data from Kite Connect and loads it into PostgreSQL."""

    KITE_HIST_URL_TEMPLATE = "https://api.kite.trade/instruments/historical/{token}/minute"
    MARKET_OPEN = "09:15:00"
    MARKET_CLOSE = "15:29:00"

    # Strike-price rounding step used per index when generating the option strike range.
    STRIKE_ROUNDING = {
        "SENSEX": 100,
        "NIFTY BANK": 100,
        "BANKEX": 100,
        "NIFTY 50": 50,
        "NIFTY FIN SERVICE": 50,
        "NIFTY MID SELECT": 25,
    }
    DEFAULT_STRIKE_ROUNDING = 1
    STRIKE_RANGE_PADDING_STEPS = 5  # how many rounding-steps to pad above/below the day's max/min

    def __init__(self, trade_date: Optional[str] = None):
        """
        Args:
            trade_date: Date to fetch, as 'YYYY-MM-DD'. Defaults to today.
        """
        self.trade_date = trade_date or datetime.now().strftime("%Y-%m-%d")

        api_key, access_token = fetch_kite_token()
        if not api_key or not access_token:
            raise RuntimeError("Could not obtain Kite API credentials from the token store.")

        self.headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {api_key}:{access_token}",
        }

    # ------------------------------------------------------------------ #
    # Database connections
    # ------------------------------------------------------------------ #
    @staticmethod
    def connect(dbname: str) -> PGConnection:
        """Open a new PostgreSQL connection to the given database."""
        return psycopg2.connect(
            dbname=dbname,
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"],
        )

    # ------------------------------------------------------------------ #
    # Kite API access
    # ------------------------------------------------------------------ #
    def get_historical_data(self, instrument_token: int, with_oi: bool = False) -> List[list]:
        """Fetch one trading day of minute candles for a single instrument token."""
        url = (
            f"{self.KITE_HIST_URL_TEMPLATE.format(token=instrument_token)}"
            f"?from={self.trade_date}+{self.MARKET_OPEN}&to={self.trade_date}+{self.MARKET_CLOSE}"
        )
        if with_oi:
            url += "&oi=1"

        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"Failed to fetch historical data for {instrument_token}. Status: {response.status_code}")
            return []
        return response.json().get("data", {}).get("candles", [])

    def fetch_instruments(self) -> pd.DataFrame:
        """Download the full Kite instruments master list as a DataFrame."""
        response = requests.get(inst_url, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"Failed to fetch instruments. Status code: {response.status_code}")
            return pd.DataFrame()

        data = response.content.decode("utf-8")
        return pd.read_csv(io.StringIO(data))

    @staticmethod
    def _split_timestamp(raw_timestamp: str) -> Tuple[date, time]:
        """Split a Kite candle's ISO-8601 timestamp string into (date, time) parts."""
        dt_object = datetime.strptime(raw_timestamp[:19], "%Y-%m-%dT%H:%M:%S")
        return dt_object.date(), dt_object.time()

    # ------------------------------------------------------------------ #
    # Spot (index) data
    # ------------------------------------------------------------------ #
    def store_index_data(self, conn: PGConnection, index_df: pd.DataFrame) -> None:
        """Fetch and store minute OHLC data for index spot prices."""
        insert_query = """
            INSERT INTO spot (date, time, symbol, open, high, low, close)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, time, symbol) DO NOTHING;
        """
        try:
            with conn.cursor() as cursor:
                for _, row in index_df.iterrows():
                    symbol = row["tradingsymbol"]
                    for candle in self.get_historical_data(row["instrument_token"]):
                        raw_timestamp, o, h, l, c, _volume = candle
                        date_part, time_part = self._split_timestamp(raw_timestamp)
                        cursor.execute(insert_query, (date_part, time_part, symbol, o, h, l, c))
            conn.commit()
            logger.info("Index data successfully stored in the database.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed (index): {e}")
            raise

    # ------------------------------------------------------------------ #
    # Equity data
    # ------------------------------------------------------------------ #
    def store_equity_data(self, conn: PGConnection, equity_df: pd.DataFrame) -> None:
        """Fetch and store minute OHLCV data for equity cash prices."""
        insert_query = """
            INSERT INTO equity (date, time, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, time, symbol) DO NOTHING;
        """
        try:
            with conn.cursor() as cursor:
                for _, row in equity_df.iterrows():
                    symbol = row["tradingsymbol"]
                    for candle in self.get_historical_data(row["instrument_token"]):
                        raw_timestamp, o, h, l, c, volume = candle
                        date_part, time_part = self._split_timestamp(raw_timestamp)
                        cursor.execute(insert_query, (date_part, time_part, symbol, o, h, l, c, volume))
            conn.commit()
            logger.info("Equity data successfully stored in the database.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed (equity): {e}")
            raise

    # ------------------------------------------------------------------ #
    # Strike range derivation (used to scope down the option chain)
    # ------------------------------------------------------------------ #
    def fetch_max_min_values(self, conn: PGConnection) -> Dict[str, dict]:
        """Return each index's intraday max(high)/min(low) for `self.trade_date`, keyed by symbol."""
        query = """
            SELECT symbol, date, MAX(high) AS max_high, MIN(low) AS min_low
            FROM spot
            WHERE date = %s
            GROUP BY symbol, date
            ORDER BY symbol, date;
        """
        with conn.cursor() as cursor:
            cursor.execute(query, (self.trade_date,))
            results = cursor.fetchall()

        return {
            row[0]: {"date": row[1], "max_high": row[2], "min_low": row[3]}
            for row in results
        }

    def generate_strike_lists(self, index_data: Dict[str, dict]) -> Dict[str, List[int]]:
        """Build a padded, rounded strike-price range for each index based on its day's spot range."""
        strike_data = {}

        for trading_symbol, values in index_data.items():
            round_val = self.STRIKE_ROUNDING.get(trading_symbol, self.DEFAULT_STRIKE_ROUNDING)

            max_val = int(round(values["max_high"] / round_val) * round_val)
            min_val = int(round(values["min_low"] / round_val) * round_val)
            padding = self.STRIKE_RANGE_PADDING_STEPS * round_val

            strike_data[trading_symbol] = list(range(min_val - padding, max_val + padding + round_val, round_val))

        return strike_data

    # ------------------------------------------------------------------ #
    # Options data
    # ------------------------------------------------------------------ #
    def _insert_option_rows(self, cursor, insert_query: str, contracts_df: pd.DataFrame, week_expiry: int) -> None:
        """Fetch and insert historical rows for one expiry's worth of option contracts."""
        for _, row in contracts_df.iterrows():
            symbol = row["name"]
            strike = int(row["strike"])
            otype = row["instrument_type"]
            expiry = row["expiry"]

            for candle in self.get_historical_data(row["instrument_token"], with_oi=True):
                raw_timestamp, o, h, l, c, volume, oi = candle
                date_part, time_part = self._split_timestamp(raw_timestamp)
                cursor.execute(
                    insert_query,
                    (date_part, time_part, symbol, strike, otype, o, h, l, c, oi, volume, expiry, week_expiry, 0, 2),
                )

    def filter_strikes_and_separate_expiry(
        self, conn: PGConnection, strike_data: Dict[str, List[int]], inst_df: pd.DataFrame
    ) -> None:
        """Filter option instruments to each index's strike range and load the first two expiries."""
        insert_query = """
            INSERT INTO fut_n_opt (
                date, time, symbol, strike, otype, open, high, low, close,
                open_interest, volume, expiry_date, week_expiry, month_expiry, segment
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, time, symbol, strike, otype) DO NOTHING;
        """
        try:
            with conn.cursor() as cursor:
                for trading_symbol, strike_list in strike_data.items():
                    filtered_df = inst_df[
                        (inst_df["name"] == mapper[trading_symbol]) & (inst_df["strike"].isin(strike_list))
                    ]
                    if filtered_df.empty:
                        continue  # No instruments matched this index's strike range.

                    expiry_dates = sorted(filtered_df["expiry"].unique())[:2]
                    if len(expiry_dates) < 2:
                        continue  # Need at least a near and next expiry.

                    for week_expiry, expiry_date in enumerate(expiry_dates, start=1):
                        expiry_df = filtered_df[filtered_df["expiry"] == expiry_date]
                        self._insert_option_rows(cursor, insert_query, expiry_df, week_expiry)

            conn.commit()
            logger.info("Options data successfully stored in the database.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed (options): {e}")
            raise

    # ------------------------------------------------------------------ #
    # Futures data
    # ------------------------------------------------------------------ #
    def store_fut_data(self, conn: PGConnection, fut_list: List[str], inst_df: pd.DataFrame) -> None:
        """Fetch and store historical data for the first two futures expiries of each index."""
        insert_query = """
            INSERT INTO fut_n_opt (
                date, time, symbol, strike, otype, open, high, low, close,
                open_interest, volume, expiry_date, week_expiry, month_expiry, segment
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, time, symbol, strike, otype) DO NOTHING;
        """
        try:
            with conn.cursor() as cursor:
                for fut in fut_list:
                    symbol = mapper[fut]
                    segment = "BFO-FUT" if symbol in ("BANKEX", "SENSEX") else "NFO-FUT"

                    filtered_df = inst_df[(inst_df["name"] == symbol) & (inst_df["segment"] == segment)]
                    expiry_dates = sorted(filtered_df["expiry"].unique())[:2]
                    filt_df = filtered_df[filtered_df["expiry"].isin(expiry_dates)]

                    for month_expiry, (_, row) in enumerate(filt_df.iterrows(), start=1):
                        contract_symbol = row["name"]
                        strike = int(row["strike"])
                        otype = row["instrument_type"]
                        expiry = row["expiry"]

                        for candle in self.get_historical_data(row["instrument_token"], with_oi=True):
                            raw_timestamp, o, h, l, c, volume, oi = candle
                            date_part, time_part = self._split_timestamp(raw_timestamp)
                            cursor.execute(
                                insert_query,
                                (
                                    date_part, time_part, contract_symbol, strike, otype, o, h, l, c,
                                    oi, volume, expiry, 0, month_expiry, 3,
                                ),
                            )
            conn.commit()
            logger.info("Futures data successfully stored in the database.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed (futures): {e}")
            raise

    # ------------------------------------------------------------------ #
    # Pipeline entry point
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        """Execute the full daily pipeline: ensure tables exist, then load spot, equity and F&O data."""
        spot_conn = self.connect("spot")
        equity_conn = self.connect("equity")
        fno_conn = self.connect("fut_n_opt")

        try:
            create_table(spot_conn)
            create_table_eq(equity_conn)
            create_fut_n_opt_table(fno_conn)

            inst_df = self.fetch_instruments()
            inst_df["expiry"] = pd.to_datetime(inst_df["expiry"])

            index_df = inst_df[inst_df["tradingsymbol"].isin(index)]
            equity_df = inst_df[inst_df["tradingsymbol"].isin(equity)]

            self.store_index_data(spot_conn, index_df)
            self.store_equity_data(equity_conn, equity_df)

            spot_max_min = self.fetch_max_min_values(spot_conn)
            strike_lists = self.generate_strike_lists(spot_max_min)

            self.filter_strikes_and_separate_expiry(fno_conn, strike_lists, inst_df)
            self.store_fut_data(fno_conn, index, inst_df)
        finally:
            spot_conn.close()
            equity_conn.close()
            fno_conn.close()


if __name__ == "__main__":
    KiteDataLoader().run()
