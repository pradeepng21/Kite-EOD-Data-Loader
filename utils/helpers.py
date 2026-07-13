from config.config import logger
from sqlalchemy import text

def create_table(conn):
    """
    Create the index_data table if it does not exist.
    """
    try:
        cursor = conn.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS spot (
                date DATE,
                time TIME,
                symbol TEXT,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                PRIMARY KEY (symbol, date, time)
            );
        """
        create_index_query = """
            CREATE INDEX IF NOT EXISTS datetime
            ON spot (date, time);
        """

        cursor.execute(create_table_query)
        cursor.execute(create_index_query) 

        conn.commit()
        cursor.close()

        logger.info("Table spot ensured to exist.")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")

def create_table_eq(conn):
    """
    Create the index_data table if it does not exist.
    """
    try:
        cursor = conn.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS equity (
                date DATE,
                time TIME,
                symbol TEXT,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                PRIMARY KEY (symbol, date, time)
            );
        """
        create_index_query = """
            CREATE INDEX IF NOT EXISTS datetime
            ON equity (date, time);
        """

        cursor.execute(create_table_query)
        cursor.execute(create_index_query) 

        conn.commit()
        cursor.close()

        logger.info("Table equity ensured to exist.")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")

def create_fut_n_opt_table(conn):
    """
    Create the fut_n_opt table with the specified columns and indexes.
    """
    try:
        cursor = conn.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS fut_n_opt (
                date DATE,
                time TIME,
                symbol TEXT,
                strike FLOAT,
                otype TEXT,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                open_interest FLOAT,
                volume FLOAT,
                expiry_date DATE,
                week_expiry INT,
                month_expiry INT,
                segment INT,
                PRIMARY KEY (date, time, symbol, strike, otype)
            );
        """

        create_index_query = """
            CREATE INDEX IF NOT EXISTS idx_fut_n_opt_datetime
            ON fut_n_opt (date, time);
        """

        cursor.execute(create_table_query)
        cursor.execute(create_index_query)
        conn.commit()

        logger.info("Table fut_n_opt created successfully with index on (date, time).")
    except Exception as e:
        logger.error(f"Failed to fut_n_opt create table: {e}")


def fetch_kite_token():
    """
    Fetch all records from the TokenCentral table where the username matches 'KITE'.

    Returns:
        list: A list of dictionaries containing sessionId and gcid of the fetched records.
    """
    try:
        query = text(f"""
            SELECT access_token, api_token 
            FROM {KiteDB.TABLE} 
        """)

        with KiteDB.DBENGINE.connect() as conn:
            result = conn.execute(query).fetchone()
            if result:
                access_token, api_token = result
                logger.info(f"Access Token: {access_token}")
                logger.info(f"API Token: {api_token}")
                return api_token, access_token
            else:
                logger.error("No record found")
                return None, None

    except Exception as e:
        logger.error(f"An error occurred during database operation {e}")
        return None, None
