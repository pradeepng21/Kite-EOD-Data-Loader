import os
import sys
import datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pytz
from loguru import logger


IST = pytz.timezone("Asia/Calcutta")

inst_url = "https://api.kite.trade/instruments"
hist_url = "https://api.kite.trade/instruments/historical"

db_config = {
    "user":"postgres",
    "password":"12345678",
    "host":"localhost",
    "port":"5432"
}


class KiteDB:
    USERNAME: str = "root"
    PASSWORD: str = quote_plus(r"ng@12345678")
    HOST: str = "localhost"
    PORT: str = 3306
    NAME: str = "token_db"
    TABLE: str = "kite_token"
    DBENGINE: str = create_engine(
        f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}"
        f"@{HOST}:{PORT}/{NAME}", echo=False, isolation_level="READ COMMITTED", pool_size=50, max_overflow=-1
    )

def format_time(record):
    # Convert the log record's time to the desired timezone
    local_time = record["time"].astimezone(IST)
    record["time"] = local_time

main_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

folder_name = main_directory + "/log"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

current_day = datetime.datetime.now(IST).strftime("%Y-%m-%d")

logger.remove()
logger.add(sys.stdout, level="DEBUG")

logger.add(
    os.path.join(main_directory, "log", f"KITE_{current_day}.log"),
    rotation="1 day",
    compression="zip",
    enqueue=True,
    level="INFO",
    backtrace=True,
)

logger.configure(patcher=format_time)

logger.debug(f" {'-'*10}{'>'*10}{' '*10}.....script started.....{' '*10}{'<'*10}{'-'*10} ")


equity = [
    "ABB", "ACC", "APLAPOLLO", "AUBANK", "ADANIENSOL", "ADANIENT", "ADANIGREEN",
    "ADANIPORTS", "ADANIPOWER", "ATGL", "ABCAPITAL", "ABFRL", "ALKEM", "AMBUJACEM",
    "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "AUROPHARMA", "DMART",
    "AXISBANK", "BSE", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BAJAJHLDNG", "BALKRISIND",
    "BANDHANBNK", "BANKBARODA", "BANKINDIA", "MAHABANK", "BDL", "BEL", "BHARATFORG", "BHEL",
    "BPCL", "BHARTIARTL", "BHARTIHEXA", "BIOCON", "BOSCHLTD", "BRITANNIA", "CGPOWER", "CANBK",
    "CHOLAFIN", "CIPLA", "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL", "CONCOR", "CUMMINSIND",
    "DLF", "DABUR", "DELHIVERY", "DIVISLAB", "DIXON", "DRREDDY", "EICHERMOT", "ESCORTS",
    "EXIDEIND", "NYKAA", "FEDERALBNK", "FACT", "GAIL", "GMRAIRPORT", "GODREJCP", "GODREJPROP",
    "GRASIM", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HAVELLS", "HEROMOTOCO", "HINDALCO",
    "HAL", "HINDPETRO", "HINDUNILVR", "HINDZINC", "HUDCO", "ICICIBANK", "ICICIGI", "ICICIPRULI",
    "IDBI", "IDFCFIRSTB", "IRB", "ITC", "INDIANB", "INDHOTEL", "IOC", "IOB", "IRCTC", "IRFC",
    "IREDA", "IGL", "INDUSTOWER", "INDUSINDBK", "NAUKRI", "INFY", "INDIGO", "JSWENERGY",
    "JSWINFRA", "JSWSTEEL", "JINDALSTEL", "JIOFIN", "JUBLFOOD", "KPITTECH", "KALYANKJIL",
    "KOTAKBANK", "LTF", "LICHSGFIN", "LTIM", "LT", "LICI", "LUPIN", "MRF", "LODHA", "M&MFIN",
    "M&M", "MRPL", "MANKIND", "MARICO", "MARUTI", "MFSL", "MAXHEALTH", "MAZDOCK", "MPHASIS",
    "MUTHOOTFIN", "NHPC", "NLCINDIA", "NMDC", "NTPC", "NESTLEIND", "OBEROIRLTY", "ONGC", "OIL",
    "PAYTM", "OFSS", "POLICYBZR", "PIIND", "PAGEIND", "PATANJALI", "PERSISTENT", "PETRONET",
    "PHOENIXLTD", "PIDILITIND", "POLYCAB", "POONAWALLA", "PFC", "POWERGRID", "PRESTIGE", "PNB",
    "RECLTD", "RVNL", "RELIANCE", "SBICARD", "SBILIFE", "SJVN", "SRF", "MOTHERSON", "SHREECEM",
    "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SBIN", "SAIL", "SUNPHARMA", "SUNDARMFIN",
    "SUPREMEIND", "SUZLON", "TVSMOTOR", "TATACHEM", "TATACOMM", "TCS", "TATACONSUM",
    "TATAELXSI", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TATATECH", "TECHM", "TITAN",
    "TORNTPHARM", "TORNTPOWER", "TRENT", "TIINDIA", "UPL", "ULTRACEMCO", "UNIONBANK",
    "UNITDSPR", "VBL", "VEDL", "IDEA", "VOLTAS", "WIPRO", "YESBANK", "ZOMATO", "ZYDUSLIFE",
    "NIFTYBEES", "SETFNIF50", "JUNIORBEES", "BANKBEES"
]

index = ["NIFTY 50", "NIFTY BANK", "SENSEX", "BANKEX", "NIFTY FIN SERVICE", "NIFTY MID SELECT"]

mapper = {
    "SENSEX": "SENSEX",
    "NIFTY 50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "NIFTY FIN SERVICE": "FINNIFTY",
    "NIFTY MID SELECT": "MIDCPNIFTY",
    "BANKEX": "BANKEX"
}