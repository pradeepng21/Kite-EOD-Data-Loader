# Kite Daily Data Loader (Zerodha)

A daily batch job that pulls minute-level OHLC(V) market data from the
[Zerodha Kite Connect](https://kite.trade/) API and loads it into PostgreSQL —
covering index spot prices, equity cash prices, and the near-two-expiry
options and futures chain for each index.

## What it does

Running the loader for a trading day performs the following pipeline:

1. **Ensure tables exist** — creates the `spot`, `equity`, and `fut_n_opt`
   tables (with supporting indexes) if they don't already exist.
2. **Fetch the instrument master** — downloads Kite's full instruments list
   and filters it down to the configured index and equity symbols.
3. **Load spot & equity data** — fetches minute candles for each index and
   equity symbol and upserts them into `spot` / `equity`.
4. **Derive strike ranges** — computes each index's intraday high/low from
   the data just loaded and builds a padded, rounded strike-price range
   around it (e.g. 50-point steps for NIFTY, 100-point steps for BANKNIFTY).
5. **Load options data** — filters the instrument master to contracts within
   that strike range, keeps the nearest two expiries, and loads their minute
   candles (with open interest) into `fut_n_opt`.
6. **Load futures data** — same as above, for the nearest two futures
   expiries of each index.

All inserts are idempotent (`ON CONFLICT ... DO NOTHING`), so the script can
safely be re-run for the same day without creating duplicate rows.

## Project structure

```
.
├── main.py              # KiteDataLoader — the pipeline described above
├── config/
│   └── config.py        # DB credentials, Kite URLs, logging setup, symbol lists
├── utils/
│   └── helpers.py        # Table DDL helpers + Kite token lookup
└── log/                  # Rotating daily log files (created at runtime)
```

## Requirements

- Python 3.9+
- A running PostgreSQL instance
- A MySQL instance/table holding a valid Kite Connect `access_token` /
  `api_token` pair (see `utils.helpers.fetch_kite_token`)
- A Kite Connect API subscription

Install dependencies:

```bash
pip install requests psycopg2-binary pandas sqlalchemy mysql-connector-python pytz loguru
```

## Configuration

Connection details, the tradable universe, and index→segment mapping live in
[`config/config.py`](config/config.py):

- `db_config` — PostgreSQL connection parameters
- `KiteDB` — MySQL connection used to look up the current Kite access token
- `index`, `equity`, `mapper` — the symbols tracked and their strike-rounding
  buckets

> **Security note:** `config/config.py` currently contains hardcoded database
> passwords. Before pushing this repository (or any fork of it) to a public
> GitHub remote, move these credentials into environment variables or a
> `.env` file (already covered by `.gitignore`) and load them with something
> like `os.environ` or `python-dotenv` instead of committing them in plaintext.

## Usage

```bash
python main.py
```

This runs the full pipeline for today's date. To load a specific past date
(subject to Kite's historical data window limits), instantiate the loader
directly:

```python
from main import KiteDataLoader

KiteDataLoader(trade_date="2025-03-05").run()
```

## Database schema

| Table       | Purpose                                   | Primary key                              |
|-------------|--------------------------------------------|-------------------------------------------|
| `spot`      | Index minute OHLC                          | `(symbol, date, time)`                    |
| `equity`    | Equity minute OHLCV                        | `(symbol, date, time)`                    |
| `fut_n_opt` | Futures & options minute OHLCV + OI        | `(date, time, symbol, strike, otype)`     |

`fut_n_opt.segment` distinguishes contract type: `2` = options, `3` = futures.
`week_expiry` / `month_expiry` indicate whether a row belongs to the 1st or
2nd nearest options/futures expiry respectively.

## Logging

Logs are written via [loguru](https://github.com/Delgan/loguru) to both
stdout and `log/KITE_<date>.log`, rotated daily and compressed.
