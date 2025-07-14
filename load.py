import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# Load DB creds
DB_HOST = os.getenv("PG_HOST")
DB_PORT = os.getenv("PG_PORT")
DB_NAME = os.getenv("PG_DATABASE")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")

# Read data
df = pd.read_csv("./stock_data.csv")


df.rename(columns={
    'datetime': 'timestamp',
    'pct_changes': 'pct_change'
}, inplace=True)
df['symbol'] = "AAPL"
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['transformed_at'] = pd.to_datetime(df['transformed_at'])
df = df.where(pd.notnull(df), None)

columns = [
    "symbol", "timestamp", "open", "high", "low", "close", "volume",
    "pct_change", "volatility_zone", "price_range", "close_open_diff",
    "volume_change", "trend", "transformed_at"
]

# Clean BIGINT columns
for col in ['volume', 'volume_change']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # convert to float or NaN
        df[col] = df[col].fillna(0).astype(int)  # fill NaN with 0 and cast to int


values = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

INSERT_QUERY = sql.SQL("""
    INSERT INTO stock_data ({fields})
    VALUES %s
    ON CONFLICT (symbol, timestamp) DO NOTHING
""").format(fields=sql.SQL(', ').join(map(sql.Identifier, columns)))

# Connect and insert
conn = None
cursor = None

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    print(f"✅ CONNECTED TO {DB_NAME}")
    cursor = conn.cursor()
    execute_values(cursor, INSERT_QUERY.as_string(conn), values)
    conn.commit()
    print(f"✅ Inserted {len(values)} rows into stock_data")

except Exception as e:
    print(f"❌ ERROR: {e}")

finally:
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()
