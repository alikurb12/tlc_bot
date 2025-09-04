import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

conn = None
cursor = None

def init_db():
    global conn, cursor
    try:
        conn=psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            cursor_factory=RealDictCursor
        )
        cursor = conn.cursor()
        logger.info("DataBase connected")

        cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    exchange VARCHAR(20) NOT NULL,
                    order_id VARCHAR(255) NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    position_side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_loss REAL,
                    take_profit_1 REAL,
                    take_profit_2 REAL,
                    take_profit_3 REAL,
                    sl_order_id VARCHAR(255),
                    tp1_order_id VARCHAR(255),
                    tp2_order_id VARCHAR(255),
                    tp3_order_id VARCHAR(255),
                    status TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
                )
                """)
        conn.commit()
    except Exception as e:
        logger.error(f"DataBase connection failed: {e}")
        raise

def get_cursor():
    return cursor

def commit():
    conn.commit()

def close_db():
    if cursor:
        cursor.close()
    if conn:
        conn.close()