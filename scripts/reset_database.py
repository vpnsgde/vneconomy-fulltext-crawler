import sqlite3
import os
import logging

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../database/vneconomy_news.db')
LOG_PATH = os.path.join(BASE_DIR, '../logs/reset_database_log.txt')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8'),
        logging.StreamHandler()  # log to terminal
    ]
)

logging.info("Starting database reset process...")

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
logging.info(f"Connected to database at {DB_PATH}")

# --- Define schema ---
TABLES = {
    "links": """
        CREATE TABLE IF NOT EXISTS links (
            idx INTEGER PRIMARY KEY AUTOINCREMENT,
            category_index INTEGER,
            paper_link TEXT
        )
    """,
    "contents": """
        CREATE TABLE IF NOT EXISTS contents (
            idx INTEGER PRIMARY KEY AUTOINCREMENT,
            category_index INTEGER,
            publish_date TEXT,
            title TEXT,
            text TEXT
        )
    """
}

# --- Init or Reset Tables ---
for table_name, create_sql in TABLES.items():
    # Create table if not exists
    cursor.execute(create_sql)
    conn.commit()
    logging.info(f"Ensured table '{table_name}' exists")

    # Clear data
    cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()
    logging.info(f"Table '{table_name}' cleared")

    # Reset AUTOINCREMENT counter
    cursor.execute("DELETE FROM sqlite_sequence WHERE name=?", (table_name,))
    conn.commit()
    logging.info(f"AUTOINCREMENT reset for table '{table_name}'")

# Done
conn.close()
logging.info("All tables initialized and reset successfully")
