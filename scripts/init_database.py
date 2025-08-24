import sqlite3
import os
import csv

# --- Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, '../database')
DB_PATH = os.path.join(DB_DIR, 'vneconomy_news.db')
CATEGORIES_CSV = os.path.join(BASE_DIR, '../tmp/categories.csv')

# --- Ensure database folder exists ---
os.makedirs(DB_DIR, exist_ok=True)

# --- Connect to SQLite ---
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# --- Table schemas ---
TABLES = {
    "categories": """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY,
            category_link TEXT
        )
    """,
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
            text BLOB
        )
    """
}

# --- Create tables if not exist ---
for table_name, create_sql in TABLES.items():
    cursor.execute(create_sql)
    conn.commit()
    print(f"[INFO] Table '{table_name}' ensured to exist.")

# --- Import categories.csv ---
if os.path.exists(CATEGORIES_CSV):
    with open(CATEGORIES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_inserted = 0
        for row in reader:
            try:
                category_id = int(row['index'])
                category_link = row['category_link'].strip()
                if category_link:
                    cursor.execute("""
                        INSERT OR IGNORE INTO categories (id, category_link)
                        VALUES (?, ?)
                    """, (category_id, category_link))
                    rows_inserted += cursor.rowcount
            except Exception as e:
                print(f"[ERROR] Inserting row {row}: {e}")
    conn.commit()
    print(f"[INFO] Imported {rows_inserted} rows from '{CATEGORIES_CSV}' into 'categories' table.")
else:
    print(f"[WARNING] Categories CSV '{CATEGORIES_CSV}' not found, skipping import.")

# --- Close connection ---
conn.close()
print(f"[INFO] Database initialized at '{DB_PATH}' successfully.")
