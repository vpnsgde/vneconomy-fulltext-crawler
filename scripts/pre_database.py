import sqlite3
import csv
import os
import logging

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../database/vneconomy_news.db')
CSV_PATH = os.path.join(BASE_DIR, '../tmp/categories.csv')
TABLES_INFO_PATH = os.path.join(BASE_DIR, '../tmp/tables_info.txt')
LOG_PATH = os.path.join(BASE_DIR, '../logs/pre_database_log.txt')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def export_categories_to_csv(conn):
    """Export categories table to CSV file"""
    cursor = conn.cursor()
    cursor.execute('SELECT id, category_link FROM categories ORDER BY id')
    rows = cursor.fetchall()

    with open(CSV_PATH, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['index', 'category_link'])
        for row in rows:
            writer.writerow(row)

    logging.info(f"{len(rows)} categories exported to {CSV_PATH}")

def dump_tables_info(conn):
    """Dump tables info (name, columns, number of records) to text file"""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    logging.info(f"Found {len(tables)} tables in database")

    with open(TABLES_INFO_PATH, 'w', encoding='utf-8') as f:
        for (table_name,) in tables:
            # Get columns info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            col_names = [col[1] for col in columns_info]
            col_count = len(col_names)

            # Get records count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            record_count = cursor.fetchone()[0]

            # Write info
            f.write(f"Table: {table_name}\n")
            f.write(f"  Number of columns: {col_count}\n")
            f.write(f"  Columns: {', '.join(col_names)}\n")
            f.write(f"  Number of records: {record_count}\n")
            f.write("-" * 50 + "\n")

            logging.info(f"Table '{table_name}': {record_count} records, {col_count} columns")

    logging.info(f"Tables info exported to {TABLES_INFO_PATH}")

def main():
    if not os.path.exists(DB_PATH):
        logging.error(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    logging.info(f"Connected to database at {DB_PATH}")

    # 1. Export categories table
    export_categories_to_csv(conn)

    # 2. Dump tables info
    dump_tables_info(conn)

    conn.close()
    logging.info("Database export and tables info dump completed")

if __name__ == "__main__":
    main()
