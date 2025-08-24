import os
import csv
import re
import sqlite3
import shutil
import logging

TMP_FRESH_DIR = os.path.join("tmp", "fresh_links")
CONTENT_DIR = "content_data"
PAPER_LINKS_DIR = "paper_links"
CATEGORIES_CSV = os.path.join("tmp", "categories.csv")
DB_PATH = os.path.join("database", "vneconomy_news.db")
LOG_PATH = os.path.join("logs", "post_database_log.txt")

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def load_category_index():
    mapping = {}
    if not os.path.exists(CATEGORIES_CSV):
        logging.error(f"Categories CSV '{CATEGORIES_CSV}' not found")
        return mapping
    with open(CATEGORIES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, 1):
            if 'category_link' not in row:
                continue
            link = row['category_link'].strip()
            if not link:
                continue
            category_name = link.rstrip("/").split("/")[-1].replace(".htm", "").replace(".html", "")
            mapping[category_name] = idx
    logging.info(f"Loaded {len(mapping)} categories from CSV")
    return mapping

def append_to_paper_links(category_name, fresh_rows):
    os.makedirs(PAPER_LINKS_DIR, exist_ok=True)
    target_file = os.path.join(PAPER_LINKS_DIR, f"{category_name}.csv")

    last_index = 0
    if os.path.exists(target_file):
        with open(target_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'index' in row and row['index'].strip().isdigit():
                    last_index = max(last_index, int(row['index'].strip()))

    if not os.path.exists(target_file) or os.path.getsize(target_file) == 0:
        with open(target_file, 'w', newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(['index', 'category_index', 'paper_link'])

    with open(target_file, 'a', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        for i, (category_index, paper_link) in enumerate(fresh_rows, start=last_index + 1):
            writer.writerow([i, category_index, paper_link])
    logging.info(f"[{category_name}] Appended {len(fresh_rows)} rows to {target_file}")

def import_links(conn):
    if not os.path.exists(TMP_FRESH_DIR):
        logging.info("No fresh links folder, skip import links")
        return

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS links (
            idx INTEGER PRIMARY KEY AUTOINCREMENT,
            category_index INTEGER NOT NULL,
            paper_link TEXT NOT NULL,
            UNIQUE(category_index, paper_link)
        )
    """)
    conn.commit()

    total_inserted = 0
    for csv_file in os.listdir(TMP_FRESH_DIR):
        if not csv_file.endswith(".csv"):
            continue
        category_name = os.path.splitext(csv_file)[0]
        csv_path = os.path.join(TMP_FRESH_DIR, csv_file)

        fresh_rows = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    category_index = int(row['category_index'])
                    paper_link = row['paper_link'].strip()
                    if paper_link:
                        fresh_rows.append((category_index, paper_link))
                        cursor.execute("""
                            INSERT OR IGNORE INTO links (category_index, paper_link)
                            VALUES (?, ?)
                        """, (category_index, paper_link))
                        total_inserted += cursor.rowcount
                except Exception as e:
                    logging.error(f"Inserting row {row}: {e}")
        conn.commit()

        append_to_paper_links(category_name, fresh_rows)

    shutil.rmtree(TMP_FRESH_DIR, ignore_errors=True)
    logging.info(f"Folder '{TMP_FRESH_DIR}' deleted")
    logging.info(f"Links import finished. Total inserted: {total_inserted}")

def import_contents(conn, category_map):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contents (
            idx INTEGER PRIMARY KEY AUTOINCREMENT,
            category_index INTEGER NOT NULL,
            publish_date TEXT,
            title TEXT,
            text BLOB,
            UNIQUE(category_index, publish_date, title)
        )
    """)
    conn.commit()

    total_inserted = 0
    for folder in os.listdir(CONTENT_DIR):
        if not folder.startswith("fresh_"):
            continue
        category_name = folder.replace("fresh_", "").replace("_", "-")
        category_index = category_map.get(category_name)
        if category_index is None:
            logging.warning(f"Category '{category_name}' not found in categories.csv, skip.")
            continue

        folder_path = os.path.join(CONTENT_DIR, folder)
        dest_folder = os.path.join(CONTENT_DIR, category_name)
        os.makedirs(dest_folder, exist_ok=True)

        for txt_file in os.listdir(folder_path):
            if not txt_file.endswith(".txt"):
                continue
            txt_path = os.path.join(folder_path, txt_file)
            m = re.match(r"(\d{4}-\d{2}-\d{2}-\d{2}-\d{2})-(.+)\.txt", txt_file)
            if not m:
                logging.warning(f"Filename '{txt_file}' does not match pattern, skip.")
                continue
            publish_date_str = m.group(1)
            title = m.group(2).replace("_", " ")
            try:
                with open(txt_path, "r", encoding="utf-8") as f:
                    text = f.read()
                cursor.execute("""
                    INSERT OR IGNORE INTO contents (category_index, publish_date, title, text)
                    VALUES (?, ?, ?, ?)
                """, (category_index, publish_date_str, title, text))
                total_inserted += cursor.rowcount

                shutil.copy2(txt_path, os.path.join(dest_folder, txt_file))

            except Exception as e:
                logging.error(f"Inserting file '{txt_file}': {e}")

        shutil.rmtree(folder_path, ignore_errors=True)
        logging.info(f"Folder '{folder_path}' deleted")

    conn.commit()
    logging.info(f"Contents import finished. Total inserted: {total_inserted}")

def main():
    category_map = load_category_index()
    if not category_map:
        logging.error("No categories loaded, exiting.")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        import_links(conn)
        import_contents(conn, category_map)
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
