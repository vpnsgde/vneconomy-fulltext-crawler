import csv
import os
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TMP_DIR = os.path.join(BASE_DIR, '../tmp')
OUTPUT_DIR = os.path.join(BASE_DIR, '../paper_links')
FRESH_DIR = os.path.join(TMP_DIR, 'fresh_links')
LOG_PATH = os.path.join(BASE_DIR, '../logs/pages_processing_log.txt')

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FRESH_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

BASE_URL = "https://vneconomy.vn"
MAX_PAGES = 200
MAX_THREADS = 20
MAX_EMPTY_STREAK = 5  # dừng nếu 5 page liên tiếp không link mới

def sanitize_filename(name):
    return "".join(c if c.isalnum() else "_" for c in name)

def extract_links_from_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    links = set()
    for a in soup.select("a.link-layer-imt"):
        href = a.get("href")
        if href:
            full_link = href if href.startswith("http") else BASE_URL + href
            links.add(full_link)
    return links

def read_existing_links(category_name):
    file_path = os.path.join(OUTPUT_DIR, f"{sanitize_filename(category_name)}.csv")
    existing_links = set()
    last_index = 0
    if not os.path.exists(file_path):
        return existing_links, last_index

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'paper_link' in row and row['paper_link'].strip():
                existing_links.add(row['paper_link'].strip())
            if 'index' in row and row['index'].strip().isdigit():
                last_index = max(last_index, int(row['index'].strip()))
    return existing_links, last_index

def save_fresh_links(category_name, links, category_index, start_index=0):
    file_path = os.path.join(FRESH_DIR, f"{sanitize_filename(category_name)}.csv")
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(['index', 'category_index', 'paper_link'])
        for i, link in enumerate(sorted(links), start=start_index + 1):
            writer.writerow([i, category_index, link])
    logging.info(f"[{category_name}] {len(links)} new links saved to {file_path}")

def crawl_category(category_url, category_index):
    category_name = category_url.rstrip("/").split("/")[-1].replace(".htm", "").replace(".html", "")
    logging.info(f"=== Start crawling category: {category_name} ===")
    
    existing_links, last_index = read_existing_links(category_name)
    all_links = set()
    empty_streak = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{category_url}?page={page_num}"
            try:
                page.goto(url, timeout=30000)
                page.wait_for_load_state("networkidle")
                html_content = page.content()
                links = extract_links_from_html(html_content)
                new_links = links - existing_links - all_links

                if new_links:
                    empty_streak = 0
                    all_links.update(new_links)
                    logging.info(f"[{category_name}] Page {page_num}: {len(new_links)} new links found (Total: {len(all_links)})")
                else:
                    empty_streak += 1
                    logging.info(f"[{category_name}] Page {page_num}: no new links (empty streak {empty_streak})")
                    if empty_streak >= MAX_EMPTY_STREAK:
                        logging.info(f"[{category_name}] Stop crawling due to {MAX_EMPTY_STREAK} empty pages in a row.")
                        break
            except PlaywrightTimeoutError:
                logging.warning(f"[{category_name}] Page {page_num} timeout/error, skip to next page.")
                continue
            except Exception as e:
                logging.error(f"[{category_name}] Page {page_num} unexpected error: {e}, skip.")
                continue

        browser.close()

    if all_links:
        save_fresh_links(category_name, all_links, category_index, start_index=last_index)
    else:
        logging.info(f"[{category_name}] No new links found.")

def main():
    categories_csv = os.path.join(TMP_DIR, 'categories.csv')
    categories = []
    category_index_map = {}

    with open(categories_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row['category_link'].strip()
            idx = int(row['index'])
            categories.append(url)
            category_index_map[url] = idx

    logging.info(f"Start crawling {len(categories)} categories with {MAX_THREADS} threads")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(crawl_category, url, category_index_map[url]): url for url in categories}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in thread for {futures[future]}: {e}")

    logging.info("Crawling all categories completed.")

if __name__ == "__main__":
    main()
