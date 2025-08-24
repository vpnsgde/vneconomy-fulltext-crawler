import os
import csv
import time
import shutil
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

TMP_FRESH_DIR = os.path.join("tmp", "fresh_links")
TMP_HTML_DIR = os.path.join("tmp", "paper_html")
CONTENT_DIR = "content_data"
MAX_THREADS = 20
LOG_PATH = os.path.join("logs", "content_processing_log.txt")

os.makedirs(CONTENT_DIR, exist_ok=True)
os.makedirs(TMP_HTML_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def sanitize_filename(name):
    return "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in name.lower())

def fetch_page_html(url, tmp_html_path):
    """Fetch rendered HTML and save to tmp HTML file"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=30000)
        
        last_height = 0
        for _ in range(20):
            page.evaluate("window.scrollBy(0, 1000);")
            time.sleep(0.3)
            current_height = page.evaluate("document.body.scrollHeight")
            if current_height == last_height:
                break
            last_height = current_height
        time.sleep(0.5)
        html_content = page.content()
        with open(tmp_html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        browser.close()
        logging.info(f"Fetched HTML for {url} -> {tmp_html_path}")
        return html_content

def save_txt_from_html(html_content, output_path, url):
    soup = BeautifulSoup(html_content, "html.parser")
    
    date_tag = soup.select_one("p.date[data-field='distributionDate']")
    if date_tag:
        date_str = date_tag.get_text(strip=True)
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y, %H:%M")
            date_prefix = dt.strftime("%Y-%m-%d-%H-%M")
        except:
            date_prefix = "article"
    else:
        date_prefix = "article"
    
    body_tag = soup.select_one("div[data-field='body']")
    if body_tag:
        paragraphs = [p.get_text(strip=True) for p in body_tag.find_all("p")]
        content = "\n\n".join(paragraphs)
    else:
        content = "Content could not be extracted"
    
    title = url.rstrip("/").split("/")[-1].replace(".htm", "").replace(".html", "")
    title_sanitized = sanitize_filename(title)

    filename = f"{date_prefix}-{title_sanitized}.txt"
    file_path = os.path.join(output_path, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    logging.info(f"Saved text file {file_path}")
    return filename

def crawl_paper(url, tmp_html_path, output_path):
    try:
        html_content = fetch_page_html(url, tmp_html_path)
        filename = save_txt_from_html(html_content, output_path, url)
        return True, filename
    except PlaywrightTimeoutError:
        logging.warning(f"Timeout/Error fetching {url}")
        return False, f"{url} timeout/error"
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return False, f"{url} error: {e}"

def process_category(csv_file):
    category_name = os.path.splitext(csv_file)[0]
    output_dir = os.path.join(CONTENT_DIR, f"fresh_{category_name}")
    tmp_category_dir = os.path.join(TMP_HTML_DIR, category_name)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(tmp_category_dir, exist_ok=True)

    csv_path = os.path.join(TMP_FRESH_DIR, csv_file)
    links = []

    if not os.path.exists(csv_path):
        logging.warning(f"CSV file '{csv_file}' not found, skip.")
        return

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if 'paper_link' not in reader.fieldnames:
            logging.warning(f"CSV file '{csv_file}' missing 'paper_link' column, skip.")
            return
        for row in reader:
            link = row['paper_link'].strip()
            if link:
                links.append(link)

    if not links:
        logging.info(f"No paper links found in '{csv_file}', skip category.")
        shutil.rmtree(tmp_category_dir, ignore_errors=True)
        return

    logging.info(f"Processing category '{category_name}' | Total links: {len(links)}")
    start_time = time.time()
    success_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {}
        for idx, url in enumerate(links, 1):
            tmp_html_path = os.path.join(tmp_category_dir, f"tmp_{idx}.html")
            futures[executor.submit(crawl_paper, url, tmp_html_path, output_dir)] = url

        for future in as_completed(futures):
            try:
                success, info = future.result()
                if success:
                    logging.info(f"[SUCCESS] {info}")
                    success_count += 1
                else:
                    logging.warning(f"[FAIL] {info}")
                    fail_count += 1
            except Exception as e:
                logging.error(f"[EXCEPTION] {e}")
                fail_count += 1

    elapsed = time.time() - start_time
    logging.info(f"Category '{category_name}' finished. Success: {success_count}, Fail: {fail_count}, Time: {elapsed:.2f}s")

    if os.path.exists(tmp_category_dir):
        shutil.rmtree(tmp_category_dir)
        logging.info(f"Temporary HTML folder '{tmp_category_dir}' deleted.")

def main():
    if not os.path.exists(TMP_FRESH_DIR):
        logging.error(f"Folder '{TMP_FRESH_DIR}' does not exist.")
        return

    csv_files = [f for f in os.listdir(TMP_FRESH_DIR) if f.endswith(".csv")]
    if not csv_files:
        logging.info(f"No CSV files found in '{TMP_FRESH_DIR}'.")
        return

    for csv_file in csv_files:
        process_category(csv_file)

if __name__ == "__main__":
    main()
