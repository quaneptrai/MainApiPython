# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import time, random, json, re, datetime
import cloudscraper
import pyodbc  # dùng để check DB
import os
# ================= CONFIG =================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:116.0) Gecko/20100101 Firefox/116.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 Safari/605.1.15"
]

# ================= DB CHECK =================
def get_existing_urls_from_db():
    conn = pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=db47010.public.databaseasp.net;"
        "Database=db47010;"
        "UID=db47010;"
        "PWD=585810quan;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )

    cursor = conn.cursor()
    cursor.execute("SELECT OriginalUrl FROM JobPostings")

    urls = set()
    for row in cursor.fetchall():
        urls.add(row[0])

    conn.close()
    return urls

# ================= UTILS =================
def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]

def get_missing_fields(job):
    return [k for k in ["Responsibilities", "Requirements", "Benefits", "Locations", "WorkTime"] if not job[k]]

def remove_tail_noise(text):
    if not text: return text
    text = re.split(r"\.\.\.và\s*\d+\s*địa điểm khác.*", text, flags=re.IGNORECASE)[0]
    return text.strip()

def is_active(deadline):
    """Trả về True nếu deadline chưa đến"""
    if not deadline: return True
    try:
        # Chuẩn hóa ngày
        deadline_date = datetime.datetime.strptime(deadline, "%d/%m/%Y")
        return deadline_date >= datetime.datetime.now()
    except:
        return True

# ================= PARSE =================
def parse_job_from_soup(soup):
    job_section = soup.select_one("div.job-description")
    if not job_section: return None

    data = {
        "Responsibilities": [], "Requirements": [], "Benefits": [],
        "Locations": [], "LocationTags": [], "WorkTime": "",
        "Salary": "", "Deadline": "", "Experience": ""
    }

    def clean(nodes):
        return [x.get_text(strip=True) for x in nodes if x.get_text(strip=True)]

    for item in job_section.select("div.job-description__item"):
        title_node = item.select_one("h3")
        content_node = item.select_one(".job-description__item--content")
        if not title_node or not content_node: continue
        title = title_node.get_text(strip=True)
        if "Mô tả công việc" in title:
            data["Responsibilities"] = clean(content_node.select("p, li"))
        elif "Yêu cầu" in title:
            data["Requirements"] = clean(content_node.select("p, li"))
        elif "Quyền lợi" in title:
            data["Benefits"] = clean(content_node.select("p, li"))
        elif "Địa điểm" in title:
            raw = [x.get_text(strip=True) for x in content_node.select("div, p") if x.get_text(strip=True)]
            data["Locations"] = [remove_tail_noise(x) for x in raw if remove_tail_noise(x)]
        elif "Thời gian" in title:
            data["WorkTime"] = content_node.get_text(strip=True)

    # Extra
    salary = soup.select_one(".section-salary .job-detail__info--section-content-value")
    if salary: data["Salary"] = salary.get_text(strip=True)
    tags = soup.select(".section-location .job-detail__info--section-content-value a")
    if tags: data["LocationTags"] = [x.get_text(strip=True) for x in tags if x.get_text(strip=True)]
    exp = soup.select_one(".section-experience .job-detail__info--section-content-value")
    if exp: data["Experience"] = exp.get_text(strip=True)
    deadline_tag = soup.select_one(".job-detail__info--deadline")
    if deadline_tag:
        raw = deadline_tag.get_text(strip=True)
        raw = re.sub(r"Hạn nộp hồ sơ:\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\(Còn\s*\d+\s*ngày\)", "", raw, flags=re.IGNORECASE)
        data["Deadline"] = raw.strip()
    return data

# ================= DRIVER =================
def create_driver(headless=False, user_agent=None):
    options = webdriver.ChromeOptions()
    if headless: options.add_argument("--headless=new")
    options.add_argument("--disable-dev-shm-usage") # Tránh lỗi bộ nhớ đệm trên Docker/Linux
    options.add_argument("--disable-extensions")
    options.add_argument("--blink-settings=imagesEnabled=false") # Tắt load ảnh để nhanh hơn
    options.add_argument("--no-proxy-server")
    options.add_argument("--proxy-server='direct://'")
    options.add_argument("--proxy-bypass-list=*")
    if user_agent: options.add_argument(f"user-agent={user_agent}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# ================= CRAWL =================
def crawl_job_fast(url):
    try:
        ua = random.choice(USER_AGENTS)
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url, headers={"User-Agent": ua}, timeout=15)
        if res.status_code != 200: return None
        if "Just a moment" in res.text or "cf-browser-verification" in res.text: return None
        soup = BeautifulSoup(res.text, "html.parser")
        parsed = parse_job_from_soup(soup)
        if not parsed: return None
        title = soup.select_one("h1")
        company = soup.select_one("div.company-name-label a")
        return {
            "OriginalUrl": url,
            "URL": url,
            "Title": title.get_text(strip=True) if title else "",
            "Company": company.get_text(strip=True) if company else "",
            **parsed
        }
    except: return None

def crawl_job_selenium(url):
    driver = create_driver(headless=True)
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".job-description__item")) > 2
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        parsed = parse_job_from_soup(soup)
        if not parsed: return None
        title = soup.select_one("h1")
        company = soup.select_one("div.company-name-label a")
        return {
            "OriginalUrl": url,
            "URL": url,
            "Title": title.get_text(strip=True) if title else "",
            "Company": company.get_text(strip=True) if company else "",
            **parsed
        }
    except TimeoutException: return None
    finally: driver.quit()

def retry_worker(link, timeout=20):
    ua = random.choice(USER_AGENTS)
    driver = create_driver(headless=True, user_agent=ua)
    try:
        driver.set_page_load_timeout(timeout)
        driver.get(link)
        WebDriverWait(driver, 10).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".job-description__item")) > 1
        )
        time.sleep(random.uniform(3, 6))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        parsed = parse_job_from_soup(soup)
        if not parsed: return None
        title = soup.select_one("h1")
        company = soup.select_one("div.company-name-label a")
        return {
            "OriginalUrl": link,
            "URL": link,
            "Title": title.get_text(strip=True) if title else "",
            "Company": company.get_text(strip=True) if company else "",
            **parsed
        }
    except TimeoutException:
        return None
    finally: driver.quit()

# ================= WORKER =================
def worker(link):
    data = crawl_job_fast(link)
    if not data:
        data = crawl_job_selenium(link)
        if not data: return None

    # Tạo fulltext
    parts = [
        f"Title: {data.get('Title','')}",
        f"Company: {data.get('Company','')}"
    ]
    if data.get("Locations"): parts.append("Locations: " + "; ".join(data["Locations"]))
    if data.get("WorkTime"): parts.append(f"WorkTime: {data['WorkTime']}")
    for field in ["Salary", "Deadline", "Experience"]:
        if data.get(field): parts.append(f"{field}: {data[field]}")
    for field in ["Responsibilities", "Requirements", "Benefits"]:
        if data.get(field): parts.append(f"{field}:\n" + "\n".join(data[field]))
    if data.get("LocationTags"): parts.append("LocationTags: " + ", ".join(data["LocationTags"]))
    data["FullText"] = "\n\n".join(parts)

    # isActive
    data["isActive"] = is_active(data.get("Deadline"))

    return data
def get_links():
    driver = create_driver(headless=False)
    driver.get("https://www.topcv.vn/")
    input("Chọn vị trí xong nhấn Enter...")
    pages = int(input("Số trang crawl: "))
    all_links = []
    for p in range(pages):
        print(f"👉 Page {p+1}")
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".feature-job-item h3 a"))
        )
        jobs = driver.find_elements(By.CSS_SELECTOR, ".feature-job-item h3 a")
        for job in jobs:
            href = job.get_attribute("href")
            if href: all_links.append(href)
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, ".btn-feature-jobs-next")
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(2)
        except:
            break
    driver.quit()
    return list(set(all_links))
# ================= MAIN =================
if __name__ == "__main__":
    print("Getting links...")
    links = get_links()
    print(f"Total links: {len(links)}")

    # Check DB để loại link đã có
    existing_urls = get_existing_urls_from_db()
    links = [l for l in links if l not in existing_urls]
    print(f"Links after DB filter: {len(links)}")

    results = []
    failed_links = []

    # ===== PHASE 1: FAST =====
    print("\n⚡ Phase 1: Fast crawl (cloudscraper)")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(worker, link): link for link in links}
        for i, future in enumerate(as_completed(futures)):
            link = futures[future]
            data = future.result()
            print(f"[FAST {i+1}/{len(links)}] {link} -> {'Success' if data else 'Fail'}")
            if data: results.append(data)
            else: failed_links.append(link)

    # ===== PHASE 2: RETRY =====
    batch_size = 2
    print(f"\n🔁 Phase 2: Selenium retry {len(failed_links)} links")
    for i in range(0, len(failed_links), batch_size):
        batch = failed_links[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = {executor.submit(retry_worker, link): link for link in batch}
            for j, future in enumerate(as_completed(futures)):
                link = futures[future]
                data = future.result()
                progress = i + j + 1
                print(f"[RETRY {progress}/{len(failed_links)}] {link} -> {'Success' if data else 'Fail'}")
                if data:
                    # Tạo fulltext + isActive
                    parts = [
                        f"Title: {data.get('Title','')}",
                        f"Company: {data.get('Company','')}"
                    ]
                    if data.get("Locations"): parts.append("Locations: " + "; ".join(data["Locations"]))
                    if data.get("WorkTime"): parts.append(f"WorkTime: {data['WorkTime']}")
                    for field in ["Salary", "Deadline", "Experience"]:
                        if data.get(field): parts.append(f"{field}: {data[field]}")
                    for field in ["Responsibilities", "Requirements", "Benefits"]:
                        if data.get(field): parts.append(f"{field}:\n" + "\n".join(data[field]))
                    if data.get("LocationTags"): parts.append("LocationTags: " + ", ".join(data["LocationTags"]))
                    data["FullText"] = "\n\n".join(parts)
                    data["isActive"] = is_active(data.get("Deadline"))
                    results.append(data)

    # ===== SAVE JSON =====
    if os.path.exists("jobs.json"):
        with open("jobs.json", "r", encoding="utf-8") as f:
            old_results = json.load(f)
    else:
        old_results = []

    # Loại trùng lặp theo OriginalUrl
    existing_urls_json = {job["OriginalUrl"] for job in old_results if "OriginalUrl" in job}
    new_results = [r for r in results if r["OriginalUrl"] not in existing_urls_json]
    all_results = old_results + new_results

    with open("jobs.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=4)

    print(f"Done saved to jobs.json, total {len(all_results)} jobs")

