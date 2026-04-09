# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
import time, random, json, re

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:116.0) Gecko/20100101 Firefox/116.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 Safari/605.1.15"
]

# ================= DRIVER =================
def create_driver(headless=False, user_agent=None):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    if user_agent:
        options.add_argument(f"user-agent={user_agent}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(options=options)


# ================= UTILS =================
def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]


def get_missing_fields(job):
    return [
        k for k in ["Responsibilities", "Requirements", "Benefits", "Locations", "WorkTime"]
        if not job[k]
    ]


def remove_tail_noise(text):
    if not text:
        return text
    text = re.split(r"\.\.\.và\s*\d+\s*địa điểm khác.*", text, flags=re.IGNORECASE)[0]
    return text.strip()


# ================= PARSE =================
def parse_job_from_soup(soup):
    job_section = soup.select_one("div.job-description")
    if not job_section:
        return None

    data = {
        "Responsibilities": [],
        "Requirements": [],
        "Benefits": [],
        "Locations": [],
        "LocationTags": [],
        "WorkTime": "",
        "Salary": "",
        "Deadline": "",
        "Experience": ""
    }

    def clean(nodes):
        return [x.get_text(strip=True) for x in nodes if x.get_text(strip=True)]

    for item in job_section.select("div.job-description__item"):
        title_node = item.select_one("h3")
        content_node = item.select_one(".job-description__item--content")

        if not title_node or not content_node:
            continue

        title = title_node.get_text(strip=True)

        if "Mô tả công việc" in title:
            data["Responsibilities"] = clean(content_node.select("p, li"))

        elif "Yêu cầu" in title:
            data["Requirements"] = clean(content_node.select("p, li"))

        elif "Quyền lợi" in title:
            data["Benefits"] = clean(content_node.select("p, li"))

        elif "Địa điểm" in title:
            raw = [x.get_text(strip=True) for x in content_node.select("div, p") if x.get_text(strip=True)]
            cleaned = []
            for item in raw:
                item = remove_tail_noise(item)
                if item:
                    cleaned.append(item)
            data["Locations"] = cleaned

        elif "Thời gian" in title:
            data["WorkTime"] = content_node.get_text(strip=True)

    # ===== EXTRA =====
    salary = soup.select_one(".section-salary .job-detail__info--section-content-value")
    if salary:
        data["Salary"] = salary.get_text(strip=True)

    tags = soup.select(".section-location .job-detail__info--section-content-value a")
    if tags:
        data["LocationTags"] = [x.get_text(strip=True) for x in tags if x.get_text(strip=True)]

    exp = soup.select_one(".section-experience .job-detail__info--section-content-value")
    if exp:
        data["Experience"] = exp.get_text(strip=True)

    deadline_tag = soup.select_one(".job-detail__info--deadline")
    if deadline_tag:
        raw = deadline_tag.get_text(strip=True)
        raw = re.sub(r"Hạn nộp hồ sơ:\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\(Còn\s*\d+\s*ngày\)", "", raw, flags=re.IGNORECASE)
        data["Deadline"] = raw.strip()

    return data


# ================= GET LINKS =================
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
            if href:
                all_links.append(href)

        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, ".btn-feature-jobs-next")
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(2)
        except:
            break

    driver.quit()
    return list(set(all_links))


# ================= REQUESTS =================
def crawl_job_requests(url):
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)

        if "Just a moment" in res.text:
            return None

        soup = BeautifulSoup(res.text, "html.parser")
        parsed = parse_job_from_soup(soup)

        if not parsed:
            return None

        title = soup.select_one("h1")
        company = soup.select_one("div.company-name-label a")

        return {
            "URL": url,
            "Title": title.get_text(strip=True) if title else "",
            "Company": company.get_text(strip=True) if company else "",
            **parsed
        }

    except:
        return None


# ================= SELENIUM =================
def crawl_job_selenium(url):
    driver = create_driver(headless=True)

    try:
        driver.get(url)

        WebDriverWait(driver, 10).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".job-description__item")) > 2
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        parsed = parse_job_from_soup(soup)

        if not parsed:
            return None

        title = soup.select_one("h1")
        company = soup.select_one("div.company-name-label a")

        return {
            "URL": url,
            "Title": title.get_text(strip=True) if title else "",
            "Company": company.get_text(strip=True) if company else "",
            **parsed
        }

    except:
        return None

    finally:
        driver.quit()


# ================= WORKER =================
def worker(link):
    data = crawl_job_requests(link)

    if not data:
        return None

    missing = get_missing_fields(data)

    if missing:
        retry = crawl_job_selenium(link)
        if retry:
            for k in missing:
                if retry.get(k):
                    data[k] = retry[k]

    # ===== FULLTEXT =====
    parts = []

    parts.append(f"Title: {data.get('Title','')}")
    parts.append(f"Company: {data.get('Company','')}")

    if data.get("Locations"):
        parts.append("Locations: " + "; ".join(data["Locations"]))

    if data.get("WorkTime"):
        parts.append(f"WorkTime: {data['WorkTime']}")

    for field in ["Salary", "Deadline", "Experience"]:
        if data.get(field):
            parts.append(f"{field}: {data[field]}")

    for field in ["Responsibilities", "Requirements", "Benefits"]:
        if data.get(field):
            parts.append(f"{field}:\n" + "\n".join(data[field]))

    if data.get("LocationTags"):
        parts.append("LocationTags: " + ", ".join(data["LocationTags"]))

    data["FullText"] = "\n\n".join(parts)

    return data


# ================= RETRY =================
def retry_worker(link, max_attempts=5, wait_between=2):
    print(f"🔁 Retry: {link}")
    for attempt in range(1, max_attempts+1):
        ua = random.choice(USER_AGENTS)
        driver = create_driver(headless=True, user_agent=ua)
        try:
            driver.get(link)
            WebDriverWait(driver,10).until(lambda d: len(d.find_elements(By.CSS_SELECTOR,".job-description__item"))>2)
            soup = BeautifulSoup(driver.page_source,"html.parser")
            parsed = parse_job_from_soup(soup)
            if parsed:
                title = soup.select_one("h1")
                company = soup.select_one("div.company-name-label a")
                data = {"URL": link, "Title": title.get_text(strip=True) if title else "",
                        "Company": company.get_text(strip=True) if company else "", **parsed}
                return data
        finally:
            driver.quit()
        time.sleep(wait_between + random.random()*2)
    return None


# ================= MAIN =================
if __name__ == "__main__":
    print("Getting links...")
    links = get_links()

    print(f"Total links: {len(links)}")

    results = []
    failed_links = []

    # ===== MAIN =====
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(worker, link): link for link in links}

        for i, future in enumerate(as_completed(futures)):
            link = futures[future]
            print(f"[{i+1}/{len(links)}]")

            data = future.result()

            if data:
                results.append(data)
            else:
                failed_links.append(link)

    # ===== RETRY =====
    print(f"\n🔁 Retry failed: {len(failed_links)}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(retry_worker, link) for link in failed_links]

        for future in as_completed(futures):
            data = future.result()
            if data:
                results.append(data)

    # ===== SAVE =====
    with open("jobs.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    print("Done saved to jobs.json")
    print(f"Saved {len(results)} JSON objects")