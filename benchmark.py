# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time, random, json

# ================= DRIVER =================
def create_driver():
    options = webdriver.ChromeOptions()

    # options.add_argument("--headless")  # KHÔNG nên bật với TopCV
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")

    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)

    # Ẩn webdriver
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    return driver


# ================= UTILS =================
def human_delay(a=1.5, b=3.0):
    time.sleep(random.uniform(a, b))


def human_scroll(driver):
    for _ in range(random.randint(2, 4)):
        driver.execute_script("window.scrollBy(0, window.innerHeight/2);")
        time.sleep(random.uniform(0.5, 1.2))


def wait_page_loaded(driver, timeout=10):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


# ================= MODE 1 (FIX STALE) =================
def mode_1(driver):
    driver.get("https://www.topcv.vn/")
    human_delay()

    choice = input("Chọn location (hn / hcm / random): ").lower()

    locations = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located(
            (By.XPATH, "//div[contains(@class,'box-smart-list-location')]//div")
        )
    )

    if choice == "random":
        random.choice(locations).click()
    else:
        for loc in locations:
            text = loc.text.lower()
            if choice == "hn" and "ha noi" in text:
                loc.click()
                break
            elif choice == "hcm" and "ho chi minh" in text:
                loc.click()
                break

    human_delay()

    all_links = []
    num_pages = int(input("Muốn crawl bao nhiêu lần ? : "))

    for _ in range(num_pages):
        human_scroll(driver)

        # 👉 LẤY LINK NGAY → KHÔNG GIỮ ELEMENT
        jobs = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, ".feature-job-item h3 a")
            )
        )

        links = []
        for job in jobs:
            try:
                href = job.get_attribute("href")
                if href:
                    links.append(href)
            except:
                continue

        for link in links:
            print(link)
            all_links.append(link)

        # NEXT
        try:
            next_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, ".btn-feature-jobs-next")
                )
            )
            driver.execute_script("arguments[0].click();", next_btn)
            human_delay()
        except:
            break

    return list(set(all_links))


# ================= CRAWL JOB =================
def crawl_job_fast(driver, url, retry=False):
    try:
        driver.get(url)
        wait_page_loaded(driver)

        if not retry:
            human_scroll(driver)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
        )

        title = ""
        company = ""

        try:
            title = driver.find_element(By.CSS_SELECTOR, "h1").text.strip()
        except:
            pass

        try:
            company = driver.find_element(
                By.CSS_SELECTOR, "div.company-name-label a"
            ).text.strip()
        except:
            pass

        # 👉 retry mode (nhanh)
        if retry:
            return {
                "Title": title,
                "Company": company
            }

        soup = BeautifulSoup(driver.page_source, "html.parser")
        job_section = soup.select_one("div.job-description")

        full_text = job_section.get_text("\n", strip=True) if job_section else ""

        return {
            "URL": url,
            "Title": title,
            "Company": company,
            "FullText": full_text
        }

    except Exception as e:
        print("❌ Lỗi:", url, e)
        return None


# ================= RETRY =================
def crawl_with_retry(driver, url):
    data = crawl_job_fast(driver, url, retry=False)

    if not data:
        return None

    if data["Company"] == "" or data["Title"] == "":
        print("🔄 Retry:", url)

        retry_data = crawl_job_fast(driver, url, retry=True)

        if retry_data:
            if data["Company"] == "":
                data["Company"] = retry_data["Company"]
            if data["Title"] == "":
                data["Title"] = retry_data["Title"]

    return data


# ================= MAIN =================
if __name__ == "__main__":
    print("===== TOPCV CRAWLER (STABLE VERSION) =====")

    driver = create_driver()

    all_links = mode_1(driver)
    print(f"\n✅ Tổng số link: {len(all_links)}")

    results = []

    for i, link in enumerate(all_links):
        print(f"[{i+1}/{len(all_links)}] Crawling...")

        data = crawl_with_retry(driver, link)

        if data:
            results.append(data)

        time.sleep(random.uniform(0.8, 1.5))

        # restart driver để tránh block
        if (i + 1) % 15 == 0:
            driver.quit()
            driver = create_driver()

    driver.quit()  # ✅ CHỈ QUIT 1 LẦN Ở ĐÂY

    with open("jobs.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    print(f"\n✅ DONE: {len(results)} jobs saved")