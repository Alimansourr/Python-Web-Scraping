import re, time, pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------- Shared config ----------
TARGET_TOTAL_ROWS = 1250
BASE_URL = "https://www.coingecko.com/en/all-cryptocurrencies"
TARGET_COLUMNS = [
    "Rank", "Name", "Symbol", "Price", "Change_1h", "Change_24h", "Change_7d", "Change_30d",
    "Volume_24h", "Circulating_Supply", "Total_Supply", "Market_Cap"
]

# ---------- format helpers ----------
def pct_to_str(x):
    if x is None or x == "": return ""
    try: return f"{float(x):.2f}%"
    except: return str(x)

def money_to_str(x):
    if x is None or x == "": return ""
    try: return f"${float(x):,.8f}".rstrip("0").rstrip(".")
    except: return str(x)

def num_to_str(x):
    if x is None or x == "": return ""
    try: return f"{float(x):,.8f}".rstrip("0").rstrip(".")
    except: return str(x)

def ensure_columns(df):
    for c in TARGET_COLUMNS:
        if c not in df.columns: df[c] = ""
    return df[TARGET_COLUMNS]

# ======================= Selenium (direct) =======================
def scrape_page_data(page_number, target_total_rows=TARGET_TOTAL_ROWS):
    def norm_header(txt):
        t = re.sub(r"\s+"," ",(txt or "").strip()).lower()
        t = t.replace("volume (24h)","24h volume").replace("vol (24h)","24h volume")
        if "24h" in t and "volume" in t: t = "24h volume"
        t = t.replace("marketcap","market cap").replace("market capitalization","market cap")
        t = t.replace("coin name","coin")
        return t

    def get_header_map(driver):
        ths = driver.find_elements(By.CSS_SELECTOR, 'table[data-view-component="true"] thead th')
        mapping = {}
        for i, th in enumerate(ths):
            lab = norm_header(th.text)
            if lab: mapping[lab] = i
        return mapping

    def count_rows(driver):
        return len(driver.find_elements(By.CSS_SELECTOR, 'tbody[data-more-content-target="content"] tr[data-view-component="true"]'))

    SHOW_MORE = [
        (By.XPATH, "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'show more')]"),
        (By.CSS_SELECTOR, "button[data-action*='more-content#load']"),
        (By.XPATH, "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'show more')]"),
        (By.CSS_SELECTOR, "a[data-action*='more-content#load']"),
    ]

    def find_show_more(driver):
        for how, sel in SHOW_MORE:
            els = driver.find_elements(how, sel)
            if els: return (how, sel), els[0]
        return None, None

    def click_show_more(driver, wait):
        loc, el = find_show_more(driver)
        if not el: return False
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        if loc: wait.until(EC.element_to_be_clickable(loc))
        try: el.click()
        except: driver.execute_script("arguments[0].click();", el)
        return True

    def load_until_rows(driver, wait, target_rows, max_tries=100):
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'tbody[data-more-content-target="content"] tr[data-view-component="true"]')))
        time.sleep(0.8); prev=-1; tries=0
        while tries < max_tries:
            current = count_rows(driver)
            if current >= target_rows or current == prev: break
            if not click_show_more(driver, wait): break
            WebDriverWait(driver, 20).until(lambda d: count_rows(d) > current)
            time.sleep(0.6); prev=current; tries+=1

    def safe_cell_text(tds, idx):
        try: return tds[idx].text.strip()
        except: return ""

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized"); options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 25)

    try:
        driver.get(BASE_URL); time.sleep(1.5)
        header_map = get_header_map(driver)
        load_until_rows(driver, wait, target_total_rows)
        rows = driver.find_elements(By.CSS_SELECTOR, 'tbody[data-more-content-target="content"] tr[data-view-component="true"]')
        i = header_map; out=[]
        for r in rows:
            tds = r.find_elements(By.TAG_NAME, "td")
            if not tds: continue
            name=symbol=""
            if i.get("coin") is not None and i["coin"] < len(tds):
                coin_td = tds[i["coin"]]
                try: name = coin_td.find_element(By.CSS_SELECTOR, "a[href*='/coins/']").text.strip()
                except: name = coin_td.text.strip()
                try: symbol = coin_td.find_element(By.TAG_NAME, "small").text.strip().upper()
                except:
                    try: symbol = coin_td.find_element(By.CSS_SELECTOR,"span[class*='coin-item-symbol']").text.strip().upper()
                    except: symbol=""
            out.append({
                "Rank": safe_cell_text(tds, i.get("#")),
                "Name": name,
                "Symbol": symbol,
                "Price": safe_cell_text(tds, i.get("price")),
                "Change_1h": safe_cell_text(tds, i.get("1h")),
                "Change_24h": safe_cell_text(tds, i.get("24h")),
                "Change_7d": safe_cell_text(tds, i.get("7d")),
                "Change_30d": safe_cell_text(tds, i.get("30d")),
                "Volume_24h": safe_cell_text(tds, i.get("24h volume")),
                "Circulating_Supply": safe_cell_text(tds, i.get("circulating supply")),
                "Total_Supply": safe_cell_text(tds, i.get("total supply")),
                "Market_Cap": safe_cell_text(tds, i.get("market cap")),
            })
    finally:
        driver.quit()
    return ensure_columns(pd.DataFrame(out))

# ======================= Multithreading =======================
def scrape_via_selenium_multithreaded(target_total_rows=TARGET_TOTAL_ROWS):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(scrape_page_data, page) for page in range(1, (target_total_rows // 250) + 1)]
        result = []
        for future in as_completed(futures):
            result.extend(future.result())
    return pd.DataFrame(result)

# ======================= Run & Save =======================
if __name__ == "__main__":
    method = "selenium_multithreaded"
    df = scrape_via_selenium_multithreaded(TARGET_TOTAL_ROWS)
    out_name = "coingecko_selenium_multithreaded.csv"
    df.to_csv(out_name, index=False, encoding="utf-8-sig")
    print(f" {method} → saved {len(df)} rows to {out_name}")
