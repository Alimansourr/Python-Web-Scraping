
import re, time, pandas as pd


# ---------- Shared config ----------
TARGET_TOTAL_ROWS = 1250
BASE_URL = "https://www.coingecko.com/en/all-cryptocurrencies"
TARGET_COLUMNS = [
    "Rank","Name","Symbol","Price",
    "Change_1h","Change_24h","Change_7d","Change_30d",
    "Volume_24h","Circulating_Supply","Total_Supply","Market_Cap"
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
# ======================= Selenium + BS4 =======================
def scrape_via_bs4(target_total_rows=TARGET_TOTAL_ROWS):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    from bs4 import BeautifulSoup

    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu"); options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 25)

    def click_show_more_until_done():
        prev=-1
        while True:
            count = len(driver.find_elements(By.CSS_SELECTOR, "table tbody tr"))
            if count >= target_total_rows or count == prev: break
            try:
                btn = driver.find_element(
                    By.XPATH,
                    "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'show more')]"
                )
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                time.sleep(0.5); btn.click(); time.sleep(2.5)
            except: break
            prev = count

    def norm_header(txt):
        t = re.sub(r"\s+", " ", (txt or "").strip()).lower()
        # Normalize all variants to a SINGLE canonical key
        if "24h" in t and ("vol" in t or "volume" in t):
            t = "24h volume"
        t = t.replace("volume (24h)", "24h volume").replace("vol (24h)", "24h volume")
        t = t.replace("marketcap", "market cap").replace("market capitalization", "market cap")
        t = t.replace("coin name", "coin")
        return t

    def get_header_map(soup):
        mapping={}
        for i, th in enumerate(soup.select("table thead th")):
            lab = norm_header(th.get_text())
            if lab: mapping[lab]=i
        return mapping

    def safe_text(tds, idx):
        return tds[idx].get_text(strip=True) if idx is not None and idx < len(tds) else ""

    def extract_rows(soup, i):
        out=[]
        for r in soup.select("table tbody tr"):
            tds = r.find_all("td")
            if not tds: continue
            name=symbol=""
            if i.get("coin") is not None and i["coin"] < len(tds):
                coin_td = tds[i["coin"]]
                link = coin_td.select_one("a[href*='/coins/']")
                name = link.get_text(strip=True) if link else coin_td.get_text(strip=True)
                small = coin_td.select_one("small")
                if small: symbol = small.get_text(strip=True).upper()
            out.append({
                "Rank": safe_text(tds, i.get("#")),
                "Name": name,
                "Symbol": symbol,
                "Price": safe_text(tds, i.get("price")),
                "Change_1h": safe_text(tds, i.get("1h")),
                "Change_24h": safe_text(tds, i.get("24h")),
                "Change_7d": safe_text(tds, i.get("7d")),
                "Change_30d": safe_text(tds, i.get("30d")),
                "Volume_24h": safe_text(tds, i.get("24h volume")),
                "Circulating_Supply": safe_text(tds, i.get("circulating supply")),
                "Total_Supply": safe_text(tds, i.get("total supply")),
                "Market_Cap": safe_text(tds, i.get("market cap")),
            })
        return out

    try:
        driver.get(BASE_URL)
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table tbody tr"))); time.sleep(1)
        click_show_more_until_done()
        soup = BeautifulSoup(driver.page_source, "lxml")
        i = get_header_map(soup)
        data = extract_rows(soup, i)
    finally:
        driver.quit()
    return ensure_columns(pd.DataFrame(data))

# ======================= Run & Save =======================
if __name__ == "__main__":
    method = "bs4"

    df = scrape_via_bs4(TARGET_TOTAL_ROWS)
    out_name = "coingecko_bs4.csv"

    df.to_csv(out_name, index=False, encoding="utf-8-sig")
    print(f" {method} → saved {len(df)} rows to {out_name}")
