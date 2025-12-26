import time, requests, pandas as pd

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

# ======================= API =======================
def fetch_via_api(max_pages=5, per_page=250):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d,30d"
    }
    all_rows, page_count = [], 0
    print("Fetching (API)…")
    while page_count < max_pages:
        r = requests.get(url, params=params)
        if r.status_code != 200:
            print(f"API error {r.status_code}: {r.text[:120]}")
            if r.status_code == 429:
                print("Rate limit → sleeping 60s."); time.sleep(60); continue
            break
        data = r.json()
        if not isinstance(data, list) or not data: break
        for coin in data:
            all_rows.append({
                "Rank": str(coin.get("market_cap_rank") or "").strip(),
                "Name": coin.get("name",""),
                "Symbol": (coin.get("symbol","") or "").upper(),
                "Price": money_to_str(coin.get("current_price")),
                "Change_1h": pct_to_str(coin.get("price_change_percentage_1h_in_currency")),
                "Change_24h": pct_to_str(coin.get("price_change_percentage_24h_in_currency") or coin.get("price_change_percentage_24h")),
                "Change_7d": pct_to_str(coin.get("price_change_percentage_7d_in_currency")),
                "Change_30d": pct_to_str(coin.get("price_change_percentage_30d_in_currency")),
                "Volume_24h": money_to_str(coin.get("total_volume")),
                "Circulating_Supply": num_to_str(coin.get("circulating_supply")),
                "Total_Supply": num_to_str(coin.get("total_supply")),
                "Market_Cap": money_to_str(coin.get("market_cap")),
            })
        print(f"  API page {params['page']} → {len(data)} rows")
        params["page"] += 1; page_count += 1; time.sleep(1)
    return ensure_columns(pd.DataFrame(all_rows))


# ======================= Run & Save =======================
if __name__ == "__main__":
    method = "api"

    df = fetch_via_api(max_pages=5, per_page=250)
    out_name = "coingecko_api.csv"

    df.to_csv(out_name, index=False, encoding="utf-8-sig")
    print(f" {method} → saved {len(df)} rows to {out_name}")
