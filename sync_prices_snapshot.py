import requests
import os
import time
from supabase import create_client
from dotenv import load_dotenv

# ==============================
# Load Environment Variables
# ==============================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"


# ==============================
# Fetch NSE Symbols from DB
# ==============================

def fetch_nse_symbols():
    result = (
        supabase.table("companies")
        .select("id,symbol")
        .eq("exchange", "NSE")
        .execute()
    )

    return result.data


# ==============================
# Fetch Prices from Yahoo
# ==============================

def fetch_prices(symbols):
    yahoo_symbols = [f"{s}.NS" for s in symbols]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    }

    try:
        response = requests.get(
            YAHOO_QUOTE_URL,
            params={"symbols": ",".join(yahoo_symbols)},
            headers=headers,
            timeout=10,
        )

        print("Status:", response.status_code)

        if response.status_code != 200:
            print("⚠ Yahoo returned non-200 response")
            print(response.text[:200])
            return []

        if not response.text.strip():
            print("⚠ Empty response from Yahoo")
            return []

        data = response.json()
        return data.get("quoteResponse", {}).get("result", [])

    except Exception as e:
        print("⚠ Error fetching prices:", e)
        return []


# ==============================
# Insert Prices into DB
# ==============================

def insert_prices(price_data, symbol_map):
    for stock in price_data:
        raw_symbol = stock.get("symbol", "").replace(".NS", "")
        company_id = symbol_map.get(raw_symbol)

        if not company_id:
            continue

        try:
            supabase.table("prices").insert(
                {
                    "company_id": company_id,
                    "price": stock.get("regularMarketPrice"),
                    "open": stock.get("regularMarketOpen"),
                    "high": stock.get("regularMarketDayHigh"),
                    "low": stock.get("regularMarketDayLow"),
                    "volume": stock.get("regularMarketVolume"),
                    "change": stock.get("regularMarketChange"),
                    "pchange": stock.get("regularMarketChangePercent"),
                }
            ).execute()
        except Exception as e:
            print(f"⚠ DB insert error for {raw_symbol}: {e}")


# ==============================
# Main
# ==============================

def main():
    print("Starting price snapshot sync...")

    companies = fetch_nse_symbols()
    print(f"Fetched {len(companies)} NSE companies from DB.")

    symbol_map = {c["symbol"]: c["id"] for c in companies}
    symbols = list(symbol_map.keys())

    batch_size = 50  # safer for Yahoo

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]

        print(f"Fetching batch {i} to {i + batch_size}...")

        price_data = fetch_prices(batch)

        if price_data:
            insert_prices(price_data, symbol_map)
        else:
            print("⚠ Skipping batch due to empty response")

        time.sleep(1)  # prevent rate limit

    print("Price snapshot sync completed successfully.")


if __name__ == "__main__":
    main()