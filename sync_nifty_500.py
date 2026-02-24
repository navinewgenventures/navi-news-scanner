import requests
import os
from supabase import create_client
from dotenv import load_dotenv

# ==============================
# Load Environment Variables
# ==============================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing SUPABASE credentials in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================
# NSE Fetch Logic
# ==============================

def fetch_nifty_500():
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%20500",
        "Connection": "keep-alive"
    }

    session = requests.Session()

    # First request to establish cookies
    session.get("https://www.nseindia.com", headers=headers)

    response = session.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"NSE API failed: {response.status_code}")

    data = response.json()

    if "data" not in data:
        raise Exception("Unexpected NSE response format")

    return data["data"]


# ==============================
# Supabase Logic
# ==============================

def upsert_company(symbol, name):
    result = supabase.table("companies").upsert(
        {
            "symbol": symbol,
            "name": name,
            "exchange": "NSE"
        },
        on_conflict="symbol"
    ).execute()

    return result.data[0]["id"]


def sync_index_membership(company_ids):
    # Fetch index ID
    index = supabase.table("indices") \
        .select("id") \
        .eq("name", "NIFTY_500") \
        .execute()

    if not index.data:
        raise Exception("NIFTY_500 index not found in indices table")

    index_id = index.data[0]["id"]

    # Mark all existing members inactive
    supabase.table("index_membership") \
        .update({"is_active": False}) \
        .eq("index_id", index_id) \
        .execute()

    # Reactivate current members
    for cid in company_ids:
        supabase.table("index_membership").upsert(
            {
                "index_id": index_id,
                "company_id": cid,
                "is_active": True
            },
            on_conflict="index_id,company_id"
        ).execute()


# ==============================
# Main Execution
# ==============================

def main():
    print("Fetching Nifty 500 data...")

    stocks = fetch_nifty_500()

    print(f"Fetched {len(stocks)} stocks.")

    company_ids = []

    for stock in stocks:
        symbol = stock.get("symbol")

       # Skip index summary row
    if not symbol or symbol == "NIFTY 500":
            continue

        # NSE endpoint does not provide full company name
        name = symbol

        cid = upsert_company(symbol, name)
        company_ids.append(cid)

    print("Updating index membership...")
    sync_index_membership(company_ids)

    print("Nifty 500 sync completed successfully.")


if __name__ == "__main__":
    main()