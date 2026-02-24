import requests
import csv
import io
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

NSE_EQUITY_CSV = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"


# ==============================
# Fetch NSE Master Equity List
# ==============================

def fetch_nse_equities():
    print("Fetching NSE master equity list...")

    response = requests.get(NSE_EQUITY_CSV)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch NSE equity list: {response.status_code}")

    decoded = response.content.decode("utf-8")
    decoded = decoded.replace('\ufeff', '')  # Remove BOM if present

    reader = csv.DictReader(io.StringIO(decoded))

    # Normalize headers
    reader.fieldnames = [field.strip() for field in reader.fieldnames]

    equities = []

    for row in reader:
        # Normalize row keys and values
        row = {k.strip(): (v.strip() if v else "") for k, v in row.items()}

        if row.get("SERIES") == "EQ":
            equities.append(row)

    return equities


# ==============================
# Upsert Company
# ==============================

def upsert_company(row):
    symbol = row.get("SYMBOL", "")
    name = row.get("NAME OF COMPANY", "")
    isin = row.get("ISIN NUMBER", "")

    if not symbol:
        return None

    result = supabase.table("companies").upsert(
        {
            "symbol": symbol,
            "name": name,
            "isin": isin,
            "exchange": "NSE",
            "is_listed": True
        },
        on_conflict="symbol"
    ).execute()

    return result.data[0]["id"] if result.data else None


# ==============================
# Main Execution
# ==============================

def main():
    print("Starting NSE universe sync...")

    equities = fetch_nse_equities()

    print(f"Total NSE EQ equities found: {len(equities)}")

    count = 0

    for row in equities:
        cid = upsert_company(row)
        if cid:
            count += 1

    print(f"Upserted {count} companies into database.")
    print("NSE universe sync completed successfully.")


if __name__ == "__main__":
    main()