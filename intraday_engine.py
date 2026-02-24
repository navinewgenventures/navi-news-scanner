import os
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client
from dotenv import load_dotenv

# ==============================
# Load Environment Variables
# ==============================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials missing.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================
# Telegram Alert Function
# ==============================

def send_telegram_alert(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("Telegram credentials missing.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print("Telegram failed:", response.text)
    except Exception as e:
        print("Telegram error:", e)

# ==============================
# Keyword Scoring Dictionaries
# ==============================

HIGH_IMPACT = {
    "fraud": -60,
    "scam": -60,
    "default": -50,
    "bankruptcy": -70,
    "investigation": -40,
    "resignation": -35,
    "penalty": -30,
    "downgrade": -25,
    "crash": -50,
    "plunge": -40,
    "acquisition": 50,
    "buyback": 40,
    "stake increase": 35,
    "order win": 30,
    "major contract": 40,
    "record profit": 35
}

MEDIUM_IMPACT = {
    "growth": 15,
    "upgrade": 15,
    "expansion": 20,
    "guidance raise": 25,
    "decline": -15,
    "loss": -20
}

LOW_IMPACT = {
    "volatility": 5,
    "market reaction": 5
}

# ==============================
# Fetch Recent Events (12 hours)
# ==============================

def fetch_recent_events():
    twelve_hours_ago = (
        datetime.now(timezone.utc) - timedelta(hours=12)
    ).isoformat()

    result = (
        supabase.table("processed_events")
        .select("id, raw_news_id, company_id, processed_at")
        .gte("processed_at", twelve_hours_ago)
        .execute()
    )

    return result.data or []

# ==============================
# Fetch Article Text
# ==============================

def fetch_article_text(raw_news_id):
    result = (
        supabase.table("raw_news")
        .select("title, content")
        .eq("id", raw_news_id)
        .single()
        .execute()
    )

    data = result.data or {}
    return f"{data.get('title','')} {data.get('content','')}".lower()

# ==============================
# Classify Event
# ==============================

def classify_event(text):
    score = 0

    for keyword, weight in HIGH_IMPACT.items():
        if keyword in text:
            score += weight

    for keyword, weight in MEDIUM_IMPACT.items():
        if keyword in text:
            score += weight

    for keyword, weight in LOW_IMPACT.items():
        if keyword in text:
            score += weight

    if score >= 40:
        severity = "HIGH"
    elif abs(score) >= 20:
        severity = "MEDIUM"
    elif abs(score) > 0:
        severity = "LOW"
    else:
        severity = None

    return severity, score

# ==============================
# Prevent Duplicate Signals
# ==============================

def signal_exists(raw_news_id):
    result = (
        supabase.table("signals")
        .select("id")
        .eq("raw_news_id", raw_news_id)
        .execute()
    )
    return bool(result.data)

# ==============================
# Generate Signals
# ==============================

def generate_intraday_signals():
    print("Running intraday shock engine...")

    events = fetch_recent_events()
    print(f"Found {len(events)} recent events.")

    for event in events:
        raw_news_id = event["raw_news_id"]
        company_id = event["company_id"]

        if signal_exists(raw_news_id):
            continue

        text = fetch_article_text(raw_news_id)
        severity, score = classify_event(text)

        print(f"DEBUG → Score: {score}, Severity: {severity}")

        if not severity:
            continue

        if score >= 40:
            signal_type = "BUY"
        elif score <= -40:
            signal_type = "SELL"
        else:
            continue

        # Insert signal
        supabase.table("signals").insert({
            "company_id": company_id,
            "raw_news_id": raw_news_id,
            "signal_type": signal_type,
            "severity": severity,
            "signal_score": score,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "is_active": True
        }).execute()

        # Fetch metadata
        company = supabase.table("companies").select("name").eq("id", company_id).single().execute()
        news = supabase.table("raw_news").select("title").eq("id", raw_news_id).single().execute()

        company_name = company.data["name"]
        headline = news.data["title"]

        message = f"""
🚨 *{signal_type} SIGNAL*
Company: *{company_name}*
Severity: *{severity}*
Score: *{score}*

📰 {headline}
"""

        send_telegram_alert(message)

        print(f"{signal_type} signal sent for {company_name}")

    print("Intraday engine completed.")

# ==============================

if __name__ == "__main__":
    generate_intraday_signals()