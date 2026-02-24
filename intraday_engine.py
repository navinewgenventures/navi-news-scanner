import os
import requests
from datetime import datetime, timedelta
from supabase import create_client
from dotenv import load_dotenv

# ==============================
# Load Environment Variables
# ==============================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

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
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print("Telegram error:", e)

# ==============================
# Severity Dictionaries
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
    twelve_hours_ago = (datetime.utcnow() - timedelta(hours=12)).isoformat()

    result = (
        supabase.table("processed_events")
        .select("id, raw_news_id, company_id, processed_at")
        .gte("processed_at", twelve_hours_ago)
        .execute()
    )

    return result.data

# ==============================
# Fetch Raw News Text
# ==============================

def fetch_article_text(raw_news_id):
    result = (
        supabase.table("raw_news")
        .select("title, content")
        .eq("id", raw_news_id)
        .single()
        .execute()
    )

    data = result.data
    return f"{data.get('title','')} {data.get('content','')}".lower()

# ==============================
# Classify Event Severity
# ==============================

def classify_event(text):
    score = 0
    severity = None

    for keyword, weight in HIGH_IMPACT.items():
        if keyword in text:
            score += weight
            severity = "HIGH"

    if not severity:
        for keyword, weight in MEDIUM_IMPACT.items():
            if keyword in text:
                score += weight
                severity = "MEDIUM"

    if not severity:
        for keyword, weight in LOW_IMPACT.items():
            if keyword in text:
                score += weight
                severity = "LOW"

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
    return len(result.data) > 0

# ==============================
# Generate Intraday Signals
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

        if not severity:
            continue

        if score >= 10:
            signal_type = "BUY"
        elif score <= -10:
            signal_type = "SELL"
        else:
            continue

        # Insert Signal
        supabase.table("signals").insert({
            "company_id": company_id,
            "raw_news_id": raw_news_id,
            "signal_type": signal_type,
            "severity": severity,
            "signal_score": score,
            "generated_at": datetime.utcnow().isoformat(),
            "is_active": True
        }).execute()

        # Fetch company name
        company = (
            supabase.table("companies")
            .select("name")
            .eq("id", company_id)
            .single()
            .execute()
        )

        company_name = company.data["name"]

        # Fetch headline
        news = (
            supabase.table("raw_news")
            .select("title")
            .eq("id", raw_news_id)
            .single()
            .execute()
        )

        headline = news.data["title"]

        message = f"""
🚨 *{signal_type} SIGNAL*
Company: *{company_name}*
Severity: *{severity}*
Score: *{score}*

📰 {headline}
"""
def send_telegram_alert(message):
    token = os.getenv("8640786170:AAGTaiZSACLjBM0giXrwy1WS8HN97ge0HIc")
    chat_id = os.getenv("450110004")

    if not token or not chat_id:
        print("Telegram credentials missing.")
        return

    url = f"https://api.telegram.org/bot8640786170:AAGTaiZSACLjBM0giXrwy1WS8HN97ge0HIc/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    for attempt in range(3):  # retry 3 times
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                return
        except Exception as e:
            print(f"Telegram attempt {attempt+1} failed:", e)

# ==============================

import time

if __name__ == "__main__":
    generate_intraday_signals()