import os
from datetime import datetime
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
# Keyword Rules (Phase 1)
# ==============================

POSITIVE_KEYWORDS = ["profit", "growth", "surge", "rally", "beat", "upgrade"]
NEGATIVE_KEYWORDS = ["loss", "fall", "decline", "drop", "downgrade", "miss"]

# ==============================
# Fetch Unprocessed News
# ==============================

def fetch_unprocessed_news():
    result = (
        supabase.table("raw_news")
        .select("*")
        .eq("is_processed", False)
        .execute()
    )
    return result.data

# ==============================
# Fetch Companies
# ==============================

def fetch_companies():
    result = (
        supabase.table("companies")
        .select("id,name,symbol")
        .eq("exchange", "NSE")
        .execute()
    )
    return result.data

# ==============================
# Sentiment Scoring
# ==============================

def analyze_sentiment(text):
    text_lower = text.lower()

    positive_hits = sum(word in text_lower for word in POSITIVE_KEYWORDS)
    negative_hits = sum(word in text_lower for word in NEGATIVE_KEYWORDS)

    if positive_hits > negative_hits:
        return "bullish", positive_hits
    elif negative_hits > positive_hits:
        return "bearish", negative_hits
    else:
        return "neutral", 0

# ==============================
# Company Detection
# ==============================

def detect_company(text, companies):
    for company in companies:
        name = company["name"].lower()
        symbol = company["symbol"].lower()

        if name in text or symbol in text:
            return company["id"]

    return None

# ==============================
# Extract Keywords
# ==============================

def extract_keywords(text):
    text_lower = text.lower()
    detected = []

    for word in POSITIVE_KEYWORDS + NEGATIVE_KEYWORDS:
        if word in text_lower:
            detected.append(word)

    return detected

# ==============================
# Process News
# ==============================

def process_news():
    print("Starting news processing...")

    news_items = fetch_unprocessed_news()
    companies = fetch_companies()

    print(f"Found {len(news_items)} unprocessed articles.")

    for article in news_items:
        title = article.get("title", "")
        content = article.get("content", "") or ""

        combined_text = f"{title} {content}".lower()

        company_id = detect_company(combined_text, companies)

        if company_id:
            sentiment, score_hits = analyze_sentiment(combined_text)

            # Only insert meaningful signals
            if score_hits > 0:
                detected_keywords = extract_keywords(combined_text)

                base_score = score_hits * 10
                confidence = min(score_hits * 20, 100)

                supabase.table("processed_events").insert({
                    "raw_news_id": article["id"],
                    "company_id": company_id,
                    "detected_keywords": detected_keywords,
                    "category": "GENERAL",
                    "base_score": base_score,
                    "market_cap_boost": 0,
                    "final_score": base_score,
                    "sentiment": sentiment,
                    "confidence_score": confidence,
                    "processed_at": datetime.utcnow().isoformat()
                }).execute()

        # Always mark article processed
        (
            supabase.table("raw_news")
            .update({"is_processed": True})
            .eq("id", article["id"])
            .execute()
        )

    print("News processing completed.")

if __name__ == "__main__":
    process_news()