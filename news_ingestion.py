import feedparser
import hashlib
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
# Fetch Active RSS Sources
# ==============================

def fetch_rss_sources():
    result = supabase.table("news_sources") \
        .select("id,name,base_url") \
        .eq("type", "RSS") \
        .eq("is_active", True) \
        .execute()

    return result.data


# ==============================
# Generate Hash (Deduplication)
# ==============================

def generate_hash(title, url):
    raw_string = f"{title}-{url}"
    return hashlib.sha256(raw_string.encode("utf-8")).hexdigest()


# ==============================
# Insert News Article
# ==============================

def insert_article(source_id, title, content, url, published_at):
    hash_signature = generate_hash(title, url)

    # Check duplicate
    existing = supabase.table("raw_news") \
        .select("id") \
        .eq("hash_signature", hash_signature) \
        .execute()

    if existing.data:
        return False  # Already exists

    supabase.table("raw_news").insert({
        "source_id": source_id,
        "title": title,
        "content": content,
        "url": url,
        "published_at": published_at,
        "fetched_at": datetime.utcnow().isoformat(),
        "is_processed": False,
        "hash_signature": hash_signature
    }).execute()

    return True


# ==============================
# Ingest RSS Feed
# ==============================

def ingest_feed(source):
    print(f"Ingesting from {source['name']}...")

    feed = feedparser.parse(source["base_url"])

    inserted_count = 0

    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        summary = entry.get("summary", "").strip()

        if not title or not link:
            continue

        published = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            published = datetime(*entry.published_parsed[:6]).isoformat()

        inserted = insert_article(
            source_id=source["id"],
            title=title,
            content=summary,
            url=link,
            published_at=published
        )

        if inserted:
            inserted_count += 1

    print(f"Inserted {inserted_count} new articles from {source['name']}.")


# ==============================
# Main
# ==============================

def main():
    print("Starting RSS news ingestion...")

    sources = fetch_rss_sources()

    if not sources:
        print("No active RSS sources found.")
        return

    for source in sources:
        ingest_feed(source)

    print("News ingestion completed successfully.")


if __name__ == "__main__":
    main()