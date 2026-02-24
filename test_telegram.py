import os
import requests
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("TELEGRAM_BOT_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")

url = f"https://api.telegram.org/bot{token}/sendMessage"

payload = {
    "chat_id": chat_id,
    "text": "🚀 NAVI Intraday Scanner is LIVE!",
    "parse_mode": "Markdown"
}

try:
    response = requests.post(url, json=payload, timeout=10)
    print(response.json())
except Exception as e:
    print("Error:", e)