import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

print("Loaded URL:", url)
print("Loaded KEY exists:", key is not None)

if not url:
    raise Exception("SUPABASE_URL not loaded")

supabase = create_client(url, key)