from config import supabase

response = supabase.table("companies").select("*").limit(1).execute()

print("RESPONSE:", response)