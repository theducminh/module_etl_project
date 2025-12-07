#load_data/upload_to_supabase_storage1.py
import os
import sys
from utils.supabase_client import get_supabase_client

def upload_to_storage(local_path, bucket="datalake-traffic", dest_folder="raw"):
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"❌ File not found: {local_path}")

    supabase = get_supabase_client()
    filename = os.path.basename(local_path)
    dest = f"{dest_folder}/{filename}"

    with open(local_path, "rb") as f:
        supabase.storage.from_(bucket).upload(dest, f)

    print(f"[INFO] Uploaded {local_path} -> {bucket}/{dest}")
    return f"{bucket}/{dest}"

# Không cần chạy trực tiếp
if __name__ == "__main__":
    print("⚠ This module should not be run directly")
