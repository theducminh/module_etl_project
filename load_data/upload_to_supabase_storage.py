# load_data/upload_to_supabase_storage.py
import os
import sys
import glob
import datetime

# Fix Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.supabase_client import get_supabase_client


def upload_to_storage(local_path, bucket="datalake-open-meteo", dest_folder="raw"):
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"❌ File not found: {local_path}")

    supabase = get_supabase_client()
    filename = os.path.basename(local_path)
    dest = f"{dest_folder}/{filename}"

    with open(local_path, "rb") as f:
        supabase.storage.from_(bucket).upload(dest, f)

    print(f"[INFO] Uploaded {local_path} -> {bucket}/{dest}")
    return f"{bucket}/{dest}"


if __name__ == "__main__":
    print("⚠ This module should not be run directly")
