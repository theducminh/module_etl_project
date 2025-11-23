import os
import csv
import requests
from datetime import datetime

def copy_into_postgres(local_clean_csv_path, table="fact_temperature", batch_size=500):
    """
    Upsert cleaned CSV into Supabase via REST API (auto upsert on `time` primary key).
    Logs detailed info for invalid rows.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not service_role_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env vars!")

    # Upsert endpoint with on_conflict parameter
    endpoint = f"{supabase_url}/rest/v1/{table}?on_conflict=time"

    rows = []
    with open(local_clean_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                time_val = r.get("time")
                temp_val = r.get("temperature_c")
                if not time_val:
                    print(f"[WARN] Skipping row with empty time: {r}")
                    continue
                # convert timestamp to ISO 8601 UTC
                dt = datetime.fromisoformat(time_val)
                time_iso = dt.astimezone().isoformat()
                rows.append({
                    "time": time_iso,
                    "temperature_c": float(temp_val) if temp_val else None
                })
            except Exception as e:
                print(f"[ERROR] Skipping invalid row {r}: {e}")

    if not rows:
        print("[WARN] CSV empty or all rows invalid → no ingestion")
        return

    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }

    total_rows = len(rows)
    print(f"[INFO] Preparing to upsert {total_rows} rows into {table}")

    for i in range(0, total_rows, batch_size):
        batch = rows[i:i+batch_size]
        response = requests.post(endpoint, json=batch, headers=headers)
        if response.status_code not in (200, 201, 204):
            print("❌ ERROR inserting batch:")
            print("Status:", response.status_code)
            print("Response:", response.text)
            print("Batch rows:", batch)
            raise Exception("Supabase upsert failed")
        print(f"[INFO] Upserted batch {i} → {i+len(batch)} rows successfully")

    print(f"✅ Successfully upserted {total_rows} rows into {table}")
    return total_rows
