import os, csv, requests
from datetime import datetime

def copy_into_postgres(local_clean_csv_path, table="fact_temperature1", batch_size=500):
    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    endpoint = f"{supabase_url}/rest/v1/{table}?on_conflict=city,time"

    rows = []
    with open(local_clean_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                city = r.get("city")
                lat = float(r.get("latitude"))
                lon = float(r.get("longitude"))
                time_val = r.get("time")
                temp_val = r.get("temperature_c")
                if not city or not time_val:
                    continue
                dt = datetime.fromisoformat(time_val)
                rows.append({
                    "city": city,
                    "latitude": lat,
                    "longitude": lon,
                    "time": dt.astimezone().isoformat(),
                    "temperature_c": float(temp_val) if temp_val else None
                })
            except Exception as e:
                print(f"[ERROR] Skipping invalid row {r}: {e}")

    if not rows:
        print("CSV empty → nothing to insert")
        return

    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        resp = requests.post(endpoint, json=batch, headers=headers)
        if resp.status_code not in (200, 201, 204):
            raise Exception(f"Supabase upsert failed: {resp.text}")
        print(f"[INFO] Upserted batch {i} → {i+len(batch)} rows")

    print(f"✅ Upserted {len(rows)} rows into {table}")
    return len(rows)
