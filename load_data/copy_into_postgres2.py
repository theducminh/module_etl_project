# load_data/copy_into_postgres_crypto.py
import os
import csv
import requests
from datetime import datetime, timezone

def copy_into_postgres(csv_path, table="fact_crypto", batch_size=500):

    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not service_role_key:
        raise RuntimeError("Missing Supabase env vars")

    endpoint = f"{supabase_url}/rest/v1/{table}?on_conflict=id"


    float_fields = {
        "current_price",
        "market_cap",
        "price_change_24h",
        "price_change_pct_24h",
        "total_volume"
    }

    rows = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            try:
                # Convert last_updated to UTC ISO
                dt = datetime.fromisoformat(r["last_updated"].replace("Z", "+00:00"))
                last_updated_iso = dt.astimezone(timezone.utc).isoformat()

                row = {
                    "id": r["id"],
                    "symbol": r["symbol"],
                    "name": r["name"],
                    "last_updated": last_updated_iso
                }

                for k, v in r.items():
                    if k in ("id", "symbol", "name", "last_updated"):
                        continue

                    if v in ("", None):
                        row[k] = None
                        continue

                    if k in float_fields:
                        row[k] = float(v)
                        continue

                rows.append(row)

            except Exception as e:
                print(f"[WARN] Bad row skipped: {e}")

    print(f"[INFO] Preparing to upsert {len(rows)} crypto rows")

    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        res = requests.post(endpoint, json=batch, headers=headers)
        if res.status_code not in (200, 201, 204):
            print("[ERROR] Insert failed:", res.text)
            raise Exception("Batch insert error")
        print(f"[INFO] Inserted rows {i} - {i+len(batch)}")

    print("✅ Crypto data inserted successfully")

    return True
