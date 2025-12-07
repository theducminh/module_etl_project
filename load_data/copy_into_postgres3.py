import os
import csv
import requests
from datetime import datetime, timezone


def copy_into_postgres(csv_path, table="fact_house_listings", batch_size=500):
    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not service_role_key:
        raise RuntimeError("Missing Supabase env vars")

    endpoint = f"{supabase_url}/rest/v1/{table}?on_conflict=id"

    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            try:
                # ---- Parse timestamp ----
                def parse_ts(val):
                    if not val or val == "":
                        return None
                    try:
                        return datetime.fromtimestamp(int(float(val)), tz=timezone.utc).isoformat()
                    except:
                        try:
                            return datetime.fromisoformat(val).astimezone(timezone.utc).isoformat()
                        except:
                            return None

                row = {
                    "id": int(r["id"]) if r.get("id") else None,
                    "title": r.get("title"),
                    "description": r.get("description"),
                    "price": float(r["price"]) if r.get("price") else None,
                    "price_million": float(r["price_million"]) if r.get("price_million") else None,
                    "area_m2": float(r["area_m2"]) if r.get("area_m2") else None,
                    "price_per_m2": float(r["price_per_m2"]) if r.get("price_per_m2") else None,
                    "region": r.get("region"),
                    "district": r.get("district"),
                    "ward": r.get("ward"),
                    "street": r.get("street"),
                    "lat": float(r["lat"]) if r.get("lat") else None,
                    "lng": float(r["lng"]) if r.get("lng") else None,
                    "property_type": r.get("property_type"),
                    "category": r.get("category"),
                    "post_time": parse_ts(r.get("post_time")),
                    "images_count": int(r["images_count"]) if r.get("images_count") else None,
                    "crawled_at": parse_ts(r.get("crawled_at")),
                }

                rows.append(row)

            except Exception as e:
                print(f"[WARN] Skipping bad row: {e}")

    print(f"[INFO] Preparing to upsert {len(rows)} rows into {table}")

    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        res = requests.post(endpoint, json=batch, headers=headers)

        if res.status_code not in (200, 201, 204):
            print("[ERROR] Insert failed:", res.status_code, res.text)
            raise Exception(f"Batch insert error: {res.status_code} {res.text}")

        print(f"[INFO] Inserted rows {i} → {i+len(batch)}")

    print("✅ DONE: House data inserted successfully")
