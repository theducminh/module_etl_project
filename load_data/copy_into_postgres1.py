import os
import csv
import requests
from datetime import datetime, timezone


def copy_into_postgres(local_clean_csv_path, table="fact_traffic", batch_size=500):
    """
    Upsert cleaned CSV into Supabase Postgres via REST API.

    Handles correct data types:
    - time → timestamp
    - lat/lon → float
    - *_time → int
    - speed → float
    - road_closure → boolean
    - location_name, frc → string

    Args:
        local_clean_csv_path (str): Path to cleaned CSV
        table (str): Supabase table name
        batch_size (int): Batch size for insert
    """

    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not service_role_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env vars!")

    # Lưu ý: on_conflict phải là danh sách cột trong bảng Supabase
    endpoint = f"{supabase_url}/rest/v1/{table}?on_conflict=lat,lon"

    # Columns grouping (for type-safe casting)
    float_fields = {"lat", "lon", "confidence", "speed_kmph", "free_flow_speed"}
    int_fields = {"currentTravelTime", "freeFlowTravelTime"}
    bool_fields = {"road_closure"}

    rows = []

    with open(local_clean_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            try:
                # ----- Convert time -----
                time_val = r.get("time")
                if not time_val:
                    print(f"[WARN] Skipping row with empty time: {r}")
                    continue

                # Đảm bảo datetime hợp lệ, chuyển sang UTC ISO format
                dt = datetime.fromisoformat(time_val)
                time_iso = dt.astimezone(timezone.utc).isoformat()

                row_data = {"time": time_iso}

                # ----- Convert other fields -----
                for k, v in r.items():
                    if k == "time":
                        continue

                    if v in ("", None):
                        row_data[k] = None
                        continue

                    # INT fields
                    if k in int_fields:
                        try:
                            row_data[k] = int(v)
                        except ValueError:
                            print(f"[WARN] Invalid INT for {k}: {v}")
                            row_data[k] = None
                        continue

                    # FLOAT fields
                    if k in float_fields:
                        try:
                            row_data[k] = float(v)
                        except ValueError:
                            print(f"[WARN] Invalid FLOAT for {k}: {v}")
                            row_data[k] = None
                        continue

                    # BOOLEAN fields
                    if k in bool_fields:
                        row_data[k] = str(v).lower() in ("true", "1", "yes")
                        continue

                    # STRING fields (location_name, frc, etc.)
                    row_data[k] = v

                rows.append(row_data)

            except Exception as e:
                print(f"[ERROR] Skipping invalid row {r}: {e}")

    if not rows:
        print("[WARN] CSV empty or all rows invalid → no ingestion")
        return 0

    # ----- Supabase headers -----
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }

    total_rows = len(rows)
    print(f"[INFO] Preparing to upsert {total_rows} rows into {table}")

    # ----- Batch upsert -----
    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]

        try:
            response = requests.post(endpoint, json=batch, headers=headers)
            response.raise_for_status()  # Tự động raise nếu status_code >= 400
        except requests.exceptions.RequestException as e:
            print(f"❌ ERROR inserting batch {i}-{i+len(batch)}: {e}")
            print("Batch data sample:", batch[:2])
            raise

        print(f"[INFO] Upserted batch {i} → {i + len(batch)} rows")

    print(f"✅ Successfully upserted {total_rows} rows into {table}")
    return total_rows


if __name__ == "__main__":
    csv_path = "output_data/traffic_clean/traffic_clean.csv"
    copy_into_postgres(csv_path, table="fact_traffic")
