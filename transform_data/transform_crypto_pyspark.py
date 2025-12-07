# transform_data/transform_crypto.py
import os
import json
import csv
from datetime import datetime, timezone

def transform_crypto(raw_path, output_dir="output_data/crypto_clean"):
    os.makedirs(output_dir, exist_ok=True)

    clean_filename = os.path.basename(raw_path).replace(".json", ".csv")
    clean_path = f"{output_dir}/{clean_filename}"

    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    rows = []

    for item in raw:
        rows.append({
            "id": item.get("id"),
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "current_price": item.get("current_price"),
            "market_cap": item.get("market_cap"),
            "price_change_24h": item.get("price_change_24h"),
            "price_change_pct_24h": item.get("price_change_percentage_24h"),
            "total_volume": item.get("total_volume"),
            "last_updated": item.get("last_updated")
        })

    with open(clean_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id", "symbol", "name",
                "current_price", "market_cap",
                "price_change_24h", "price_change_pct_24h",
                "total_volume", "last_updated"
            ]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Clean CSV saved → {clean_path}")
    return clean_path
