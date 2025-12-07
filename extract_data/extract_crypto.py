# extract_data/extract_crypto.py
import os
import json
from datetime import datetime, timezone
from curl_cffi import requests

def extract_crypto(limit=500, output_dir="data_input/crypto_raw"):
    os.makedirs(output_dir, exist_ok=True)

    url = f"https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": limit,
        "page": 1,
        "sparkline": False
    }

    print(f"[INFO] Fetching {limit} crypto records from CoinGecko")

    resp = requests.get(
        url,
        params=params,
        impersonate="chrome120",
        timeout=20,
    )

    data = resp.json()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = f"{output_dir}/crypto_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved crypto raw → {file_path}")
    return file_path

if __name__ == "__main__":
    extract_crypto(limit=500, output_dir="data_input/crypto_raw")
    print("CS")
