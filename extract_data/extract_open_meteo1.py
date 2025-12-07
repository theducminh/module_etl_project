import os, json, datetime, requests, csv

# Tọa độ trung tâm 63 tỉnh/thành Việt Nam (mẫu, bổ sung đủ)
VIETNAM_CITIES = {
    "Hà Nội": (21.028472, 105.854167),
    "Hồ Chí Minh": (10.76262, 106.6822),
    "Đà Nẵng": (16.05441, 108.2022),
    "Hải Phòng": (20.86514, 106.6823),
    "Cần Thơ": (10.03316, 105.7860),
    "Khánh Hòa": (12.23879, 109.1968),
    # ... thêm các tỉnh còn lại
}

def extract_open_meteo_for_city(city, lat, lon):
    today = datetime.date.today().isoformat()
    DOWNLOAD_DIR = f"data_input/open_meteo1/{today}/{city.replace(' ', '_')}"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m"
        f"&forecast_days=7"
        f"&timezone=Asia/Ho_Chi_Minh"
    )

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_json = os.path.join(DOWNLOAD_DIR, f"open_meteo_raw_{ts}.json")
    out_csv = os.path.join(DOWNLOAD_DIR, f"open_meteo_raw_{ts}.csv")

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(data, f)

    times = data.get("hourly", {}).get("time", [])
    temps = data.get("hourly", {}).get("temperature_2m", [])
    with open(out_csv, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["city","latitude","longitude","time","temperature_c"])
        for t, temp in zip(times, temps):
            writer.writerow([city, lat, lon, t, temp])

    print(f"[INFO] Extracted {city}: {out_json}, {out_csv}")
    return os.path.abspath(out_csv)

def extract_open_meteo():
    paths = []
    for city, (lat, lon) in VIETNAM_CITIES.items():
        try:
            path = extract_open_meteo_for_city(city, lat, lon)
            paths.append(path)
        except Exception as e:
            print(f"[ERROR] Failed for {city}: {e}")
    return paths

if __name__ == "__main__":
    csv_paths = extract_open_meteo()
    print("✅ Extracted Open-Meteo data for all cities")
    for p in csv_paths:
        print(p)
