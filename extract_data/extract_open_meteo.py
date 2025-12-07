# extract_data/extract_open_meteo.py
import os, json, datetime, requests


def extract_open_meteo(lat=21.0278, lon=105.8342, start=None, end=None, **kwargs):
    """
    Download hourly temperature time series from Open-Meteo.
    Save JSON + CSV into folder data_input/open_meteo/YYYY-MM-DD.
    Return: absolute path of the CSV (for XCom).
    """

    today = datetime.date.today().isoformat()
    DOWNLOAD_DIR = f"data_input/open_meteo/{today}"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Auto dates
    if start is None:
        start = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    if end is None:
        end = datetime.date.today().isoformat()

    url = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={lat}&longitude={lon}"
    f"&hourly=temperature_2m"
    f"&forecast_days=7"
    f"&timezone=UTC"
)



    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    out_json = os.path.join(DOWNLOAD_DIR, f"open_meteo_raw_{ts}.json")
    out_csv = os.path.join(DOWNLOAD_DIR, f"open_meteo_raw_{ts}.csv")

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(data, f)

    # CSV
    times = data.get("hourly", {}).get("time", [])
    temps = data.get("hourly", {}).get("temperature_2m", [])

    with open(out_csv, "w", encoding="utf-8") as f:
        f.write("time,temperature_c\n")
        for t, temp in zip(times, temps):
            f.write(f"{t},{temp}\n")

    print(f"[INFO] Extracted: {out_json}, {out_csv}")
    path = os.path.abspath(out_csv)
    print(f"[INFO] Extracted CSV path111111: {path}")

    # Return absolute path for Airflow
    return path


if __name__ == "__main__":
    p = extract_open_meteo()
    print("CSV Saved:", p)
    print("✅ Extracted Open-Meteo data")
