# extract_data/crawl_data_fao.py
import requests, os, datetime

def extract_fao():
    DOWNLOAD_DIR = "data_input/data_fao"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    URL = "https://www.fao.org/fishery/services/statistics/api/data/download/query/aquaculture_quantity/en"
    headers = {"Content-Type": "application/json"}

    years = [str(y) for y in range(2023, 1949, -1)]
    payload = {
        "aggregationType": "sum",
        "disableSymbol": "false",
        "includeNullValues": "true",
        "grouped": True,
        "rows": [{"field": "country_un_code", "group": "COUNTRY", "groupField": "name_en"}],
        "columns": [{"field": "year", "values": years, "condition": "In", "sort": "desc"}],
        "filters": []
    }

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(DOWNLOAD_DIR, f"aquaculture_data_{timestamp}.csv")

    print("[INFO] Gửi request đến FAO API...")
    res = requests.post(URL, json=payload, headers=headers)

    if res.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(res.content)
        print(f"✅ Dữ liệu được lưu tại: {file_path}")
    else:
        print(f"❌ Lỗi {res.status_code}: {res.text}")
