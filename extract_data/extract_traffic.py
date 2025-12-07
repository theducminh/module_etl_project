# extract_data/extract_traffic.py

import os
import json
import datetime
import requests
import csv
import re
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("TOMTOM_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing TOMTOM_API_KEY env var!")

# Danh sách giao lộ Hà Nội (giữ nguyên, có thể mở rộng)
HANOI_INTERSECTIONS = [ {"name": "Ngã Tư Trần Duy Hưng - Khuất Duy Tiến", "lat": 21.0080, "lon": 105.7990}, {"name": "Ngã Tư Hoàng Cầu - Thái Hà", "lat": 21.0110, "lon": 105.8205}, {"name": "Ngã Tư Phạm Hùng - Mễ Trì", "lat": 20.9955, "lon": 105.7805}, {"name": "Ngã Tư Hoàng Mai - Kim Đồng", "lat": 20.9785, "lon": 105.8630}, {"name": "Ngã Tư Lê Văn Lương - Khuất Duy Tiến", "lat": 20.9965, "lon": 105.8120}, {"name": "Ngã Tư Tây Sơn - Chùa Bộc", "lat": 21.0125, "lon": 105.8275}, {"name": "Ngã Tư Trần Khát Chân - Giải Phóng", "lat": 21.0005, "lon": 105.8530}, {"name": "Ngã Tư Liễu Giai - Đào Tấn", "lat": 21.0395, "lon": 105.8210}, {"name": "Ngã Tư Phan Chu Trinh - Hàng Bài", "lat": 21.0283, "lon": 105.8540}, {"name": "Ngã Tư Trần Phú - Nguyễn Chí Thanh", "lat": 21.0220, "lon": 105.8190}, {"name": "Ngã Tư Cầu Giấy - Duy Tân", "lat": 21.0380, "lon": 105.7870}, {"name": "Ngã Tư Xuân Thủy - Nguyễn Phong Sắc", "lat": 21.0360, "lon": 105.7920}, {"name": "Ngã Tư Nguyễn Văn Cừ - Long Biên", "lat": 21.0445, "lon": 105.8935}, {"name": "Ngã Tư Hồ Tùng Mậu - Phạm Văn Đồng", "lat": 21.0505, "lon": 105.7900}, {"name": "Ngã Tư Mai Dịch - Cầu Giấy", "lat": 21.0355, "lon": 105.7865}, {"name": "Ngã Tư Hoàng Quốc Việt - Phạm Văn Đồng", "lat": 21.0565, "lon": 105.7910}, {"name": "Ngã Tư Vọng - Giải Phóng", "lat": 21.0015, "lon": 105.8570}, {"name": "Ngã Tư Kim Liên - Xã Đàn", "lat": 21.0140, "lon": 105.8300}, {"name": "Ngã Tư Láng - Thái Thịnh", "lat": 21.0160, "lon": 105.8185}, {"name": "Ngã Tư Trần Phú - Láng Hạ", "lat": 21.0225, "lon": 105.8180}, {"name": "Ngã Tư Giải Phóng - Đại Cồ Việt", "lat": 21.0010, "lon": 105.8510}, {"name": "Ngã Tư Thanh Nhàn - Bạch Mai", "lat": 21.0025, "lon": 105.8550}, {"name": "Ngã Tư Hàng Khay - Hàng Bài", "lat": 21.0286, "lon": 105.8545}, {"name": "Ngã Tư Láng Hạ - Thái Hà", "lat": 21.0175, "lon": 105.8190}, {"name": "Ngã Tư Điện Biên Phủ - Trần Phú", "lat": 21.0240, "lon": 105.8220}, {"name": "Ngã Tư Điện Biên Phủ - Cửa Nam", "lat": 21.0320, "lon": 105.8215}, {"name": "Ngã Tư Nguyễn Chí Thanh - Láng Hạ", "lat": 21.0210, "lon": 105.8175}, {"name": "Ngã Tư Lê Duẩn - Nguyễn Thái Học", "lat": 21.0330, "lon": 105.8390}, {"name": "Ngã Tư Nguyễn Trãi - Khuất Duy Tiến", "lat": 20.9985, "lon": 105.8155}, {"name": "Ngã Tư Lê Trọng Tấn - Quang Trung", "lat": 21.0060, "lon": 105.8135}, {"name": "Ngã Tư Giáp Bát - Lò Đúc", "lat": 21.0000, "lon": 105.8470}, {"name": "Ngã Tư Hoàng Mai - Giải Phóng", "lat": 20.9775, "lon": 105.8600}, {"name": "Ngã Tư Minh Khai - Trương Định", "lat": 21.0050, "lon": 105.8710}, {"name": "Ngã Tư Cầu Giấy - Xuân Thủy", "lat": 21.0350, "lon": 105.7910}, {"name": "Ngã Tư Trần Duy Hưng - Khuất Duy Tiến", "lat": 21.0080, "lon": 105.7990}, {"name": "Ngã Tư Hoàng Cầu - Thái Hà", "lat": 21.0110, "lon": 105.8205}, {"name": "Ngã Tư Phạm Hùng - Mễ Trì", "lat": 20.9955, "lon": 105.7805}, {"name": "Ngã Tư Hoàng Mai - Kim Đồng", "lat": 20.9785, "lon": 105.8630}, {"name": "Ngã Tư Lê Văn Lương - Khuất Duy Tiến", "lat": 20.9965, "lon": 105.8120}, {"name": "Ngã Tư Tây Sơn - Chùa Bộc", "lat": 21.0125, "lon": 105.8275}, {"name": "Ngã Tư Trần Khát Chân - Giải Phóng", "lat": 21.0005, "lon": 105.8530}, {"name": "Ngã Tư Liễu Giai - Đào Tấn", "lat": 21.0395, "lon": 105.8210}, {"name": "Ngã Tư Phan Chu Trinh - Hàng Bài", "lat": 21.0283, "lon": 105.8540}, {"name": "Ngã Tư Trần Phú - Nguyễn Chí Thanh", "lat": 21.0220, "lon": 105.8190}, {"name": "Ngã Tư Cầu Giấy - Duy Tân", "lat": 21.0380, "lon": 105.7870}, {"name": "Ngã Tư Xuân Thủy - Nguyễn Phong Sắc", "lat": 21.0360, "lon": 105.7920}, {"name": "Ngã Tư Nguyễn Văn Cừ - Long Biên", "lat": 21.0445, "lon": 105.8935}, {"name": "Ngã Tư Hồ Tùng Mậu - Phạm Văn Đồng", "lat": 21.0505, "lon": 105.7900}, {"name": "Ngã Tư Mai Dịch - Cầu Giấy", "lat": 21.0355, "lon": 105.7865}, {"name": "Ngã Tư Hoàng Quốc Việt - Phạm Văn Đồng", "lat": 21.0565, "lon": 105.7910}, {"name": "Ngã Tư Vọng - Giải Phóng", "lat": 21.0015, "lon": 105.8570}, {"name": "Ngã Tư Kim Liên - Xã Đàn", "lat": 21.0140, "lon": 105.8300}, {"name": "Ngã Tư Láng - Thái Thịnh", "lat": 21.0160, "lon": 105.8185}, {"name": "Ngã Tư Trần Phú - Láng Hạ", "lat": 21.0225, "lon": 105.8180}, {"name": "Ngã Tư Giải Phóng - Đại Cồ Việt", "lat": 21.0010, "lon": 105.8510}, {"name": "Ngã Tư Thanh Nhàn - Bạch Mai", "lat": 21.0025, "lon": 105.8550}, {"name": "Ngã Tư Hàng Khay - Hàng Bài", "lat": 21.0286, "lon": 105.8545}, {"name": "Ngã Tư Láng Hạ - Thái Hà", "lat": 21.0175, "lon": 105.8190}, {"name": "Ngã Tư Điện Biên Phủ - Trần Phú", "lat": 21.0240, "lon": 105.8220}, {"name": "Ngã Tư Điện Biên Phủ - Cửa Nam", "lat": 21.0320, "lon": 105.8215}, {"name": "Ngã Tư Nguyễn Chí Thanh - Láng Hạ", "lat": 21.0210, "lon": 105.8175}, {"name": "Ngã Tư Lê Duẩn - Nguyễn Thái Học", "lat": 21.0330, "lon": 105.8390}, {"name": "Ngã Tư Nguyễn Trãi - Khuất Duy Tiến", "lat": 20.9985, "lon": 105.8155}, {"name": "Ngã Tư Lê Trọng Tấn - Quang Trung", "lat": 21.0060, "lon": 105.8135}, {"name": "Ngã Tư Giáp Bát - Lò Đúc", "lat": 21.0000, "lon": 105.8470}, {"name": "Ngã Tư Hoàng Mai - Giải Phóng", "lat": 20.9775, "lon": 105.8600}, {"name": "Ngã Tư Minh Khai - Trương Định", "lat": 21.0050, "lon": 105.8710}, {"name": "Ngã Tư Cầu Giấy - Xuân Thủy", "lat": 21.0350, "lon": 105.7910}, {"name": "IC Pháp Vân - Vành đai 3 Pháp Vân - Cầu Giẽ", "lat": 20.9875, "lon": 105.8490}, {"name": "Cầu Thanh Trì giao với Vành đai 3", "lat": 20.9983, "lon": 105.8600}, {"name": "IC Cổ Linh - Gia Lâm Đông Hà Nội", "lat": 21.0280, "lon": 105.9000}, {"name": "Cầu Nhật Tân cửa ngõ Tây Bắc Hà Nội", "lat": 21.0600, "lon": 105.8385}, {"name": "Cầu Thăng Long - cửa ngõ Nội Bài Tây Bắc", "lat": 21.0940, "lon": 105.7650}, {"name": "IC Mai Dịch - Vành đai 3 / QL32 ( Hướng Tây Bắc)", "lat": 21.0450, "lon": 105.7700}, {"name": "Nút giao Vành đai 3 - Nguyễn Xiển Linh Đàm (hướng Đông Nam)", "lat": 20.9750, "lon": 105.8410}, {"name": "Nút giao Vành đai 3 - Mỹ Đình Nhổn (hướng Tây Bắc)", "lat": 21.0370, "lon": 105.7830}, {"name": "Nút giao Vành đai 3 - Tây Tựu Bắc Từ Liêm", "lat": 21.0630, "lon": 105.7660}, {"name": "Nút giao Vành đai 3.5 - Đại lộ Thăng Long Hoài Đức", "lat": 21.0270, "lon": 105.7500}, {"name": "IC Hoài Đức - Vành đai 3.5 Quốc lộ 32", "lat": 21.0350, "lon": 105.7460}, {"name": "Cửa ngõ Đông Nam - Gia Lâm cầu Vĩnh Tuy Cổ Linh", "lat": 21.0370, "lon": 105.9100}, {"name": "Cửa ngõ phía Đông - Gia Lâm QL5B Hải Phòng", "lat": 21.0440, "lon": 106.0000}, {"name": "Cửa ngõ phía Bắc - Đông Anh Nội Bài Airport (qua Thăng Long Bridge)", "lat": 21.1200, "lon": 105.7800}, {"name": "Cửa ngõ phía Nam - Thanh Trì Pháp Vân IC", "lat": 20.9800, "lon": 105.8400}, ]

def safe_filename(name: str) -> str:
    """Chuyển tên giao lộ thành tên file hợp lệ: thay tất cả ký tự / \ : * ? " < > | bằng _"""
    name = re.sub(r'[\\/:"*?<>|]+', "_", name)
    name = re.sub(r"\s+", "_", name)  # thay khoảng trắng bằng _
    return name

def fetch_segment(lat, lon):
    """Gọi TomTom Traffic Flow API cho 1 tọa độ"""
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        f"?key={API_KEY}&point={lat},{lon}&unit=KMPH"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("flowSegmentData", {})

def save_to_csv(filename, rows):
    """Lưu danh sách dict vào CSV"""
    if not rows:
        print("[WARN] No data to save.")
        return
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

def extract_traffic(intersections=None):
    """Crawl traffic cho nhiều giao lộ Hà Nội và lưu JSON + CSV"""
    if intersections is None:
        intersections = HANOI_INTERSECTIONS

    today = datetime.date.today().isoformat()
    DOWNLOAD_DIR = f"data_input/traffic/{today}"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    timestamp = datetime.datetime.utcnow().isoformat()
    rows = []

    for inter in intersections:
        try:
            data = fetch_segment(inter["lat"], inter["lon"])
        except Exception as e:
            print(f"[ERROR] Failed to fetch {inter['name']}: {e}")
            continue

        # Lưu dòng CSV
        row = {
            "time": timestamp,
            "intersection": inter["name"],
            "lat": inter["lat"],
            "lon": inter["lon"],
            "currentSpeed": data.get("currentSpeed", 0),
            "freeFlowSpeed": data.get("freeFlowSpeed", 0),
            "currentTravelTime": data.get("currentTravelTime", 0),
            "freeFlowTravelTime": data.get("freeFlowTravelTime", 0),
            "confidence": data.get("confidence", 0),
            "roadClosure": data.get("roadClosure", False)
        }
        rows.append(row)

        # Tạo file JSON với tên hợp lệ
        filename_safe = safe_filename(inter["name"])
        out_json = os.path.join(
            DOWNLOAD_DIR, f"traffic_{filename_safe}_{timestamp.replace(':','')}.json"
        )
        os.makedirs(os.path.dirname(out_json), exist_ok=True)
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # Lưu CSV tổng hợp
    out_csv = os.path.join(DOWNLOAD_DIR, f"hanoi_traffic_{timestamp.replace(':','')}.csv")
    save_to_csv(out_csv, rows)

    print(f"[INFO] Extracted {len(rows)} intersections to CSV: {out_csv}")
    return os.path.abspath(out_csv)

if __name__ == "__main__":
    path = extract_traffic()
    print("CSV Saved:", path)
