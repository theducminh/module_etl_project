#extract_data/crawl_data_fao.py
import os
import glob
import shutil
import datetime

def extract_fao(**kwargs):
    """
    Trả về đường dẫn tới CSV demo. Nếu muốn crawl thật thì cần token hợp lệ.
    """
    DOWNLOAD_DIR = "data_input/data_fao"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Kiểm tra có file demo sẵn không, nếu không thì copy một file mẫu
    demo_file = os.path.join(DOWNLOAD_DIR, "fao_demo_sample.csv")
    if not os.path.exists(demo_file):
        # Tạo một CSV demo nhỏ trực tiếp
        with open(demo_file, "w", encoding="utf-8") as f:
            f.write(
                "Country Name En,Unit Name,2023 value,2023 flag,2022 value,2022 flag\n"
                "China,Tonnes - live weight,10000,E,9500,E\n"
                "India,Tonnes - live weight,8000,E,7800,E\n"
                "Vietnam,Tonnes - live weight,5000,E,4800,E\n"
            )

    # Copy file với timestamp để mô phỏng "download mới"
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(DOWNLOAD_DIR, f"fao_demo_{timestamp}.csv")
    shutil.copy(demo_file, file_path)

    print(f"[INFO] Using demo CSV: {file_path}")
    return file_path
if __name__ == "__main__":
    csv_file = extract_fao()
    print(f"✅ Extracted file: {csv_file}")