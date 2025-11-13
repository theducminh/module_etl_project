import pandas as pd
import os
from utils.supabase_client import get_supabase_client
from utils.config import TABLES

def load_fao_supabase():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    OUTPUT_DIR = os.path.join(BASE_DIR, "output_data", "fao_long_cleaned")

    csv_files = [os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("⚠️ Không tìm thấy file output nào để tải lên Supabase")
    
    file_path = csv_files[0]
    df = pd.read_csv(file_path)
    print(f"[INFO] Đang tải {len(df)} dòng vào Supabase...")

    supabase = get_supabase_client()
    batch_size = 500
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size].to_dict(orient="records")
        supabase.table(table_name=TABLES["fao"]).insert(batch).execute()

    print(f"✅ Đã tải thành công {len(df)} dòng vào bảng {TABLES['fao']}")