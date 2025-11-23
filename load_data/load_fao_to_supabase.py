# load_data/load_fao_to_supabase.py
import pandas as pd
from supabase import create_client, Client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def load_fao_supabase(input_file=None, **kwargs):
    """
    Pull transformed CSV path from XCom, load to Supabase
    """
    if input_file is None:
        ti = kwargs['ti']
        input_file = ti.xcom_pull(task_ids="transform_fao")

    df = pd.read_csv(input_file)

    print(f"[INFO] Loading {len(df)} records to Supabase...")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    table_name = "fao_long"

    for _, row in df.iterrows():
        data = {
            "country": row["country"],
            "unit": row["unit"],
            "year": int(row["year"]),
            "value": float(row["value"])
        }
        supabase.table(table_name).insert(data).execute()

    print("✅ Load hoàn tất")
    return len(df)  # push record count to XCom

if __name__ == "__main__":
    load_fao_supabase("output_data/fao_long/fao_long.csv")
    print("✅ Loaded FAO data to Supabase")