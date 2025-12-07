import os
import pandas as pd
from datetime import datetime, timezone


def clean_house(raw_path, output_dir="output_data/house_clean"):
    """
    Transform CSV thô từ bước extract thành CSV sạch.
    - Chuẩn hoá cột
    - Bổ sung price_million, crawled_at
    - Đảm bảo đúng schema cho load lên Fact Table
    """

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"RAW CSV not found: {raw_path}")

    os.makedirs(output_dir, exist_ok=True)

    print(f"🔄 Loading RAW CSV: {raw_path}")
    df = pd.read_csv(raw_path)

    # --- Chuẩn hóa tên cột ---
    rename_map = {
        "area": "area_m2",
        "region_name": "region",
        "area_name": "district",
        "ward_name": "ward",
        "street_name": "street",
        "list_id": "id"
    }
    df.rename(columns=rename_map, inplace=True)

    # --- Chuẩn hóa giá ---
    if "price" in df.columns:
        df["price_million"] = df["price"].apply(
            lambda x: x / 1_000_000 if pd.notnull(x) else None
        )
    else:
        df["price_million"] = None

    # --- Bổ sung timestamp ---
    df["crawled_at"] = datetime.now(timezone.utc).isoformat()

    # --- Chuẩn hóa số ảnh ---
    if "images" in df.columns:
        df["images_count"] = df["images"].apply(
            lambda x: len(eval(x)) if isinstance(x, str) and x.startswith("[") else None
        )
    else:
        df["images_count"] = None

    # --- Đảm bảo đúng schema ---
    final_cols = [
        "id",
        "title",
        "description",
        "price",
        "price_million",
        "area_m2",
        "price_per_m2",
        "region",
        "district",
        "ward",
        "street",
        "lat",
        "lng",
        "property_type",
        "category",
        "post_time",
        "images_count",
        "crawled_at"
    ]

    for col in final_cols:
        if col not in df.columns:
            df[col] = None

    df = df[final_cols]

    # --- Save CSV clean ---
    clean_filename = os.path.basename(raw_path).replace(".csv", "_clean.csv")
    clean_path = os.path.join(output_dir, clean_filename)

    df.to_csv(clean_path, index=False, encoding="utf-8")
    print(f"✅ TRANSFORM DONE → {clean_path}")

    return clean_path
