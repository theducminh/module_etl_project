# analyze/analyze_house.py

import os
import pandas as pd
import matplotlib.pyplot as plt


def analyze_house(csv_path, output_dir="output_data/house_analysis"):
    """
    Phân tích cơ bản: distribution price, area, price_per_m2,
    lưu ra các ảnh biểu đồ.
    """
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(csv_path)

    if df.empty:
        print("[WARN] Empty house CSV, skip analysis")
        return

    # ensure numeric types
    for c in ["price", "area_m2", "price_per_m2"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # drop NaN for plotting where appropriate
    df_price = df["price"].dropna()
    df_area = df["area_m2"].dropna()
    df_ppm2 = df["price_per_m2"].dropna()

    # Price histogram
    if not df_price.empty:
        plt.figure(figsize=(10,5))
        df_price.plot(kind="hist", bins=40)
        plt.title("Distribution of Listing Prices")
        plt.xlabel("Price (VND)")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "price_distribution.png"))
        plt.close()

    # Area histogram
    if not df_area.empty:
        plt.figure(figsize=(10,5))
        df_area.plot(kind="hist", bins=40)
        plt.title("Distribution of Area (m2)")
        plt.xlabel("Area (m2)")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "area_distribution.png"))
        plt.close()

    # Price per m2 boxplot
    if not df_ppm2.empty:
        plt.figure(figsize=(8,5))
        df_ppm2.plot(kind="box")
        plt.title("Price per m² (boxplot)")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "price_per_m2_boxplot.png"))
        plt.close()

    # Top 10 most expensive listings
    if "price" in df.columns:
        top10 = df.sort_values("price", ascending=False).head(10)
        top10_path = os.path.join(output_dir, "top10_most_expensive.csv")
        top10.to_csv(top10_path, index=False)

    print(f"📊 Analysis completed → {output_dir}")
    return os.path.abspath(output_dir)
