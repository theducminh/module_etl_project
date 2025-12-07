# analyze/analyze_crypto.py
import os
import pandas as pd
import matplotlib.pyplot as plt

def analyze_crypto(csv_path, output_dir="output_data/crypto_analysis"):
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(csv_path)
    if df.empty:
        print("[WARN] Empty crypto CSV, skip")
        return

    # Top 20 market cap
    df_top = df.sort_values("market_cap", ascending=False).head(20)

    plt.figure(figsize=(12,5))
    plt.bar(df_top["symbol"], df_top["market_cap"])
    plt.title("Top 20 Crypto by Market Cap")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/top20_marketcap.png")
    plt.close()

    print(f"📊 Analysis done → {output_dir}")
