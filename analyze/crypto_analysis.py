# analyze/analyze_crypto.py
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_crypto(csv_path, output_dir="output_data/crypto_analysis"):
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(csv_path)
    if df.empty:
        print("[CẢNH BÁO] File CSV rỗng, bỏ qua")
        return

    # Ép kiểu số và dọn dữ liệu
    num_cols = ["current_price", "market_cap", "price_change_24h", "price_change_pct_24h", "total_volume"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper()

    sns.set_style("whitegrid")
    plt.rcParams['figure.facecolor'] = 'white'

    # ===== 1) Top 20 Vốn Hóa (1 ảnh) =====
    df_top = (
        df[["symbol", "market_cap"]]
        .dropna(subset=["market_cap"])
        .sort_values("market_cap", ascending=False)
        .head(20)
    )
    if not df_top.empty:
        plt.figure(figsize=(14, 6))
        colors = plt.cm.viridis(np.linspace(0, 1, len(df_top)))
        plt.bar(df_top["symbol"], df_top["market_cap"] / 1e9, color=colors)
        plt.xlabel("Tiền điện tử", fontsize=12, fontweight='bold')
        plt.ylabel("Vốn hóa (Tỷ USD)", fontsize=12, fontweight='bold')
        plt.title("Top 20 Tiền Điện Tử Theo Vốn Hóa Thị Trường", fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/01_top20_von_hoa.png", dpi=300)
        plt.close()
        print("Đã tạo: 01_top20_von_hoa.png")
    else:
        print("[BỎ QUA] Thiếu dữ liệu cho Top 20 Vốn Hóa")

    # ===== 2) Phân phối biến động 24h (Histogram + KDE overlay, 1 ảnh) =====
    s = pd.to_numeric(df.get("price_change_pct_24h", pd.Series(dtype=float)), errors="coerce")
    s = s[(s >= -50) & (s <= 50)].dropna()
    if len(s) > 1:
        plt.figure(figsize=(12, 6))
        bins = min(50, max(15, int(np.sqrt(len(s)))))
        plt.hist(s, bins=bins, color='steelblue', edgecolor='black', alpha=0.65, density=True, label="Histogram")
        try:
            sns.kdeplot(x=s, color='coral', linewidth=2, label="KDE")
        except Exception:
            pass
        plt.axvline(0, color='red', linestyle='--', linewidth=1.5, label='0%')
        plt.xlabel("Biến động giá 24h (%)", fontsize=11, fontweight='bold')
        plt.ylabel("Mật độ", fontsize=11, fontweight='bold')
        plt.title("Phân Phối Biến Động Giá 24h", fontsize=13, fontweight='bold')
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/02_phan_phoi_bien_dong_gia.png", dpi=300)
        plt.close()
        print("Đã tạo: 02_phan_phoi_bien_dong_gia.png")
    else:
        print("[BỎ QUA] Thiếu dữ liệu cho Phân phối biến động 24h")

    # ===== 3) Market Cap vs Volume (Scatter, 1 ảnh) =====
    cols_scatter = ["market_cap", "total_volume", "price_change_pct_24h", "symbol"]
    
    df_scatter = df.dropna(subset=cols_scatter)
    if len(df_scatter) > 1:
        plt.figure(figsize=(12, 7))
        scatter = plt.scatter(
            df_scatter['market_cap'] / 1e9,
            df_scatter['total_volume'] / 1e9,
            s=100,
            c=df_scatter['price_change_pct_24h'],
            cmap='RdYlGn',
            alpha=0.6,
            edgecolors='black',
            linewidth=0.5
        )
        
        # Gán nhãn top 10
        for idx, row in df_scatter.head(10).iterrows():
            plt.annotate(
                row['symbol'].upper(),
                (row['market_cap'] / 1e9, row['total_volume'] / 1e9),
                fontsize=9,
                fontweight='bold',
                xytext=(5, 5),
                textcoords='offset points'
            )
        
        plt.colorbar(scatter, label='Biến động giá 24h (%)')
        plt.xlabel("Vốn hóa (Tỷ USD)", fontsize=12, fontweight='bold')
        plt.ylabel("Khối lượng giao dịch 24h (Tỷ USD)", fontsize=12, fontweight='bold')
        plt.title("Mối Quan Hệ Vốn Hóa và Khối Lượng Giao Dịch (Top 100)", fontsize=14, fontweight='bold')
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/03_khoi_luong_vs_von_hoa.png", dpi=300)
        plt.close()
        print("Đã tạo: 03_khoi_luong_vs_von_hoa.png")
    else:
        print("[BỎ QUA] Thiếu dữ liệu cho biểu đồ Khối Lượng vs Vốn Hóa")

    # ===== 4) Top Tăng & Giảm Mạnh (Horizontal Bar, 1 ảnh) =====
    top_gainers = df.nlargest(10, 'price_change_pct_24h')[['symbol', 'name', 'price_change_pct_24h']]
    top_losers = df.nsmallest(10, 'price_change_pct_24h')[['symbol', 'name', 'price_change_pct_24h']]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Top tăng
    if not top_gainers.empty:
        ax1.barh(top_gainers['symbol'], top_gainers['price_change_pct_24h'], color='green', alpha=0.7)
        ax1.set_xlabel("Biến động giá (%)", fontsize=11, fontweight='bold')
        ax1.set_title("Top 10 Tăng Giá Mạnh Nhất (24h)", fontsize=13, fontweight='bold', color='darkgreen')
        ax1.grid(axis='x', alpha=0.3)
    else:
        print("[BỎ QUA] Thiếu dữ liệu cho Top Tăng Giá")

    # Top giảm
    if not top_losers.empty:
        ax2.barh(top_losers['symbol'], top_losers['price_change_pct_24h'], color='red', alpha=0.7)
        ax2.set_xlabel("Biến động giá (%)", fontsize=11, fontweight='bold')
        ax2.set_title("Top 10 Giảm Giá Mạnh Nhất (24h)", fontsize=13, fontweight='bold', color='darkred')
        ax2.grid(axis='x', alpha=0.3)
    else:
        print("[BỎ QUA] Thiếu dữ liệu cho Top Giảm Giá")

    plt.tight_layout()
    plt.savefig(f"{output_dir}/04_top_tang_giam.png", dpi=300)
    plt.close()
    print("Đã tạo: 04_top_tang_giam.png")

    print(f"\nPhân tích hoàn tất, lưu tại: {output_dir}")
    print(f"Đã tạo 4 biểu đồ:")
    print(f"   1. 01_top20_von_hoa.png")
    print(f"   2. 02_phan_phoi_bien_dong_gia.png")
    print(f"   3. 03_khoi_luong_vs_von_hoa.png")
    print(f"   4. 04_top_tang_giam.png")

# # ===== TEST NHANH =====
# if __name__ == "__main__":
#     import glob
#     csv_files = glob.glob("output_data/crypto_clean/*.csv")
#     if csv_files:
#         latest_csv = max(csv_files, key=os.path.getctime)
#         print(f"[TEST] Đang sử dụng file: {latest_csv}\n")
#         analyze_crypto(latest_csv)
#     else:
#         print("[LỖI] Không tìm thấy file CSV. Chạy extract + transform trước!")
#         print("Chạy lệnh: python extract_data/extract_crypto.py")