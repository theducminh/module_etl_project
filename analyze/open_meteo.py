# analyze/open_meteo.py
import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def _analyze_temperature(clean_path: str, output_dir: str = "output_data/open_meteo"):
    """
    Vẽ 4 biểu đồ từ CSV (time, temperature_c) của Open-Meteo.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Đọc CSV đã làm sạch
    df = pd.read_csv(clean_path)

    # Kiểm tra cột
    required_cols = {"time", "temperature_c"}
    if not required_cols.issubset(df.columns):
        raise ValueError("CSV thiếu cột bắt buộc: time, temperature_c")

    # Chuẩn hóa kiểu dữ liệu
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df = df.dropna(subset=["time", "temperature_c"]).sort_values("time").reset_index(drop=True)

    # Style
    sns.set_style("whitegrid")
    plt.rcParams["figure.facecolor"] = "white"

    outputs = []

    # ----- 1) Nhiệt độ theo thời gian (Line) -----
    if not df.empty:
        plt.figure(figsize=(14, 6))
        plt.plot(df["time"], df["temperature_c"], color="#1f77b4", linewidth=1.5, label="Nhiệt độ (giờ)")
        # Trung bình trượt 3 giờ cho mượt
        if len(df) >= 3:
            roll = df["temperature_c"].rolling(3, min_periods=1).mean()
            plt.plot(df["time"], roll, color="#ff7f0e", linewidth=2, label="Trung bình trượt (3h)")
        plt.title("Nhiệt độ theo thời gian (UTC)", fontsize=14, fontweight="bold")
        plt.xlabel("Thời gian", fontsize=12, fontweight="bold")
        plt.ylabel("Nhiệt độ (°C)", fontsize=12, fontweight="bold")
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()
        out1 = os.path.join(output_dir, "01_temp_timeseries.png")
        plt.savefig(out1, dpi=300)
        plt.close()
        print(f"Đã tạo: {out1}")
        outputs.append(out1)

    # ----- 2) Biên độ nhiệt theo ngày (Min–Max band + Avg line) -----
    if not df.empty:
        df["date"] = df["time"].dt.date
        daily = (
            df.groupby("date", as_index=False)["temperature_c"]
              .agg(min="min", max="max", mean="mean", count="count")
        )
        if not daily.empty:
            x = pd.to_datetime(daily["date"])
            plt.figure(figsize=(14, 6))
            # Dải min-max
            plt.fill_between(
                x, daily["min"], daily["max"],
                color="#a6cee3", alpha=0.5, label="Dải Min–Max"
            )
            # Trung bình ngày
            plt.plot(x, daily["mean"], color="#1f78b4", linewidth=2, label="Trung bình ngày")
            plt.title("Biên độ nhiệt theo ngày (UTC)", fontsize=14, fontweight="bold")
            plt.xlabel("Ngày", fontsize=12, fontweight="bold")
            plt.ylabel("Nhiệt độ (°C)", fontsize=12, fontweight="bold")
            plt.legend()
            plt.grid(alpha=0.3)
            plt.tight_layout()
            out2 = os.path.join(output_dir, "02_daily_range.png")
            plt.savefig(out2, dpi=300)
            plt.close()
            print(f"Đã tạo: {out2}")
            outputs.append(out2)

    # ----- 3) Chu kỳ nhiệt theo giờ (Mean by hour-of-day) -----
    if not df.empty:
        df["hour"] = df["time"].dt.hour
        hourly = df.groupby("hour", as_index=False)["temperature_c"].mean()
        if not hourly.empty:
            plt.figure(figsize=(12, 6))
            colors = plt.cm.viridis(np.linspace(0, 1, len(hourly)))
            plt.bar(hourly["hour"], hourly["temperature_c"], color=colors, edgecolor="black", alpha=0.9)
            plt.xticks(range(0, 24, 1))
            plt.title("Nhiệt độ trung bình theo giờ trong ngày (UTC)", fontsize=14, fontweight="bold")
            plt.xlabel("Giờ (0–23)", fontsize=12, fontweight="bold")
            plt.ylabel("Nhiệt độ (°C)", fontsize=12, fontweight="bold")
            plt.grid(axis="y", alpha=0.3)
            plt.tight_layout()
            out3 = os.path.join(output_dir, "03_diurnal_cycle.png")
            plt.savefig(out3, dpi=300)
            plt.close()
            print(f"Đã tạo: {out3}")
            outputs.append(out3)

    # ----- 4) Phân phối nhiệt độ (Histogram + KDE) -----
    s = df["temperature_c"].dropna()
    if len(s) > 1:
        plt.figure(figsize=(12, 6))
        bins = min(60, max(20, int(np.sqrt(len(s)))))
        # Dùng seaborn histplot để có KDE trên cùng một axes (1 biểu đồ)
        sns.histplot(s, bins=bins, kde=True, color="steelblue", stat="count", edgecolor="black", alpha=0.8)
        plt.title("Phân phối nhiệt độ", fontsize=14, fontweight="bold")
        plt.xlabel("Nhiệt độ (°C)", fontsize=12, fontweight="bold")
        plt.ylabel("Tần suất", fontsize=12, fontweight="bold")
        plt.grid(alpha=0.3)
        plt.tight_layout()
        out4 = os.path.join(output_dir, "04_temp_distribution.png")
        plt.savefig(out4, dpi=300)
        plt.close()
        print(f"Đã tạo: {out4}")
        outputs.append(out4)

    return outputs


# # ===== TEST NHANH =====
# if __name__ == "__main__":
#     # Tìm CSV mới nhất trong data_input/open_meteo/**/
#     candidates = glob.glob("data_input/open_meteo/*/open_meteo_raw_*.csv")
#     if not candidates:
#         print("[LỖI] Không tìm thấy CSV. Hãy chạy extract trước:")
#         print("python extract_data/extract_open_meteo.py")
#     else:
#         latest_csv = max(candidates, key=os.path.getmtime)
#         print(f"[TEST] Đang dùng file: {latest_csv}")
#         outs = analyze_open_meteo(latest_csv)
#         print("Ảnh đã tạo:")
#         for p in outs:
#             print(" -", p)