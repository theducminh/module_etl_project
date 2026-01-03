import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")


def analyze_traffic_flow(clean_csv_path, output_dir="output_data/charts"):
    """
    Analyze traffic with 4 diverse charts:
      1. Speed distribution (histogram) - PHÂN BỐ
      2. Congestion distribution (histogram) - PHÂN BỐ
      3. Speed vs Free Flow scatter plot - SO SÁNH
      4. Top 10 most congested locations (bar chart) - TOP
    """
    if not os.path.isfile(clean_csv_path):
        raise FileNotFoundError(f"CSV not found: {clean_csv_path}")

    os.makedirs(output_dir, exist_ok=True)
    df = pd.read_csv(clean_csv_path)
    df["congestion"] = df["free_flow_speed"] - df["speed_kmph"]
    df["congestion_pct"] = (df["congestion"] / df["free_flow_speed"]) * 100

    # ===== 1. SPEED DISTRIBUTION (Histogram) =====
    plt.figure(figsize=(10, 6))
    plt.hist(df["speed_kmph"], bins=20, color="skyblue", edgecolor="black", alpha=0.7)
    plt.axvline(df["speed_kmph"].mean(), color="red", linestyle="--", 
                label=f"Trung binh: {df['speed_kmph'].mean():.1f} km/h")
    plt.xlabel("Toc do (km/h)")
    plt.ylabel("So luong diem")
    plt.title("Phan bo toc do giao thong tai Ha Noi")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "speed_distribution.png"), dpi=150)
    plt.close()

    # ===== 2. CONGESTION DISTRIBUTION (Histogram) =====
    plt.figure(figsize=(10, 6))
    plt.hist(df["congestion"], bins=20, color="coral", edgecolor="black", alpha=0.7)
    plt.axvline(df["congestion"].mean(), color="darkred", linestyle="--", 
                label=f"Trung binh: {df['congestion'].mean():.1f} km/h")
    plt.xlabel("Do tac nghen (km/h)")
    plt.ylabel("So luong diem")
    plt.title("Phan bo do tac nghen giao thong tai Ha Noi")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "congestion_distribution.png"), dpi=150)
    plt.close()

    # ===== 3. SPEED vs FREE FLOW SCATTER =====
    plt.figure(figsize=(10, 8))
    scatter = plt.scatter(df["free_flow_speed"], df["speed_kmph"], 
                c=df["congestion"], cmap="RdYlGn_r", s=100, alpha=0.6, edgecolors="black")
    plt.plot([0, 50], [0, 50], "k--", alpha=0.3, label="Toc do ly tuong")
    plt.colorbar(scatter, label="Do tac nghen (km/h)")
    plt.xlabel("Toc do ly tuong (km/h)")
    plt.ylabel("Toc do hien tai (km/h)")
    plt.title("So sanh toc do hien tai vs ly tuong")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "speed_comparison.png"), dpi=150)
    plt.close()

    # ===== 4. TOP 10 CONGESTED LOCATIONS (Bar Chart Vertical) =====
    top10_congested = df.nlargest(10, "congestion")[["location_name", "congestion"]].sort_values("congestion", ascending=False)
    
    # Rút ngắn tên location
    short_names = []
    for name in top10_congested["location_name"]:
        if len(name) > 30:
            short_names.append(name[:27] + "...")
        else:
            short_names.append(name)
    
    plt.figure(figsize=(14, 6))
    # Màu đỏ tăng dần theo độ tắc nghẽn
    normalized = (top10_congested["congestion"] - top10_congested["congestion"].min()) / \
                 (top10_congested["congestion"].max() - top10_congested["congestion"].min())
    colors = plt.cm.Reds(0.3 + normalized * 0.7)
    
    plt.bar(range(len(top10_congested)), top10_congested["congestion"], 
            color=colors, edgecolor="black")
    plt.xticks(range(len(top10_congested)), short_names, rotation=45, ha="right", fontsize=9)
    plt.ylabel("Do tac nghen (km/h)")
    plt.title("Top 10 diem giao thong tac nghen nhat tai Ha Noi")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "top10_congested.png"), dpi=150)
    plt.close()

    print("Charts generated:")
    print("  - speed_distribution.png (Histogram - Phan bo)")
    print("  - congestion_distribution.png (Histogram - Phan bo)")
    print("  - speed_comparison.png (Scatter plot - So sanh)")
    print("  - top10_congested.png (Bar chart - Top 10)")


if __name__ == "__main__":
    csv_file = "output_data/traffic_clean/traffic_clean.csv"
    analyze_traffic_flow(csv_file)