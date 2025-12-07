import os
import pandas as pd
import matplotlib.pyplot as plt


def analyze_traffic_flow(clean_csv_path, output_dir="output_data/charts"):
    """
    Analyze traffic flow per location (lat, lon)
    Generate:
      - Speed vs Free Flow chart
      - Congestion chart
    """
    if not os.path.isfile(clean_csv_path):
        raise FileNotFoundError(f"❌ CSV not found: {clean_csv_path}")

    os.makedirs(output_dir, exist_ok=True)
    df = pd.read_csv(clean_csv_path)
    df["time"] = pd.to_datetime(df["time"])

    # Tính congestion
    df["congestion"] = df["free_flow_speed"] - df["speed_kmph"]

    # Nhóm theo location
    grouped = df.groupby(["lat", "lon"])

    chart_paths = []

    for (lat, lon), group in grouped:
        group = group.sort_values("time")

        # ===== SPEED CHART =====
        plt.figure(figsize=(14, 6))
        plt.plot(group["time"], group["speed_kmph"], label="Current Speed")
        plt.plot(group["time"], group["free_flow_speed"], label="Free Flow Speed")
        plt.title(f"Traffic Speed @ ({lat}, {lon})")
        plt.xlabel("Time")
        plt.ylabel("Speed (km/h)")
        plt.legend()
        plt.grid(True)

        speed_path = os.path.join(output_dir, f"speed_{lat}_{lon}.png")
        plt.savefig(speed_path, dpi=150)
        plt.close()

        # ===== CONGESTION CHART =====
        plt.figure(figsize=(14, 6))
        plt.plot(group["time"], group["congestion"], label="Congestion (Free - Current)")
        plt.title(f"Traffic Congestion @ ({lat}, {lon})")
        plt.xlabel("Time")
        plt.ylabel("Speed Difference (km/h)")
        plt.legend()
        plt.grid(True)

        congestion_path = os.path.join(output_dir, f"congestion_{lat}_{lon}.png")
        plt.savefig(congestion_path, dpi=150)
        plt.close()

        chart_paths.append((speed_path, congestion_path))

    print("[INFO] Charts generated for each location.")
    return chart_paths


if __name__ == "__main__":
    csv_file = "output_data/traffic_clean/traffic_clean.csv"
    analyze_traffic_flow(csv_file)
