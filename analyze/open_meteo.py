#analyze/open_meteo.py
# --- NEW: Analysis Library ---
import pandas as pd
import matplotlib.pyplot as plt

def _analyze_temperature(clean_path):
    if not clean_path:
        raise ValueError("No cleaned CSV path for analysis")

    # Đọc CSV
    df = pd.read_csv(clean_path)

    if "time" not in df.columns or "temperature_c" not in df.columns:
        raise Exception("❌ CSV missing required columns")

    df["time"] = pd.to_datetime(df["time"])

    # Vẽ chart
    plt.figure(figsize=(14, 6))
    plt.plot(df["time"], df["temperature_c"])
    plt.title("Biến động nhiệt độ theo thời gian")
    plt.xlabel("Thời gian")
    plt.ylabel("Nhiệt độ (°C)")
    plt.grid(True)

    output_path = "/mnt/d/module_etl/output_data/temperature_plot.png"
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"[INFO] Chart saved: {output_path}")

    