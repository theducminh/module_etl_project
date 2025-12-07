import pandas as pd
import matplotlib.pyplot as plt
import os

def _analyze_temperature(clean_path):
    df = pd.read_csv(clean_path)
    df["time"] = pd.to_datetime(df["time"])

    plt.figure(figsize=(14,6))
    plt.plot(df["time"], df["temperature_c"])
    plt.title(f"Biến động nhiệt độ - {df['city'][0]}")
    plt.xlabel("Thời gian")
    plt.ylabel("Nhiệt độ (°C)")
    plt.grid(True)

    output_dir = os.path.dirname(clean_path)
    output_path = os.path.join(output_dir, f"{df['city'][0]}_temperature_plotk.png")
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"[INFO] Chart saved: {output_path}")
    return output_path
