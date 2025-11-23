# transform_data/transform_open_mete_pyspark.py
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

def transform_open_meteo(input_csv, output_dir="output_data/open_meteo_clean", **kwargs):
    """
    Transform CSV using PySpark (clean types).
    Return: absolute path to cleaned CSV with stable name.
    """
    spark = SparkSession.builder.appName("transform_open_meteo").getOrCreate()

    # Load CSV
    df = spark.read.option("header", True).csv(input_csv)

    # Clean types
    df2 = df.withColumn("time", to_timestamp(col("time"))) \
            .withColumn("temperature_c", col("temperature_c").cast("double"))

    # Make sure output folder exists
    os.makedirs(output_dir, exist_ok=True)

    # Write as single CSV part file
    df2.coalesce(1).write.option("header", True).mode("overwrite").csv(output_dir)
    print(f"[INFO] Cleaned CSV saved -> {output_dir}")

    spark.stop()

    # Find part-*.csv
    part_file = None
    for f in os.listdir(output_dir):
        if f.startswith("part-") and f.endswith(".csv"):
            part_file = os.path.join(output_dir, f)
            break

    if not part_file:
        raise FileNotFoundError("❌ Clean CSV not found (no part-*.csv)")

    # Stable final filename
    final_path = os.path.join(output_dir, "open_meteo_clean.csv")

    # Remove old file if exists
    if os.path.exists(final_path):
        os.remove(final_path)

    # Move part file to stable name
    shutil.move(part_file, final_path)
    print(f"[INFO] Cleaned CSV saved as -> {final_path}")

    return os.path.abspath(final_path)
