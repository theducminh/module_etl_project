import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lit

def transform_traffic(input_csv, output_dir="output_data/traffic_clean", **kwargs):
    """
    Transform raw traffic CSV to clean CSV:
    - Convert 'time' column to timestamp
    - Convert 'speed_kmph' and 'free_flow_speed' to double
    - Remove duplicate rows based on (time, lat, lon)
    - Save as single CSV file 'traffic_clean.csv'

    Args:
        input_csv (str): Path to raw CSV file
        output_dir (str): Folder to save cleaned CSV

    Returns:
        str: Absolute path to cleaned CSV
    """
    if not os.path.isfile(input_csv):
        raise FileNotFoundError(f"❌ Input CSV not found: {input_csv}")

    # Initialize Spark
    spark = SparkSession.builder.appName("transform_traffic").getOrCreate()

    # Read CSV
    df = spark.read.option("header", True).csv(input_csv)

    # Transform types
    df_clean = (
        df.withColumn("time", to_timestamp(col("time")))
          .withColumn("location_name", col("intersection"))
          .withColumn("speed_kmph", col("currentSpeed").cast("double"))
          .withColumn("free_flow_speed", col("freeFlowSpeed").cast("double"))
          .withColumn("confidence", col("confidence").cast("double"))
          .withColumn("road_closure", col("roadClosure").cast("boolean"))
          .withColumn("frc", lit(None).cast("string"))
          .drop("intersection", "currentSpeed", "freeFlowSpeed", "roadClosure")
    )

    # Remove duplicates based on (time, lat, lon)
    df_clean = df_clean.dropDuplicates(["time", "lat", "lon"])

    # Ensure output folder exists
    os.makedirs(output_dir, exist_ok=True)

    # Write single CSV (coalesce 1)
    df_clean.coalesce(1).write.option("header", True).mode("overwrite").csv(output_dir)

    spark.stop()

    # Rename part file to final CSV
    part_files = [f for f in os.listdir(output_dir) if f.startswith("part-") and f.endswith(".csv")]
    if not part_files:
        raise FileNotFoundError(f"❌ No part file found in {output_dir}")
    part_file = os.path.join(output_dir, part_files[0])

    final_path = os.path.join(output_dir, "traffic_clean.csv")
    if os.path.exists(final_path):
        os.remove(final_path)
    shutil.move(part_file, final_path)

    print(f"[INFO] Cleaned CSV saved -> {final_path}")
    return os.path.abspath(final_path)
