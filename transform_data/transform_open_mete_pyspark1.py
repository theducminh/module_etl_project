import os, shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

def transform_open_meteo(input_csv, output_dir="output_data/open_meteo_clean1"):
    spark = SparkSession.builder.appName("transform_open_meteo").getOrCreate()

    df = spark.read.option("header", True).csv(input_csv)
    df_clean = df.withColumn("time", to_timestamp(col("time"))) \
                 .withColumn("temperature_c", col("temperature_c").cast("double")) \
                 .withColumn("latitude", col("latitude").cast("double")) \
                 .withColumn("longitude", col("longitude").cast("double"))

    os.makedirs(output_dir, exist_ok=True)
    df_clean.coalesce(1).write.option("header", True).mode("overwrite").csv(output_dir)
    spark.stop()

    part_file = next((os.path.join(output_dir, f) for f in os.listdir(output_dir)
                     if f.startswith("part-") and f.endswith(".csv")), None)
    if not part_file:
        raise FileNotFoundError("Clean CSV not found")

    final_path = os.path.join(output_dir, "open_meteo_clean1.csv")
    if os.path.exists(final_path):
        os.remove(final_path)
    shutil.move(part_file, final_path)
    print(f"[INFO] Cleaned CSV saved -> {final_path}")
    return os.path.abspath(final_path)
