# transform_data/transform_fao_long.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import FloatType
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data_input", "data_fao")
OUTPUT_DIR = os.path.join(BASE_DIR, "output_data", "fao_long_cleaned")

def transform_fao():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    spark = SparkSession.builder.appName("FAO Wide to Long").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    csv_files = [os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("⚠️ Không có file CSV nào trong data_input/data_fao")

    df_all = None
    for file_path in csv_files:
        df = spark.read.option("header", True).csv(file_path)
        year_cols = [c.split()[0] for c in df.columns if c.endswith("value")]
        for y in year_cols:
            df = df.withColumn(f"{y} value", col(f"{y} value").cast(FloatType()))

        df_long_list = []
        for y in year_cols:
            df_y = df.select(
                col("Country Name En").alias("country"),
                col("Unit Name").alias("unit"),
                lit(int(y)).alias("year"),
                col(f"{y} value").alias("value"),
                col(f"{y} flag").alias("flag")
            )
            df_long_list.append(df_y)

        df_long = df_long_list[0]
        for d in df_long_list[1:]:
            df_long = df_long.unionByName(d)

        df_long_clean = df_long.dropna()
        df_all = df_long_clean if df_all is None else df_all.unionByName(df_long_clean)

    output_csv = os.path.join(OUTPUT_DIR, "fao_long.csv")
    df_all.repartition(1).write.option("header", True).mode("overwrite").csv(OUTPUT_DIR)
    spark.stop()
    print(f"✅ Transform hoàn tất → dữ liệu đã lưu tại: {output_csv}")
