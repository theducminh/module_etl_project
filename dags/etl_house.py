import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# =========================
# ADD PROJECT ROOT
# =========================
PROJECT_ROOT = "/mnt/d/module_etl"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =========================
# IMPORT CÁC MODULE HOUSE
# =========================
from extract_data.extract_house import extract_house
from load_data.upload_to_supabase_storage3 import upload_to_storage
from transform_data.transform_house_pyspark import clean_house
from load_data.copy_into_postgres3 import copy_into_postgres
from analyze.house_analysis import analyze_house

# =========================================
# WRAPPER FUNCTIONS FOR PYTHON OPERATOR
# =========================================

def _upload_raw(ti):
    path = ti.xcom_pull(task_ids="extract_house")
    if not path:
        raise ValueError("No extracted CSV path found")
    return upload_to_storage(path, bucket="datalake-house", dest_folder="raw")


def _transform(ti):
    raw_csv = ti.xcom_pull(task_ids="extract_house")
    if not raw_csv:
        raise ValueError("No extracted CSV found")

    clean_path = clean_house(
        raw_path=raw_csv,
        output_dir="output_data/house_clean"
    )

    ti.xcom_push(key="cleaned_csv", value=clean_path)
    return clean_path


def _copy_into_postgres(ti):
    clean_path = ti.xcom_pull(task_ids="transform_house", key="cleaned_csv")
    if not clean_path:
        raise ValueError("No cleaned CSV found")
    return copy_into_postgres(clean_path, table="fact_house_listings")


def _analyze(ti):
    clean_path = ti.xcom_pull(task_ids="transform_house", key="cleaned_csv")
    if not clean_path:
        raise ValueError("No cleaned CSV found for analysis")
    return analyze_house(clean_path)


# =========================
# DEFAULT ARGS
# =========================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

# =========================
# DAG
# =========================
with DAG(
    "etl_house",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["house", "real_estate", "chotot"]
) as dag:

    t1 = PythonOperator(
        task_id="extract_house",
        python_callable=extract_house,
        op_kwargs={"limit_rows": 300}
    )

    t2 = PythonOperator(
        task_id="upload_raw_to_storage",
        python_callable=_upload_raw
    )

    t3 = PythonOperator(
        task_id="transform_house",
        python_callable=_transform
    )

    t4 = PythonOperator(
        task_id="copy_into_postgres",
        python_callable=_copy_into_postgres
    )

    t5 = PythonOperator(
        task_id="analyze_house",
        python_callable=_analyze
    )

    # Pipeline order
    t1 >> t2 >> t3 >> t4 >> t5
