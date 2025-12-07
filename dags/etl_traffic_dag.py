import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/mnt/d/module_etl"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from extract_data.extract_traffic import extract_traffic
from load_data.upload_to_supabase_storage1 import upload_to_storage
from transform_data.transform_traffic_pyspark import transform_traffic
from load_data.copy_into_postgres1 import copy_into_postgres
from analyze.traffic_analysis import analyze_traffic_flow

# --- TASKS ---
def _upload_raw(ti):
    path = ti.xcom_pull(task_ids="extract_traffic")
    if not path:
        raise ValueError("No extracted CSV path found")
    return upload_to_storage(path, bucket="datalake-traffic", dest_folder="raw")

def _transform(ti):
    input_csv = ti.xcom_pull(task_ids="extract_traffic")
    if not input_csv:
        raise ValueError("No extracted CSV path found")
    
    cleaned_path = transform_traffic(input_csv)
    ti.xcom_push(key="cleaned_csv", value=cleaned_path)
    return cleaned_path

def _copy_into_postgres(ti):
    clean_path = ti.xcom_pull(task_ids="transform_with_pyspark", key="cleaned_csv")
    if not clean_path:
        raise ValueError("No cleaned CSV path found")
    return copy_into_postgres(clean_path, table="fact_traffic")

def _analyze(ti):
    clean_path = ti.xcom_pull(task_ids="transform_with_pyspark", key="cleaned_csv")
    if not clean_path:
        raise ValueError("No cleaned CSV path for analysis")
    return analyze_traffic_flow(clean_path)

# --- DAG ---
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    "etl_traffic",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="extract_traffic",
        python_callable=extract_traffic
    )

    t2 = PythonOperator(
        task_id="upload_raw_to_storage",
        python_callable=_upload_raw
    )

    t3 = PythonOperator(
        task_id="transform_with_pyspark",
        python_callable=_transform
    )

    t4 = PythonOperator(
        task_id="copy_into_postgres",
        python_callable=_copy_into_postgres
    )

    t5 = PythonOperator(
        task_id="analyze_traffic_flow",
        python_callable=_analyze
    )

    t1 >> t2 >> t3 >> t4 >> t5
