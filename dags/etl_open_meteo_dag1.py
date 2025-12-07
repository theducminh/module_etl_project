import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/mnt/d/module_etl"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from extract_data.extract_open_meteo1 import extract_open_meteo
from load_data.upload_to_supabase_storage4 import upload_to_storage
from transform_data.transform_open_mete_pyspark1 import transform_open_meteo
from load_data.copy_into_postgres4 import copy_into_postgres
from analyze.open_meteo1 import _analyze_temperature

def _upload_raw(ti):
    paths = ti.xcom_pull(task_ids="extract_open_meteo")
    return [upload_to_storage(p) for p in paths]

def _transform(ti):
    paths = ti.xcom_pull(task_ids="extract_open_meteo")
    cleaned_paths = [transform_open_meteo(p) for p in paths]
    ti.xcom_push(key="cleaned_csv", value=cleaned_paths)
    return cleaned_paths

def _copy_into_postgres(ti):
    cleaned_paths = ti.xcom_pull(task_ids="transform_with_pyspark", key="cleaned_csv")
    total_rows = sum([copy_into_postgres(p) for p in cleaned_paths])
    return total_rows

def _analyze(ti):
    cleaned_paths = ti.xcom_pull(task_ids="transform_with_pyspark", key="cleaned_csv")
    return [_analyze_temperature(p) for p in cleaned_paths]

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    "etl_open_meteo1",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2025,1,1),
    catchup=False
) as dag:

    t1 = PythonOperator(task_id="extract_open_meteo", python_callable=extract_open_meteo)
    t2 = PythonOperator(task_id="upload_raw_to_storage", python_callable=_upload_raw)
    t3 = PythonOperator(task_id="transform_with_pyspark", python_callable=_transform)
    t4 = PythonOperator(task_id="copy_into_postgres", python_callable=_copy_into_postgres)
    t5 = PythonOperator(task_id="analyze_temperature", python_callable=_analyze)

    t1 >> t2 >> t3 >> t4 >> t5
