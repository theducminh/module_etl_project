# dags/etl_crypto.py
import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/mnt/d/module_etl"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from extract_data.extract_crypto import extract_crypto
from transform_data.transform_crypto_pyspark import transform_crypto
from load_data.upload_to_supabase_storage2 import upload_to_storage
from load_data.copy_into_postgres2 import copy_into_postgres
from analyze.crypto_analysis import analyze_crypto

# --- WRAPPERS ---
def _upload_raw(ti):
    raw_path = ti.xcom_pull(task_ids="extract_crypto")
    return upload_to_storage(raw_path)

def _transform(ti):
    raw_path = ti.xcom_pull(task_ids="extract_crypto")
    clean_path = transform_crypto(raw_path)
    ti.xcom_push(key="cleaned_csv", value=clean_path)
    return clean_path

def _copy(ti):
    clean = ti.xcom_pull(task_ids="transform_crypto", key="cleaned_csv")
    return copy_into_postgres(clean)

def _analyze(ti):
    clean = ti.xcom_pull(task_ids="transform_crypto", key="cleaned_csv")
    return analyze_crypto(clean)

# --- DAG ---
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    "etl_crypto",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="extract_crypto",
        python_callable=extract_crypto
    )

    t2 = PythonOperator(
        task_id="upload_raw_to_storage",
        python_callable=_upload_raw
    )

    t3 = PythonOperator(
        task_id="transform_crypto",
        python_callable=_transform
    )

    t4 = PythonOperator(
        task_id="copy_into_postgres",
        python_callable=_copy
    )

    t5 = PythonOperator(
        task_id="analyze_crypto",
        python_callable=_analyze
    )

    t1 >> t2 >> t3 >> t4 >> t5
