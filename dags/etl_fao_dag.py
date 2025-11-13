# dags/etl_fao_dag.py
import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# -----------------------------
# Thêm project root vào sys.path
# -----------------------------
PROJECT_ROOT = '/mnt/d/module_etl'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# -----------------------------
# Imports ETL modules
# -----------------------------
from extract_data.crawl_data_fao import extract_fao
from transform_data.transform_fao_long import transform_fao
from load_data.load_fao_to_supabase import load_fao_supabase
from utils.supabase_client import supabase
from utils.config import SUPABASE_TABLE

# -----------------------------
# Default args
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id='etl_fao_pipeline',
    default_args=default_args,
    description='ETL Pipeline FAO → Supabase → Airflow XCom Chart',
    schedule_interval='@weekly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'fao', 'supabase', 'analytics'],
) as dag:

    # -----------------------------
    # 1️⃣ Extract FAO Data
    # -----------------------------
    extract_task = PythonOperator(
        task_id='extract_fao',
        python_callable=extract_fao,
    )

    # -----------------------------
    # 2️⃣ Transform Data
    # -----------------------------
    transform_task = PythonOperator(
        task_id='transform_fao',
        python_callable=transform_fao,
    )

    # -----------------------------
    # 3️⃣ Load Data to Supabase
    # -----------------------------
    load_task = PythonOperator(
        task_id='load_fao_supabase',
        python_callable=load_fao_supabase,
    )

    # -----------------------------
    # 4️⃣ Analyze Data & Return for XCom Chart
    # -----------------------------
    def analyze_fao():
        """
        Lấy dữ liệu từ Supabase và trả về dict để Airflow vẽ biểu đồ.
        Key = năm, Value = tổng sản lượng
        """
        data = supabase.table(SUPABASE_TABLE).select("*").execute().data
        if not data:
            return {}

        summary = {}
        for row in data:
            y = row['year']
            summary[y] = summary.get(y, 0) + row.get('value', 0)

        return summary  # Airflow XCom Chart sẽ hiển thị

    analyze_task = PythonOperator(
        task_id='analyze_fao',
        python_callable=analyze_fao,
    )

    # -----------------------------
    # Task Dependency
    # -----------------------------
    extract_task >> transform_task >> load_task >> analyze_task
