"""Airbyte collects simulated service APIs; Airflow orchestrates syncs, Silver and Gold."""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines import jobs
from pipelines.airbyte import sync_provider, verify_bronze

common = dict(start_date=pendulum.datetime(2026, 10, 20, tz="UTC"), catchup=False,
              max_active_runs=1, is_paused_upon_creation=False,
              default_args={"retries": 2, "retry_delay": timedelta(minutes=1)}, tags=["shopvibe", "simulacao"])

with DAG("shopvibe_servicos_4h", schedule="0 */4 * * *",
         description="APIs simuladas -> Airbyte -> MinIO Bronze a cada 4h UTC", **common) as external_services:
    bling = PythonOperator(task_id="airbyte_bling", python_callable=sync_provider, op_kwargs={"provider": "bling"})
    shipping = PythonOperator(task_id="airbyte_melhor_envio", python_callable=sync_provider,
                              op_kwargs={"provider": "melhor_envio"})
    bronze = PythonOperator(task_id="verify_bronze_airbyte", python_callable=verify_bronze)
    bling >> shipping >> bronze

with DAG("shopvibe_silver_diaria", schedule="0 2 * * *",
         description="Bronze -> Silver -> Gold Parquet diariamente as 02:00 UTC", **common) as silver_daily:
    build = PythonOperator(task_id="build_silver", python_callable=jobs.build_silver)
    validate = PythonOperator(task_id="validate_silver", python_callable=jobs.verify_silver,
                              op_kwargs={"snapshot_id": "{{ ti.xcom_pull(task_ids='build_silver')['snapshot_id'] }}"})
    gold = PythonOperator(task_id="build_gold", python_callable=jobs.build_gold,
                          op_kwargs={"silver_snapshot_id": "{{ ti.xcom_pull(task_ids='build_silver')['snapshot_id'] }}"})
    validate_gold = PythonOperator(task_id="validate_gold", python_callable=jobs.verify_gold,
                                   op_kwargs={"snapshot_id": "{{ ti.xcom_pull(task_ids='build_gold')['snapshot_id'] }}"})
    build >> validate >> gold >> validate_gold
