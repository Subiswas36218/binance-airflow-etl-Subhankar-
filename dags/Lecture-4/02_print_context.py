"""
Lecture 4 - Example 2: Printing the Task Context (Airflow 3.x)

This DAG prints available task context variables.
Check the task logs to see runtime metadata provided by Airflow.
"""

from datetime import datetime
from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator


def _print_context(**context):
    """Print task context variables passed by Airflow."""

    print("=== All Context Variables ===")
    for key, value in sorted(context.items()):
        if key not in ("dag", "task", "ti", "task_instance", "conf", "macros"):
            print(f"{key}: {value}")

    print("\n=== Key Variables (Airflow 3) ===")
    print(f"logical_date: {context.get('logical_date')}")
    print(f"data_interval_start: {context.get('data_interval_start')}")
    print(f"data_interval_end: {context.get('data_interval_end')}")
    print(f"ds (YYYY-MM-DD): {context.get('ds')}")
    print(f"ds_nodash: {context.get('ds_nodash')}")
    print(f"run_id: {context.get('run_id')}")
    print(f"dag_run: {context.get('dag_run')}")


# DAG Definition
dag = DAG(
    dag_id="chapter4_print_context",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lecture4", "templating", "context"],
)

print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag,
)
