"""
Lecture 4 - Example 5: templates_dict for Templated Paths (Airflow 3.x)

templates_dict allows passing templated values to PythonOperator.
Values are rendered at runtime and available in context["templates_dict"].

Use case: Date-partitioned file paths like /data/events/{{ ds }}.json
"""

from datetime import datetime
from pathlib import Path

from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator


def _process_data(**context):
    """
    Process data using paths from templates_dict.
    Access via context["templates_dict"]["key_name"]
    """

    templates = context["templates_dict"]

    input_path = templates["input_path"]
    output_path = templates["output_path"]
    ds = templates["ds"]

    print(f"Processing data for date: {ds}")
    print(f"  Input:  {input_path}")
    print(f"  Output: {output_path}")

    # Create output directory
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Simulate processing
    with open(output_path, "w") as f:
        f.write(f"Processed data for {ds}\n")
        f.write(f"Input was: {input_path}\n")

    print("Processing complete.")


# DAG Definition
dag = DAG(
    dag_id="chapter4_templates_dict",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lecture4", "templating", "templates_dict"],
)


BASE_PATH = "/Users/subhankarbiswas/airflow/data"

process_data = PythonOperator(
    task_id="process_data",
    python_callable=_process_data,
    templates_dict={
        "input_path": f"{BASE_PATH}/input/{{{{ ds }}}}.json",
        "output_path": f"{BASE_PATH}/output/{{{{ ds }}}}.csv",
        "ds": "{{ ds }}",
    },
    dag=dag,
)
