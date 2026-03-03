"""
Lecture 4 - Example 3: PythonOperator with Task Context (Airflow 3.x)

Downloads Wikipedia pageviews using PythonOperator.
Accesses logical_date from the Airflow task context.
"""

from datetime import datetime
from urllib import request, error

from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator



def _get_data(**context):
    logical_date = context["logical_date"]

    year = logical_date.year
    month = logical_date.month
    day = logical_date.day
    hour = logical_date.hour

    url = (
        f"https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:02d}/"
        f"pageviews-{year}{month:02d}{day:02d}-{hour:02d}0000.gz"
    )

    try:
        request.urlretrieve(url, "/tmp/wikipageviews.gz")
        print("Download successful.")
    except error.HTTPError as e:
        if e.code == 404:
            print("File not available yet. Skipping.")
            return
        raise

    output_path = "/tmp/wikipageviews.gz"

    print(f"Downloading {url}")
    request.urlretrieve(url, output_path)
    print(f"Saved to {output_path}")


# DAG Definition
dag = DAG(
    dag_id="chapter4_python_context",
    start_date=datetime(2024, 3, 1),
    schedule="@hourly",
    catchup=False,
    tags=["lecture4", "templating", "python"],
)

get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    dag=dag,
)
