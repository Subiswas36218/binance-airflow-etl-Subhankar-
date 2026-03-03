"""
Lecture 4 - Example 4: Templated op_kwargs (Airflow 3.x)

Values in op_kwargs are templated BEFORE being passed to the callable.
Airflow renders Jinja expressions at runtime.
"""

from datetime import datetime
from urllib import request

from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator


def _get_data(year, month, day, hour, output_path, **_):
    """
    Download Wikipedia pageviews.
    Date components are already rendered via Jinja in op_kwargs.
    """

    year = int(year)
    month = int(month)
    day = int(day)
    hour = int(hour)

    url = (
        f"https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:02d}/"
        f"pageviews-{year}{month:02d}{day:02d}-{hour:02d}0000.gz"
    )

    print(f"Downloading {url} to {output_path}")
    request.urlretrieve(url, output_path)
    print("Download complete.")


# DAG Definition
dag = DAG(
    dag_id="chapter4_op_kwargs_templating",
    start_date=datetime(2024, 3, 1),
    schedule="@hourly",
    catchup=False,
    tags=["lecture4", "templating", "op_kwargs"],
)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
    "year": "{{ (logical_date - macros.timedelta(days=2)).year }}",
    "month": "{{ (logical_date - macros.timedelta(days=2)).month }}",
    "day": "{{ (logical_date - macros.timedelta(days=2)).day }}",
    "hour": "{{ (logical_date - macros.timedelta(days=2)).hour }}",
    "output_path": "/tmp/wikipageviews.gz",
},
    dag=dag,
)
