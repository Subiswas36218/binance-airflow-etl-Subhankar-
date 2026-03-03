"""
Lecture 4 - Example 1: BashOperator with Jinja Templating

Downloads Wikipedia pageviews using BashOperator.
URL is dynamically built using Jinja templating and logical_date.

Airflow 3.x compatible.
"""

from datetime import datetime
from airflow import DAG

try:
    from airflow.operators.bash import BashOperator
except ImportError:
    from airflow.providers.standard.operators.bash import BashOperator


# DAG Definition
dag = DAG(
    dag_id="chapter4_bash_templating",
    start_date=datetime(2024, 3, 1),
    schedule="@hourly",
    catchup=False,
    tags=["lecture4", "templating", "bash"],
)


# Task Definition
get_data = BashOperator(
    task_id="get_data",
    bash_command="""
curl -sL -o /tmp/wikipageviews.gz \
"https://dumps.wikimedia.org/other/pageviews/{{ logical_date.strftime('%Y') }}/{{ logical_date.strftime('%Y-%m') }}/pageviews-{{ logical_date.strftime('%Y%m%d-%H') }}0000.gz"
""",
    dag=dag,
)
