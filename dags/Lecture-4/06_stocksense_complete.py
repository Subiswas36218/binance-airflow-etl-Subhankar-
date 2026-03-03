"""
Lecture 4 - Example 6: Complete StockSense DAG (Airflow 3.x)

StockSense: A (fictitious) stock market prediction tool using Wikipedia pageview sentiment.

Pipeline:
1. Download Wikipedia pageviews for logical_date hour
2. Extract gzip file
3. Count views for selected companies
"""

from datetime import datetime
from airflow import DAG

try:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator


PAGENAMES = {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}


def _fetch_pageviews(pagenames, **_):
    """
    Scan extracted Wikipedia pageviews file and count views for each pagename.
    Format: domain_code page_title view_count response_size
    """

    result = dict.fromkeys(pagenames, 0)

    try:
        with open("/tmp/wikipageviews", "r") as f:
            for line in f:
                parts = line.strip().split(" ")
                if len(parts) >= 4:
                    domain_code, page_title, view_count = parts[0], parts[1], parts[2]

                    if domain_code == "en" and page_title in pagenames:
                        result[page_title] = int(view_count)

    except FileNotFoundError:
        print("File not found. Ensure extract_gz runs first.")
        raise

    print("Pageview counts:")
    for company, count in result.items():
        print(f"  {company}: {count}")

    return result


# DAG Definition
dag = DAG(
    dag_id="chapter4_stocksense_complete",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["lecture4", "templating", "stocksense"],
)

# Task 1: Download pageviews (using logical_date)
get_data = BashOperator(
    task_id="get_data",
    bash_command=(
        "curl --fail -sL -o /tmp/wikipageviews.gz "
        "\"https://dumps.wikimedia.org/other/pageviews/"
        "{{ (logical_date - macros.timedelta(days=2)).strftime('%Y') }}/"
        "{{ (logical_date - macros.timedelta(days=2)).strftime('%Y-%m') }}/"
        "pageviews-{{ (logical_date - macros.timedelta(days=2)).strftime('%Y%m%d-%H') }}0000.gz\""
    ),
    dag=dag,
)

# Task 2: Extract gzip
extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip -f /tmp/wikipageviews.gz",
    dag=dag,
)

# Task 3: Count pageviews
fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": PAGENAMES},
    dag=dag,
)

# Task dependencies
get_data >> extract_gz >> fetch_pageviews
