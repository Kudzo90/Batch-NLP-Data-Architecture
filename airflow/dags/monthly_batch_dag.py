from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineer',
    'retries': 0,  # Stop retry loop for clean demo
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 1, 1),
}

with DAG(
    'monthly_nlp_batch',
    default_args=default_args,
    description='Monthly batch NLP pipeline for Babel Briefings',
    schedule_interval='0 0 1 * *',  # First day of each month
    catchup=False,  # No historical backfill
) as dag:

    # Task 1: Ingest raw JSON data and partition by month
    ingest = BashOperator(
        task_id='ingest_raw_data',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit /opt/spark-jobs/ingest_all_languages.py --month {{ ds[:7] }} || true'
    )

    # Task 2: Clean and enrich with NLP
    clean_enrich = BashOperator(
        task_id='clean_and_enrich',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit /opt/spark-jobs/clean_enrich.py --month {{ ds[:7] }} || true'
    )

    # Task 3: Quarterly aggregation (runs every month, but aggregates by quarter)
    aggregate = BashOperator(
        task_id='quarterly_aggregate',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit /opt/spark-jobs/quarterly_aggregate.py --quarter {{ ds[:7] }} || true'
    )

    # Define the pipeline flow
    ingest >> clean_enrich >> aggregate
