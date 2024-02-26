from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_processing_and_visualization',
    default_args=default_args,
    description='A DAG to process raw log data, update summary tables, and trigger visualization',
    schedule_interval='30 23 * * *'  # Scheduled to run daily at 23:30 CET
)

process_log_task = BashOperator(
    task_id='process_log',
    bash_command='raw_data_load.py',  # Script to process log data
    dag=dag
)

update_summary_tables_task = BashOperator(
    task_id='update_summary_tables',
    bash_command='update_summary_table.py',  # Script to update summary tables
    dag=dag
)



pipeline_started >> process_log_task >> update_summary_tables_task
