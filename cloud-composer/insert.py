from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'insert_data_bigquery',
    default_args=default_args,
    description='A simple DAG to insert data into BigQuery',
    schedule_interval=timedelta(days=1),
)

# Task to insert data into BigQuery
insert_data_task = BigQueryInsertJobOperator(
    task_id='insert_data_to_bigquery',
    configuration={
        'query': {
            'query': '''
            INSERT INTO `my_dataset.my_table` (id, name, created_at)
            VALUES (1, 'Sample Name', CURRENT_TIMESTAMP()),
                   (2, 'Another Name', CURRENT_TIMESTAMP())
            ''',
            'useLegacySql': False,
        }
    },
    dag=dag,
)

# Set task dependencies (if any)
# Example: task1 >> task2 >> task3

# End of DAG definition

if __name__ == "__main__":
    dag.cli()
