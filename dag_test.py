from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# local import
from table import insert_data_to_table

args = {
    'start_date': pendulum.datetime(2022, 7, 19, tz="UTC"),
    'schedule_interval': '0/1 * * * *', # run every 1 min
    'catchup': False
}


with DAG(dag_id='dag_test', default_args=args,) as dag:
    insert_data = PythonOperator(
        task_id='insert_data_to_table',
        python_callable=insert_data_to_table
    )

    insert_data

if __name__ == '__main__':
    print(f'DAG {dag.dag_id} is running')