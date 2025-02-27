from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

# Define default arguments
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def task_success():
    print("This task will succeed")
    return "Success!"

def task_random_fail():
    if random.random() < 0.1:  # 75% chance of failure
        raise Exception("Random failure!")
    return "Success!"

def task_final():
    print("Final task")
    return "Done!"

# Create the DAG
with DAG(
    'test_workflow',
    default_args=default_args,
    description='A test DAG for monitoring',
    schedule='*/5 * * * *',  # Run every 5 minutes for testing
    catchup=False
) as dag:

    # Define tasks
    task1 = PythonOperator(
        task_id='always_succeed',
        python_callable=task_success,
    )

    task2 = PythonOperator(
        task_id='randomly_fail',
        python_callable=task_random_fail,
    )

    task3 = PythonOperator(
        task_id='final_task',
        python_callable=task_final,
    )

    # Set task dependencies
    task1 >> task2 >> task3

if __name__ == "__main__":
    dag.test()