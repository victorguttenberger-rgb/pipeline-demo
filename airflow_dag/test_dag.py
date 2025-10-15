
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import TimeDiff


def hello_airflow():
    print("hello from test DAG")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1900, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id='test_dag',
    default_args=default_args,
    schedule='0 3 * * *',
    catchup=False
) as dag:

    # 1)
    task = PythonOperator(
        task_id='test',
        python_callable=hello_airflow
    )

    # 2) DummyOperator is considered obsolete as of Airflow 2.3.x so we substitute with Empty Operator
    start = EmptyOperator(
        task_id='start'
    )
    end = EmptyOperator(
        task_id='end'
    )
    start >> end

    # 3)
    N = 14
    taskn = []
    for i in range(N):
        taskn.append(EmptyOperator(task_id=f'task_{i}'))
    for i in range(0, N, 2):
        for j in range(1, N, 2):
            taskn[j] >> taskn[i]

    # 4)
    timediff_task = TimeDiff.TimeDiff(
        diff_date=datetime(2025, 1, 1),
        task_id="timediff_task"
    )

    # 5) A hook is an interface that allows to connect tasks to external resources like a common database, which provides a common context in the instances where it's needed
