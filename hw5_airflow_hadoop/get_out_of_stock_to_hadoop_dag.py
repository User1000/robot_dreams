from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from stock.stock_data import get_out_of_stock_and_save_to_hadoop

config_path = "/home/user/airflow/dags/stock/config.yaml"
tasks = []

dag = DAG(
    dag_id='get_out_of_stock_to_hadoop_dag',
    description='get_out_of_stock_to_hadoop_dag',
    start_date=datetime(2021, 10, 22),
    end_date=datetime(2025, 10, 25),
    schedule_interval = '@daily'
)

dummy_start = DummyOperator(task_id="start_dag", dag=dag)
dummy_end = DummyOperator(task_id="end_dag", dag=dag)



task = PythonOperator(
    task_id=f"get_out_of_stock",
    dag=dag,
    python_callable=get_out_of_stock_and_save_to_hadoop,
    op_kwargs={"config_path": config_path},
    provide_context=True
)


dummy_start >> task >> dummy_end
