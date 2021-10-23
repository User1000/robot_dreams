from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


from stock.stock_data import get_out_of_stock

config_path = "/home/user/airflow/dags/stock/config.yaml"
dates = ["2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"]
tasks = []

dag = DAG(
    dag_id='get_out_of_stock_dag',
    description='get_out_of_stock_dag',
    start_date=datetime(2021, 10, 22),
    end_date=datetime(2022, 10, 25),
    schedule_interval = '@daily'
)

dummy_start = DummyOperator(task_id="start_dag", dag=dag)
dummy_end = DummyOperator(task_id="end_dag", dag=dag)

for date in dates:
    tasks.append(
        PythonOperator(
            task_id=f"get_out_of_stock_{date}",
            dag=dag,
            python_callable=get_out_of_stock,
            op_kwargs={"config_path": config_path, "date": date}
            # provide_context=True
        )
    )

dummy_start >> tasks >> dummy_end
