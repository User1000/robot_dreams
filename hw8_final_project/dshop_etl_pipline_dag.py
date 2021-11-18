import os
import yaml
import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

import dshop.functions as dshop_func
import out_of_stock.functions as out_of_stock_func
import data_process.functions as data_process_func

config_path = "/home/user/airflow/dags/config.yaml"
# config_path = r"C:\Users\Oleksandr\Documents\STUDY\robot_dreams\Homework\hw8_final_project\config.yaml"

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

try:
    with open(config_path) as f:
        pipeline_config = yaml.safe_load(f)
except Exception as e:
    logging.error(f'Error during loading pipeline config. Exception: {str(e)}')
    raise


dag = DAG(
    dag_id="dshop_etl_pipline",
    description="dshop ETL pipline",
    # schedule_interval="@daily",
    # start_date=datetime(2025, 8, 2),
        schedule_interval=None,
        start_date=datetime(2021, 11, 16),
    default_args=default_args
)


def load_dshop_to_bronze_group(table):
    return PythonOperator(
        dag=dag,
        task_id="load_"+table+"_to_bronze",
        python_callable=dshop_func.extract_tables_to_bronze,
        op_kwargs={"table": table, "bronze_root_dir": pipeline_config["hdfs"]["bronze_root_dir"]},
        provide_context=True
    )

load_out_of_stock_to_bronze = PythonOperator(
    dag=dag,
    task_id="load_out_of_stock_to_bronze",
    python_callable=out_of_stock_func.get_out_of_stock_and_save_to_bronze,
    op_kwargs={"out_of_stock_config": pipeline_config["out_of_stock"],
               "bronze_root_dir": pipeline_config["hdfs"]["bronze_root_dir"]},
    provide_context=True
)

load_data_to_silver = PythonOperator(
    dag=dag,
    task_id="load_data_to_silver",
    python_callable=data_process_func.load_data_to_silver,
    op_kwargs={ "bronze_root_dir": pipeline_config["hdfs"]["bronze_root_dir"],
                "silver_root_dir": pipeline_config["hdfs"]["silver_root_dir"]},
    provide_context=True
)

load_data_to_dwh =  PythonOperator(
    dag=dag,
    task_id="load_data_to_dwh",
    python_callable=data_process_func.load_data_to_dwh,
    op_kwargs={ "silver_root_dir": pipeline_config["hdfs"]["silver_root_dir"] },
    provide_context=True
)

pipeline_start = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

pipeline_finish = DummyOperator(
    task_id='finish_pipeline',
    dag=dag
)


for table in pipeline_config['dshop']['tables']:
    pipeline_start >> load_dshop_to_bronze_group(table) >> load_data_to_silver

pipeline_start >> load_out_of_stock_to_bronze >> load_data_to_silver
load_data_to_silver >> load_data_to_dwh >> pipeline_finish