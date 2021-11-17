import os
import yaml
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

import dshop.functions as dshop_func
import out_of_stock.functions as out_of_stock_func

config_path = "/home/user/airflow/dags/stock/config.yaml"
config_path = r"C:\Users\Oleksandr\Documents\STUDY\robot_dreams\Homework\hw8_final_project\config.yaml"

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


def load_dshop_to_bronze_group(table, hdfs):
    return PythonOperator(
        task_id="load_"+table+"_to_bronze",
        python_callable=dshop_func.extract_tables_to_bronze,
        op_kwargs={"table": table, "bronze_root_dir": hdfs["bronze_root_dir"]}
    )

def get_out_of_stock_and_save_to_bronze(out_of_stock_config):
    return PythonOperator(
        task_id="out_of_stock_to_bronze",
        python_callable=out_of_stock_func.get_out_of_stock_and_save_to_bronze,
        op_kwargs={"out_of_stock_config": out_of_stock_config}
    )

def load_dshop_to_silver(table, hdfs):
    return PythonOperator(
        task_id="load_"+table+"_to_silver",
        python_callable=dshop_func.transform_tables_to_silver,
        op_kwargs={ "table": table,
                    "bronze_root_dir": hdfs["bronze_root_dir"],
                    "silver_root_dir": hdfs["silver_root_dir"]}
    )

def load_out_of_stock_to_silver(hdfs):
    return PythonOperator(
        task_id="load_out_of_stock_to_silver",
        python_callable=out_of_stock_func.transform_out_of_stock_to_silver,
        op_kwargs={ "bronze_root_dir": hdfs["bronze_root_dir"],
                    "silver_root_dir": hdfs["silver_root_dir"]}
    )



dag = DAG(
    dag_id="dshop_etl_pipline",
    description="dshop ETL pipline",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 2),
    default_args=default_args
)

pipeline_start = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)
load_to_bronze_finish = DummyOperator(
    task_id='load_to_bronze_finish',
    dag=dag
)
pipeline_finish = DummyOperator(
    task_id='finish_pipeline',
    dag=dag
)

for table in pipeline_config['dshop']['tables']:
    pipeline_start >> load_dshop_to_bronze_group(table, pipeline_config['hdfs']) >> load_to_bronze_finish
    load_to_bronze_finish >> load_dshop_to_silver(table, pipeline_config['hdfs']) >> pipeline_finish

pipeline_start >> get_out_of_stock_and_save_to_bronze(pipeline_config['out_of_stock']) >> load_to_bronze_finish
load_to_bronze_finish >> load_out_of_stock_to_silver(pipeline_config['hdfs']) >> pipeline_finish