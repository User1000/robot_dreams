from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy.inspection import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine import Engine

from services.hdfs_service import export_table_from_pg_to_hdfs

postgres_conn_id = "postgres_dshop"
dump_path = "/home/user/dshop_dump"

with DAG(
        dag_id="dump_db_to_hadoop_dag",
        schedule_interval=None,
        start_date=days_ago(2),
) as dag:
    pg_hook = PostgresHook(postgres_conn_id)
    pg_engine: Engine = pg_hook.get_sqlalchemy_engine()
    pg_inspector: Inspector = inspect(pg_engine)

    tasks = []
    for table_name in pg_inspector.get_table_names():
        tasks.append(
            PythonOperator(
                task_id=f"export_table_to_hdfs__{table_name}",
                python_callable=export_table_from_pg_to_hdfs,
                op_kwargs={
                    "table_name": table_name,
                    "pg_engine": pg_engine
                }
            )
        )


    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> tasks >> end