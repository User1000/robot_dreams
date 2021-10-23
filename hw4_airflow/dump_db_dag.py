from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from airflow.utils.dates import days_ago

from sqlalchemy.inspection import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine import Engine


postgres_conn_id = "postgres_dshop"
dump_path = "/home/user/dshop_dump"

with DAG(
        dag_id="dump_db_dag",
        schedule_interval=None,
        start_date=days_ago(2),
) as dag:
    pg_hook = PostgresHook(postgres_conn_id)
    pg_engine: Engine = pg_hook.get_sqlalchemy_engine()
    pg_inspector: Inspector = inspect(pg_engine)

    table_names = pg_inspector.get_table_names()

    tables_dump_tasks = []
    for table_name in table_names:
        tables_dump_tasks.append(
            PostgresOperator(
                task_id=f'dump_{table_name}',
                postgres_conn_id=postgres_conn_id,
                sql=f"COPY {table_name} TO '{dump_path}/{table_name}.csv' DELIMITER ',' CSV HEADER;"
            )
        )


    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> tables_dump_tasks >> end