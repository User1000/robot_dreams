import psycopg2
import logging
import os

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook

import pyspark
from pyspark.sql import SparkSession


def extract_tables_to_bronze(table, bronze_root_dir, **kwargs):

    execution_date = kwargs['execution_date']

    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    
    pg_conn = BaseHook.get_connection('postgres_dshop')
    pg_creds = {
        'host': pg_conn.host,
        'port': pg_conn.port,
        'user': pg_conn.login,
        'password': pg_conn.password,
        'database': pg_conn.schema
    }

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")
    
    client = InsecureClient(f"http://{hdfs_conn.host}:{hdfs_conn.port}", user=hdfs_conn.login)
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join(bronze_root_dir, execution_date.strftime("%Y-%m-%d"), f'{table}.csv'), overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
    
    logging.info(f"Table {table} successfully loaded to bronze")
