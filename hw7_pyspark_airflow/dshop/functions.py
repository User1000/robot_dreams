import psycopg2
import logging
import os

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook

import pyspark
from pyspark.sql import SparkSession


def load_to_bronze(table):

    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    pg_conn = BaseHook.get_connection('postgres_dshop')
    pg_creds = {
        'host': pg_conn.host,
        'port': pg_conn.port,
        'user': pg_conn.login,
        'password': pg_conn.password,
        'database': 'postgres'
    }

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")
    client = InsecureClient(f"http://{hdfs_conn.host}:{hdfs_conn.port}", user=hdfs_conn.login)
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join('/', 'bronze', 'dshop', table)) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
    logging.info("Successfully loaded")


def load_to_silver(table):
    logging.info(f"Writing table {table} from {pg_conn.host} to Silver")
    
    spark = SparkSession.builder\
            .master('local')\
            .appName('load_to_silver')\
            .getOrCreate()

    dshop_dfs = {}
    df = spark.read\
            .option(header, True)\
            .option('inferSchema', True)\
            .csv(os.path.join('/', 'bronze', 'dshop', table))
    
    df.distinct()\
        .write.parquet(os.path.join('/', 'silver', 'dshop', table))
    logging.info("Successfully moved to silver")