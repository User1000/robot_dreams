import psycopg2
import logging
import os

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook

import pyspark
from pyspark.sql import SparkSession


def load_to_bronze(table):

    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    pg_conn = BaseHook.get_connection('oltp_postgres')
    pg_creds = {
        'host': pg_conn.host,
        'port': pg_conn.port,
        'user': pg_conn.login,
        'password': pg_conn.password,
        'database': 'postgres'
    }

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")
    client = InsecureClient("http://"+hdfs_conn.host, user=hdfs_conn.login)
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join('/', 'new_datalake', 'bronze', table)) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
    logging.info("Successfully loaded")


def load_to_bronze_spark(table):

    pg_conn = BaseHook.get_connection('oltp_postgres')

    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/postgres"
    pg_properties = {"user": pg_conn.login, "password": pg_conn.password}

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('lesson_14') \
        .getOrCreate()

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")
    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)
    table_df.write.parquet(
        os.path.join('/', 'new_datalake', 'bronze', table),
        mode="overwrite")

    logging.info("Successfully loaded")

def load_to_silver(table, **kwargs):
    ds = kwargs.get('ds', str(date.today()))

    spark = SparkSession.builder\
            .master('local')\
            .appName('load_to_silver')\
            .getOrCreate()

    dshop_dfs = {}
    df = spark.read\
            .option(header, True)\
            .option('inferSchema', True)\
            .csv(os.path.join('/', 'dataLake', 'bronze', 'dshop', table))
    
    df.distinct()\
        .write.parquet(os.path.join('/', 'dataLake', 'silver', 'dshop', table))