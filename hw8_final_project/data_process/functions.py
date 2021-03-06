
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os
from datetime import datetime
import logging
from airflow.hooks.base_hook import BaseHook


def load_data_to_silver(bronze_root_dir, silver_root_dir, **kwargs):
    logging.info(f"Process dataset to Silver")

    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")

    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/mnt/share/postgresql-42.3.1.jar')\
        .master('local')\
        .appName("lesson")\
        .getOrCreate()

    ### Read tables from bronze ###

    schema = StructType() \
      .add("aisle_id", IntegerType(), True) \
      .add("aisle", StringType(), True) 

    aisles_df = spark.read\
            .option('header', True)\
            .schema(schema)\
            .csv(os.path.join(bronze_root_dir, execution_date, 'aisles.csv'))

    schema = StructType() \
        .add("client_id", IntegerType(), True)\
        .add("fullname", StringType(), True)\
        .add("location_area_id", IntegerType(), True)
        
    clients_df = spark.read\
        .option('header', True)\
        .schema(schema)\
        .csv(os.path.join(bronze_root_dir, execution_date, 'clients.csv'))

    schema = StructType() \
        .add("department_id", IntegerType(), True)\
        .add("department", StringType(), True)

    departments_df = spark.read\
        .option('header', True)\
        .schema(schema)\
        .csv(os.path.join(bronze_root_dir, execution_date, 'departments.csv'))

    schema = StructType() \
            .add("order_id", IntegerType(), True)\
            .add("product_id", IntegerType(), True)\
            .add("client_id", IntegerType(), True)\
            .add("store_id", IntegerType(), True)\
            .add("quantity", IntegerType(), True)\
            .add("order_date", DateType(), True)
        
    orders_df = spark.read\
        .option('header', True)\
        .schema(schema)\
        .csv(os.path.join(bronze_root_dir, execution_date, 'orders.csv'))

    schema = StructType() \
            .add("product_id", IntegerType(), True)\
            .add("product_name", StringType(), True)\
            .add("aisle_id", IntegerType(), True)\
            .add("department_id", IntegerType(), True)\

    products_df = spark.read\
        .option('header', True)\
        .schema(schema)\
        .csv(os.path.join(bronze_root_dir, execution_date, 'products.csv'))

    schema = StructType() \
            .add("product_id", IntegerType(), True)\
            .add("date", DateType(), True)
        
    out_of_stock_df = spark.read\
        .schema(schema)\
        .json(os.path.join(bronze_root_dir, execution_date, 'out_of_stock.json'))

    ### Transform and cleanup dataset ###

    clients_cleaned_df = clients_df.distinct()
    products_cleaned_df = products_df.distinct()
    aisles_cleaned_df = aisles_df.distinct()
    departments_cleaned_df = departments_df.distinct()

    dates_cleaned_df = orders_df.selectExpr('order_date as date')\
        .unionByName(out_of_stock_df.select('date'))\
        .withColumn('action_date', F.col('date'))\
        .withColumn('action_week', F.weekofyear('date'))\
        .withColumn('action_month', F.month('date'))\
        .withColumn('action_year', F.year('date'))\
        .withColumn('action_weekday', F.dayofweek('date'))\
        .drop('date')\
        .distinct()\
        .withColumn('time_id', F.monotonically_increasing_id())


    location_areas_cleaned_df = (clients_df
        .select('location_area_id')
        .distinct())

    orders_cleaned_df = (orders_df
        .join(dates_cleaned_df.selectExpr('time_id', 'action_date as order_date'), 'order_date')
        .drop('order_date')
        .distinct())

    out_of_stock_cleaned_df = (out_of_stock_df
        .join(dates_cleaned_df.selectExpr('time_id', 'action_date as date'), 'date')
        .drop('date')
        .distinct())

    ### Write dataset to silver ###

    clients_cleaned_df.write.mode('overwrite').parquet(os.path.join(silver_root_dir, execution_date, 'clients.parquet'))
    products_cleaned_df.write.mode('overwrite').parquet(os.path.join(silver_root_dir, execution_date, 'products.parquet'))
    aisles_cleaned_df.write.mode('overwrite').parquet(os.path.join(silver_root_dir, execution_date, 'aisles.parquet'))
    departments_cleaned_df.write.mode('overwrite').parquet(os.path.join(silver_root_dir, execution_date, 'departments.parquet'))
    dates_cleaned_df.write.mode('overwrite').parquet(os.path.join(silver_root_dir, execution_date, 'dates.parquet'))
    location_areas_cleaned_df.write.mode('overwrite').parquet(os.path.join(silver_root_dir, execution_date, 'location_areas.parquet'))
    orders_cleaned_df.write.mode('overwrite').parquet(os.path.join(silver_root_dir, execution_date, 'orders.parquet'))
    out_of_stock_cleaned_df.write.mode('overwrite').parquet(os.path.join(silver_root_dir, execution_date, 'out_of_stock.parquet'))


    logging.info("Successfully moved dataset to silver")


def load_data_to_dwh(silver_root_dir, dwh_connection_id, **kwargs):
    logging.info(f"Loading dataset to DWH...")

    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")

    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/mnt/share/postgresql-42.3.1.jar')\
        .master('local')\
        .appName("lesson")\
        .getOrCreate()

    gp_conn = BaseHook.get_connection(dwh_connection_id)
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user": gp_conn.login, "password": gp_conn.password}

    ### Read tables from silver ###
    clients_df = spark.read.parquet(os.path.join(silver_root_dir, execution_date, 'clients.parquet'))
    products_df = spark.read.parquet(os.path.join(silver_root_dir, execution_date, 'products.parquet'))
    aisles_df = spark.read.parquet(os.path.join(silver_root_dir, execution_date, 'aisles.parquet'))
    departments_df = spark.read.parquet(os.path.join(silver_root_dir, execution_date, 'departments.parquet'))
    dates_df = spark.read.parquet(os.path.join(silver_root_dir, execution_date, 'dates.parquet'))
    location_areas_df = spark.read.parquet(os.path.join(silver_root_dir, execution_date, 'location_areas.parquet'))
    orders_df = spark.read.parquet(os.path.join(silver_root_dir, execution_date, 'orders.parquet'))
    out_of_stock_df = spark.read.parquet(os.path.join(silver_root_dir, execution_date, 'out_of_stock.parquet'))


    ### Write table to DWH ###
    logging.info("Loading data to DWH...")
    logging.info("Writing clients_dim to dwh...")
    clients_df.write.mode('append').jdbc(gp_url, table="clients_dim", properties=gp_creds)
    logging.info("Done")

    logging.info("Writing products_dim to dwh...")
    products_df.write.mode('append').jdbc(gp_url, table="products_dim", properties=gp_creds)
    logging.info("Done")

    logging.info("Writing aisles_dim to dwh...")
    aisles_df.write.mode('append').jdbc(gp_url, table="aisles_dim", properties=gp_creds)
    logging.info("Done")
    
    logging.info("Writing departments_dim to dwh...")
    departments_df.write.mode('append').jdbc(gp_url, table="departments_dim", properties=gp_creds)
    logging.info("Done")
    
    logging.info("Writing dates_dim to dwh...")
    dates_df.write.mode('append').jdbc(gp_url, table="dates_dim", properties=gp_creds)
    logging.info("Done")
    
    logging.info("Writing location_areas_dim to dwh...")
    location_areas_df.write.mode('append').jdbc(gp_url, table="location_areas_dim", properties=gp_creds)
    logging.info("Done")
    
    logging.info("Writing orders_fact to dwh...")
    orders_df.write.mode('append').jdbc(gp_url, table="orders_fact", properties=gp_creds)
    logging.info("Done")
    
    logging.info("Writing out_of_stock_fact to dwh...")
    out_of_stock_df.write.mode('append').jdbc(gp_url, table="out_of_stock_fact", properties=gp_creds)
    logging.info("Done")

    logging.info(f"Successfully loaded dataset to DWH!")
