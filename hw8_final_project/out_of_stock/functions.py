import requests
import os
import logging
from urllib.error import HTTPError

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook


def get_out_of_stock_and_save_to_bronze(out_of_stock_config, bronze_root_dir, **kwargs):

    config = out_of_stock_config
    hdfs_conn = BaseHook.get_connection('datalake_hdfs')

    token = get_token(config)

    if not token:
        logging.error("No valid token found. Exit...")
        exit()

    hdfs_client = InsecureClient(f"http://{hdfs_conn.host}:{hdfs_conn.port}", user=hdfs_conn.login)

    url = config['api_root_url'] + config['out_of_stock']['endpoint']

    headers = {
        'Accept': 'application/json',
        'Authorization': token
    }

    execution_date = kwargs['execution_date']
    payload = {
        'date': execution_date.strftime("%Y-%m-%d")
    }

    try:
        r = requests.get(url, json=payload, headers=headers)
        r.raise_for_status()
    except Exception as e:
        logging.error(f'No data availlable for the date {execution_date.strftime("%Y-%m-%d")}. {str(e)}')

    if r.ok:
        try:
            storage_dir_path = os.path.join(bronze_root_dir, 'dshop', execution_date.strftime("%Y-%m-%d"))
            hdfs_conn = BaseHook.get_connection('datalake_hdfs')
            hdfs_client.mkdirs(storage_dir_path)
            
            file_path = os.path.join(storage_dir_path, f'{execution_date}_out_of_stock.json')
            with hdfs_client.write(file_path) as json_file:
                json_file.write(str(r.json()))

            print(f'Results for out of stock products for {execution_date} were successfully retrieved and saved to {file_path}')
        except Exception as e:
            print(f"Error happened during saving results. {str(e)}")


def get_token(config):

    url = config['api_root_url'] + config['auth']['endpoint']
    payload = {
        'username': config['auth']['username'],
        'password': config['auth']['password']
    }

    try:
        r = requests.post(url, json=payload)
        r.raise_for_status()
    except Exception as e:
        print(f"Error happened during http request. {str(e)}")

    token = None
    if r.ok:
        token = f'JWT {r.json()["access_token"]}'

    return token


# def load_to_silver(**kwargs):
    
#     execution_date = kwargs['execution_date'] - timedelta(days=1)
#     storage_bronze_dir_path = f'{bronze_zone_path}/{execution_date.strftime("%Y-%m-%d")}'
#     file_path = os.path.join(storage_bronze_dir_path, f'{execution_date}_out_of_stock.json')

#     logging.info(f"Writing table {table} from {pg_conn.host} to Silver")
    
#     spark = SparkSession.builder\
#             .master('local')\
#             .appName('load_to_silver')\
#             .getOrCreate()

#     df = spark.read.json(file_path)
    
    
#     storage_silver_dir_path = f'{silver_zone_path}/{execution_date.strftime("%Y-%m-%d")}'
#     file_path = os.path.join(storage_silver_dir_path, f'{execution_date}_out_of_stock.json')

#     df.distinct()\
#         .write.parquet(os.path.join('/', 'silver', 'dshop', table))
    
#     logging.info("Successfully moved to silver")