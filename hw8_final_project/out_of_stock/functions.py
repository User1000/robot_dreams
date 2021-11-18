import requests
import os
import logging
import json
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

    url = config['api_root_url'] + config['endpoint']

    headers = {
        'Accept': 'application/json',
        'Authorization': token
    }

    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    payload = {
        'date': execution_date
    }

    try:
        r = requests.get(url, json=payload, headers=headers)
        r.raise_for_status()
    except Exception as e:
        logging.error(f'No data availlable for the date {execution_date}. {str(e)}')
        raise

    if r.ok:
        try:
            hdfs_conn = BaseHook.get_connection('datalake_hdfs')
            file_path = os.path.join(bronze_root_dir, execution_date, f'out_of_stock.json')
            with hdfs_client.write(file_path, encoding='utf-8', overwrite=True) as json_file:
                json.dump(r.json(), json_file)

            print(f'Results for out of stock products for {execution_date} were successfully retrieved and saved to {file_path}')
        except Exception as e:
            print(f"Error happened during saving results. {str(e)}")
            raise


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
