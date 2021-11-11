import requests
import yaml
import os
from urllib.error import HTTPError
from datetime import timedelta
from hdfs import InsecureClient
import yaml

hdfs_client = InsecureClient('http://192.168.1.11:50070/', user='user')
bronze_zone_path = '/bronze/stock/out_of_stock'

def get_out_of_stock_and_save_to_hadoop(config_path, **kwargs):
    with open(config_path) as f:
        config = yaml.safe_load(f)

    token = get_token(config)

    if not token:
        print("No valid token found. Exit...")
        exit()

    url = config['api_root_url'] + config['out_of_stock']['endpoint']

    headers = {
        'Accept': 'application/json',
        'Authorization': token
    }

    request_date = kwargs['execution_date'] - timedelta(days=1)
    payload = {
        'date': request_date.strftime("%Y-%m-%d")
    }

    try:
        r = requests.get(url, json=payload, headers=headers)
        r.raise_for_status()
    except Exception as e:
        print(f"Error happened during http request. {str(e)}")

    if r.ok:
        try:
            storage_dir_path = f'{bronze_zone_path}/{request_date.year}/{request_date.month}'
            hdfs_client.mkdirs(storage_dir_path)
            
            file_path = os.path.join(storage_dir_path, f'{request_date}_out_of_stock.json')
            with hdfs_client.write(file_path) as json_file:
                json_file.write(str(r.json()))

            print(f'Results for out of stock products for {request_date} were successfully retrieved and saved to {file_path}')
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