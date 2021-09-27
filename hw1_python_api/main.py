import requests
import yaml
import os
from urllib.error import HTTPError

def get_out_of_stock(config):
    token = get_token(config)

    if not token:
        print("No valid found. Exit...")
        exit()

    url = config['api_root_url'] + config['out_of_stock']['endpoint']
    
    for date in config['dates']:
        headers = {
            'Accept': 'application/json',
            'Authorization': token
        }

        payload = {
            'date': date
        }

        try:
            r = requests.get(url, json=payload, headers=headers)
            r.raise_for_status()
        except Exception as e:
            print(f"Error happened during http request. {str(e)}")

        if r.ok:
            try:
                dir_path = os.path.join(config['storage_dir'], date)
                os.makedirs(dir_path, exist_ok = True)

                file_path = os.path.join(dir_path, 'out_of_stock.json')
                with open(file_path, 'w') as f:
                    f.write(str(r.json()))

                print(f'Results for out of stock products for {date} were successfully retrieved and saved to {file_path}')
            except Exception as e:
                print(f"Error during saving results. {str(e)}")


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


if __name__ == "__main__":
    with open('./config.yaml') as f:
        config = yaml.safe_load(f)
    
    get_out_of_stock(config)