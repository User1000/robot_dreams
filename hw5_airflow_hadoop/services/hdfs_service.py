from hdfs import InsecureClient
import yaml

hdfs_client = InsecureClient('http://192.168.1.11:50070/', user='user')
bronze_zone_path = '/bronze/dshop'


def export_table_from_pg_to_hdfs(table_name, pg_engine):

    hdfs_client.makedirs(bronze_zone_path)

    connection = pg_engine.connect().connection
    cursor = connection.cursor()
    with hdfs_client.write(f'{bronze_zone_path}/{table_name}.csv') as csv_file:
        print('Executing:', f'COPY {table_name} TO STDOUT WITH HEADER CSV')
        cursor.copy_expert(f'COPY {table_name} TO STDOUT WITH HEADER CSV', csv_file)
    
    connection.commit()
