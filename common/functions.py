import os
import glob
import kaggle
import psycopg2


def download_and_unzip(dataset_name: str, datalake_folder_path: str):
    print(f'Downloading dataset {dataset_name}')

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_name, path=datalake_folder_path, unzip=True, force=True)

    print(f'Dataset {dataset_name} was successfully saved into {datalake_folder_path}')


def data_lake_cleanup(data_lake_path: str, file_names_to_keep: list):
    for file_path in glob.glob(data_lake_path + '/**/*', recursive=True):
        if not any([filename in file_path for filename in file_names_to_keep]):
            if os.path.isfile(file_path):
                os.remove(file_path)


def load_csv_into_postgres(csv_file_path, destination_table_id, delimiter=','):
    connection = psycopg2.connect(host='localhost', port=5432, user='postgres', dbname='da_lab', password='1234')
    cursor = connection.cursor()

    print(f'Start moving {csv_file_path} into postgres table: {destination_table_id}')

    copy_sql = f"""
                   TRUNCATE TABLE {destination_table_id} CASCADE;
                   COPY {destination_table_id} FROM stdin
                   WITH CSV HEADER
                   DELIMITER as '{delimiter}'
               """

    with open(csv_file_path, 'r') as csv_file:
        cursor.copy_expert(sql=copy_sql, file=csv_file)
        connection.commit()
        cursor.close()

    print(f'Successfully copied data from {csv_file_path} into {destination_table_id} table')
