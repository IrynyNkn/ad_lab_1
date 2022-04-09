import os

import config as conf
import common.processors as processors
from common.functions import download_and_unzip, data_lake_cleanup, load_csv_into_postgres


def run_pipeline():
    # download datasets
    for dataset_name in conf.DATASET_NAMES:
        download_and_unzip(dataset_name, conf.DATA_LAKE_PATH)

    # deleting useless files
    data_lake_cleanup(conf.DATA_LAKE_PATH, conf.NEEDED_FILES)

    gdp_file_path = os.path.join(conf.DATA_LAKE_PATH, 'GDP.csv')
    mobile_stats_file_path = os.path.join(conf.DATA_LAKE_PATH, 'mobile-cellular-subscriptions-per-100-people.csv')
    countries_categories_file_path = os.path.join(conf.DATA_LAKE_PATH, 'edstats-csv-zip-32-mb-/EdStatsCountry.csv')

    # preprocess incoming data
    processors.preprocess_gdp_data(gdp_file_path)
    processors.preprocess_mobile_data(mobile_stats_file_path)
    processors.preprocess_ed_stats_countries(countries_categories_file_path)

    # loading data into staging zone
    load_csv_into_postgres(gdp_file_path, 'gdp_data_stage')
    load_csv_into_postgres(countries_categories_file_path, 'country_categories_data_raw')
    load_csv_into_postgres(mobile_stats_file_path, 'mobile_stats_data_stage')

    # processing and joining data
    processors.join_data(gdp_file_path, mobile_stats_file_path, countries_categories_file_path, conf.PROCESSED_FOLDER_PATH)

    # load data into dimension tables
    load_csv_into_postgres(os.path.join(conf.PROCESSED_FOLDER_PATH, 'regions.csv'), 'region')
    load_csv_into_postgres(os.path.join(conf.PROCESSED_FOLDER_PATH, 'income_groups.csv'), 'income_group')
    load_csv_into_postgres(os.path.join(conf.PROCESSED_FOLDER_PATH, 'countries.csv'), 'country')
    load_csv_into_postgres(os.path.join(conf.PROCESSED_FOLDER_PATH, 'years.csv'), 'year')
    load_csv_into_postgres(os.path.join(conf.PROCESSED_FOLDER_PATH, 'currencies.csv'), 'currency')

    # load data into fact table
    load_csv_into_postgres(os.path.join(conf.PROCESSED_FOLDER_PATH, 'facts.csv'), 'fact')


if __name__ == '__main__':
    run_pipeline()
