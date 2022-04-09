import os

import pandas as pd


def preprocess_mobile_data(file_path: str):
    df = pd.read_csv(file_path)

    df = df.rename(columns={'Mobile cellular subscriptions (per 100 people)': 'Subscriptions_per_100'})
    df = df.rename(columns={'Entity': 'Country'})

    print(f'Result of processing {file_path} is:')
    print(f'\n{df.head()}')

    df.to_csv(file_path, index=True, index_label='id')


def preprocess_gdp_data(file_path: str):
    df = pd.read_csv(file_path)

    df = df.drop(columns=['2019'])
    df = df.rename(columns={'Country ': 'Country'})

    available_years = df.columns[2:]  # 31 years
    available_years_df = pd.DataFrame(available_years, columns=['Year'])

    df_computed = df.merge(available_years_df, how='cross')
    df_computed = df_computed.drop(columns=df.columns[1:])

    concatenated_years = df.iloc[0][available_years]
    for i in range(1, df.shape[0]):
        concatenated_years = pd.concat((concatenated_years, df.iloc[i][available_years]), axis=0)

    concatenated_years = concatenated_years.reset_index()
    result = pd.concat((df_computed, concatenated_years), axis=1, ignore_index=True)

    result.columns = ['Country', 'Year', 'Year2', 'GDP']
    result = result.drop(columns=['Year2'])

    print(f'Result of processing {file_path} is:')
    print(f'\n{result.head()}')

    result.to_csv(file_path, index_label='id', index=True)


def preprocess_ed_stats_countries(file_path: str):
    df = pd.read_csv(file_path)

    columns_to_drop = [
        'Table Name', '2-alpha code', 'Country Code',
        'Special Notes', 'WB-2 code',
        'National accounts base year', 'National accounts reference year',
        'SNA price valuation', 'Lending category', 'Other groups',
        'System of National Accounts', 'Alternative conversion factor',
        'PPP survey year', 'Balance of Payments Manual in use',
        'External debt Reporting status', 'System of trade',
        'Government Accounting concept', 'IMF data dissemination standard',
        'Latest population census', 'Latest household survey',
        'Source of most recent Income and expenditure data',
        'Vital registration complete', 'Latest agricultural census',
        'Latest industrial data', 'Latest trade data',
        'Latest water withdrawal data', 'Unnamed: 31'
    ]

    df = df.drop(columns=columns_to_drop)

    df = df.rename(columns={
        'Short Name': 'Country',
        'Long Name': 'Full_country_name',
        'Currency Unit': 'Currency_unit',
        'Income Group': 'Income_group'
    })

    print(f'Result of processing {file_path} is:')
    print(f'\n{df.head()}')

    df.to_csv(file_path, index=True, index_label='id')


def join_data(gdp_file_path: str, mobile_file_path: str, countries_categories_file_path: str,
              processed_folder_path: str):
    print('Start processing and merging all data together')

    gdp = pd.read_csv(gdp_file_path).drop(columns=['id'])
    mobile_stats = pd.read_csv(mobile_file_path).drop(columns=['id'])
    countries_categories = pd.read_csv(countries_categories_file_path).drop(columns=['id'])

    df_computed = mobile_stats.merge(countries_categories, how='outer', on='Country')
    df_computed = df_computed.merge(gdp, how='outer', on=['Country', 'Year'])

    # country table
    country_table = pd.DataFrame(df_computed[['Country', 'Code', 'Full_country_name', 'Region', 'Income_group']]) \
        .drop_duplicates(subset=['Country']) \
        .reset_index() \
        .drop(columns=['index'])

    # year table
    year_table = pd.DataFrame(df_computed['Year'], columns=['Year']) \
        .drop_duplicates(subset=['Year']) \
        .dropna() \
        .reset_index() \
        .drop(columns=['index'])

    year_table['Year'] = year_table['Year'].astype(int)
    year_table['Century'] = year_table['Year'] // 100

    # currency table
    currency_table = pd.DataFrame(df_computed['Currency_unit']) \
        .drop_duplicates() \
        .reset_index() \
        .drop(columns=['index']) \
        .dropna()

    # region table
    region_table = pd.DataFrame(df_computed['Region']) \
        .drop_duplicates() \
        .reset_index() \
        .drop(columns=['index']) \
        .dropna()

    # income group table
    income_group_table = pd.DataFrame(df_computed['Income_group']) \
        .drop_duplicates() \
        .reset_index() \
        .drop(columns=['index']) \
        .dropna()

    fact_table = df_computed

    country_table['Region'] = country_table['Region'].apply(
        lambda reg: reg if pd.isna(reg) else region_table.index[region_table['Region'] == reg][0]
    )

    country_table['Income_group'] = country_table['Income_group'].apply(
        lambda income: income if pd.isna(income) else
        income_group_table.index[income_group_table['Income_group'] == income][0]
    )

    fact_table['Country'] = fact_table['Country'].apply(
        lambda country: country if pd.isna(country) else country_table.index[country_table['Country'] == country][0]
    )

    fact_table = fact_table.drop(columns=['Code', 'Full_country_name', 'Income_group', 'Region'])

    fact_table['Year'] = fact_table['Year'].apply(
        lambda year: year if pd.isna(year) else year_table.index[year_table['Year'] == year][0]
    )

    fact_table['Currency_unit'] = fact_table['Currency_unit'].apply(
        lambda currency: currency if pd.isna(currency) else
        currency_table.index[currency_table['Currency_unit'] == currency][0]
    )

    country_table.to_csv(os.path.join(processed_folder_path, 'countries.csv'), index=True, index_label='id')
    year_table.to_csv(os.path.join(processed_folder_path, 'years.csv'), index=True, index_label='id')
    currency_table.to_csv(os.path.join(processed_folder_path, 'currencies.csv'), index=True, index_label='id')
    region_table.to_csv(os.path.join(processed_folder_path, 'regions.csv'), index=True, index_label='id')
    income_group_table.to_csv(os.path.join(processed_folder_path, 'income_groups.csv'), index=True, index_label='id')

    fact_table.to_csv(os.path.join(processed_folder_path, 'facts.csv'), index=True, index_label='id')

    print('Processing and merging data is done.')