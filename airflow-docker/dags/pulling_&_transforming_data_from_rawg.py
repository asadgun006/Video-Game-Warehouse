from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests
from sqlalchemy import create_engine
import pandas as pd

"""
The function responsible for parsing the paginated API and transforming the data a little. A counter 
is initialized before the loop as page number 1 and an empty list is defined. The loop goes over until 
the API response does not have a next page value. All the results are appended to a list for further 
transformation.

One technical complication regarding the API is that one API call only returns 250 pages of data.
As a result, whenever the page 250 was reached for a single API call, the date parameter was updated and the
counter was reset within the loop to create a different search query. That is why the date_start method 
parameter is a string, and the year_end is an int as the end date search parameter remains the same.
"""
def game_data(date_start: str, year_end: int):
    games_list = []
    count = 1
    while True:
        try:
            data = requests.get(f"https://api.rawg.io/api/games?key=<key>&ordering=released&dates={date_start},{year_end}-12-31&page={count}&page_size=50&parent_platforms=1,2,3").json()
            for i in data['results']:
                games_list.append(i)
        except Exception as e:
            print(e)

        if data['next'] is None:
            break
        if count % 50 == 0:
            print(f"{count} pages parsed. {count*40} entries extracted from {date_start} to {year_end}")
        if count == 250:
            date_start = data['results'][-1].get('released')
            count = 0
        count += 1

    #Loop for creating new columns for different ratings count for each game
    for p in games_list:
        try:
            p['exceptional_rating_count'] = p['ratings'][0].get('count')
        except IndexError:
            p['exceptional_rating_count'] = 0
        try:
            p['recommended_rating_count'] = p['ratings'][1].get('count')
        except IndexError:
            p['recommended_rating_count'] = 0
        try:
            p['meh_rating_count'] = p['ratings'][2].get('count')
        except IndexError:
            p['meh_rating_count'] = 0
        try:
            p['skip_rating_count'] = p['ratings'][3].get('count')
        except IndexError:
            p['skip_rating_count'] = 0

    #Some keys are removed for better storage and analysis
    keys_to_remove = ['suggestions_count', 'score', 'added_by_status', 'ratings',
                      'reviews_text_count', 'added', 'parent_platforms', 'tags', 'tba', 'slug',
                      'background_image', 'clip', 'short_screenshots', 'user_game', 'saturated_color',
                      'dominant_color', 'community_rating']
    for i in keys_to_remove:
        for v in games_list:
            v.pop(i, None)

    #function for extracting values defined within a dictionary
    def value_extraction(value):
        list_to_store = []
        for i in value:
            for v in i.values():
                list_to_store.append(v.get('name'))
        return list_to_store


    #The loop transforms some game characteristics into comma separated values by applying the
    #value_extraction() method on each key
    for i in games_list:
        try:
            i['platforms'] = ", ".join(value_extraction(i['platforms']))
        except Exception as e:
            i['platforms'] = i['platforms']
        try:
            i['stores'] = ", ".join(value_extraction(i['stores']))
        except Exception as e:
            i['stores'] = i['stores']
        try:
            i['esrb_rating'] = i['esrb_rating'].get('name')
        except Exception as e:
            i['esrb_rating'] = i['esrb_rating']
        genres = []
        try:
            for genre in i['genres']:
                genres.append(genre.get('name'))
            i['genres'] = ", ".join(genres)
        except Exception as e:
            i['genres'] = i['genres']

    #A dataframe is created for SQL storage, and data is stored through SQL Alchemy and pandas.
    df = pd.DataFrame(data=games_list)
    df['genres'] = df['genres'].replace('', None)
    print(df.head(10).to_string())
    engine = create_engine("postgresql://<user>:<password>@rawg-data-postgres.postgres.database.azure.com:5432/postgres")
    df.to_sql('rawg_extracted_data', engine, if_exists='append')


"""
This dag is responsible for extracting all the game data from 2000-2022. The dag creates three tasks due to
the nature of game data available. The game data between 2000-2015 had ~37000 entries. Between 2016-2022 there
were ~220000 entries, same as data between 2021-2022. Three tasks dependancies are defined to extract the data
in order.
"""

default_args = {
    'retries':0
}

with DAG(
    default_args=default_args,
    dag_id='Data_extract_transform_rawg',
    description='Main Dag',
    start_date=datetime(2023, 11, 10, 2),
    schedule_interval=None
) as dag:
    first_batch = PythonOperator(task_id = 'first_rawg_data_batch',python_callable = game_data,
                                 op_args = ['2000-01-01', 2015])
    second_batch = PythonOperator(task_id = 'second_rawg_data_batch',python_callable = game_data,
                                 op_args = ['2016-01-01', 2020])
    third_batch = PythonOperator(task_id = 'third_rawg_data_batch',python_callable = game_data,
                                 op_args = ['2021-01-01', 2022])
    first_batch >> second_batch >> third_batch