import requests # pull data from api
import pandas as pd 
from sqlalchemy import create_engine # to create a connection to a sqlite database

def extract() -> dict:
    """
    This API will extract data from http://universities.hipolabs.com
    """

    API_URL = "http://universities.hipolabs.com/search?country=Germany"
    data = requests.get(API_URL).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    """Transform the data set"""
    df = pd.DataFrame(data)
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']] # without this sqlite throws up an error: it expects strings, not lists
    print(f"The total number of universities extracted is {len(data)}")
    print(df.dtypes)
    
    # return df[["domains", "country", "web_pages", "name"]].head()
    return df[["name", "country", "web_pages", "domains" ]] #.head()


# print(transform(extract()))


def load(df: pd.DataFrame) -> None:
    """Load this data into an SQLite database"""
    db_path = '/Users/trolliusmaximus/Documents/etl_pipeline/uni_info.db'
    disk_engine = create_engine(f'sqlite:///{db_path}')
    try:
        df.to_sql('de_uni', disk_engine, if_exists='replace', index=False)
        print("Data loaded successfully into 'de_uni' table.")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")



data = extract()
df = transform(data)
#print(df.dtypes)
load(df)