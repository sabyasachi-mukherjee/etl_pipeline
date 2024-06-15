from sqlalchemy import create_engine, text
import pandas as pd

# Define the SQLite database path
db_path = '/Users/trolliusmaximus/Documents/etl_pipeline/uni_info.db'

# Create SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_path}')

# Sample Query to check if we can query data successfully
query = text('SELECT * FROM de_uni')

# Execute the query and fetch results into a DataFrame
try:
    with engine.connect() as connection:
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df)
except Exception as e:
    print(f"An error occurred: {e}")


print(df.head())
