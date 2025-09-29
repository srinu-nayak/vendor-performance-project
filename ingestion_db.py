import numpy as np
import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging



logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
  df.to_sql(table_name, engine, index=False, if_exists='replace')  
    
def load_raw_data():
    '''This function will load the CSVs as DataFrames and ingest into DB'''
    start = time.time()
    data_dir = 'data'

    for file in os.listdir(data_dir):
        try:
            if file.endswith('.csv'):
                file_path = os.path.join(data_dir, file)
                df = pd.read_csv(file_path)
                logging.info(f'Ingesting {file} into DB...')
                ingest_db(df, file[:-4], engine)  # remove .csv from filename
        except Exception as e:
            logging.error(f'Error processing {file}: {e}')

    end = time.time()
    total_time = (end - start) / 60
    logging.info('------------------ Ingestion Complete ------------------')
    logging.info(f'Ingestion completed in {total_time:.2f} minutes')


# if __name__ == '__main__':
#     load_raw_data()