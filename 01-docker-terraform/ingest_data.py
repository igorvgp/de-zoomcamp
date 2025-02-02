import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_trips_name = args.table_trips_name
    url_trips = args.url_trips
    url_zone = args.url_zone
    csv_trips_name = args.csv_trips_name
    table_zone_name = args.table_zone_name
    filename_zone = args.filename_zone

    # Connect to postgres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    ################### Trips data ###################
    # Get the data from url and save it locally
    os.system(f"wget {url_trips}")
    os.system(f"gzip -d {csv_trips_name}.gz")

    #df_parquet = pd.read_parquet(parquet_trips_name)
    #df_parquet.to_csv(csv_trips_name, index = False)
    df_iter = pd.read_csv(csv_trips_name, iterator = True,  chunksize = 100000, low_memory=False)

    # first, create the table without data
    df_schema = pd.read_csv(csv_trips_name,  nrows = 1)
    df_schema['lpep_pickup_datetime'] = pd.to_datetime(df_schema['lpep_pickup_datetime'])
    df_schema['lpep_dropoff_datetime'] = pd.to_datetime(df_schema['lpep_dropoff_datetime'])
    df_schema.iloc[:, 3] = df_schema.iloc[:, 3].astype(str)
    df_schema.head(0).to_sql(table_trips_name, engine, if_exists='replace', index=False)

    # Then, insert data into it
    for df_ in df_iter:
        df_['lpep_pickup_datetime'] = pd.to_datetime(df_['lpep_pickup_datetime'])
        df_['lpep_dropoff_datetime'] = pd.to_datetime(df_['lpep_dropoff_datetime'])
        df_.iloc[:, 3] = df_.iloc[:, 3].astype(str)
        df_.to_sql(table_trips_name, engine, if_exists='append', index=False)
        print(f"{len(df_)} rows inserted")
    
    ################### Zone data ###################
    # Get the data from url an save it locally
    os.system(f"wget {url_zone}")
    df = pd.read_csv(filename_zone)
    df.to_sql(table_zone_name, engine, if_exists='replace', index=False)

if __name__== '__main__': 
    # Get parameters
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help = 'user name for postgres')
    parser.add_argument('--password', help = 'password for postgres')
    parser.add_argument('--host', help = 'host for postgres')
    parser.add_argument('--port', help = 'port for postgres')
    parser.add_argument('--db', help = 'database name for postgres')
    parser.add_argument('--table_trips_name', help = 'name of the table where we will write the trips data')
    parser.add_argument('--url_trips', help = 'url of the csv trips file')
    parser.add_argument('--url_zone', help = 'url of the csv zone file')
    parser.add_argument('--csv_trips_name', help = 'csv trips name')
    parser.add_argument('--table_zone_name', help = 'name of the table where we will write the zone data')
    parser.add_argument('--filename_zone', help = 'name of the csv zone file')

    args = parser.parse_args()

    main(args)