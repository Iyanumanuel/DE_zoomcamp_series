
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os
from decouple import config

user = config('user', default='')
password = config('password', default='')
host = config('host', default='')
port = config('port', default='')
db = config('db', default='')
table_name = config('table_name', default='')
url = config('url', default='')
csv_name = 'output.csv'


# os.system(f"wget {url}")

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

df_iter = pd.read_csv(url, iterator=True, chunksize= 100000)

df = next(df_iter)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
df.to_sql(name=table_name, con=engine, if_exists='append')

while True:
  t_start = time()
  df = next(df_iter)
  df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
  df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
  df.to_sql(con=engine, name=table_name, if_exists='append')
  t_end = time()

print('inserted another chunk..., took %.3f second'%(t_end - t_start))




# df = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv', nrows=100)

# print(pd.io.sql.get_schema(df, name='yellow_taxi_data',  con=engine))

# df_iter = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv', iterator=True, chunksize= 100000)

# df = next(df_iter)

# len(df)

# df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

# while True:
#   t_start = time()
#   df = next(df_iter)
#   df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#   df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
#   df.to_sql(con=engine, name='yellow_taxi_data', if_exists='append')
#   t_end = time()

#   print('inserted another chunk..., took %.3f second'%(t_end -t_start))
