
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://bi_user:password@66.94.120.221/taxi')
from time import time

df = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv', nrows=100)

print(pd.io.sql.get_schema(df, name='yellow_taxi_data',  con=engine))

df_iter = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv', iterator=True, chunksize= 100000)

df = next(df_iter)

len(df)

df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

# Commented out IPython magic to ensure Python compatibility.
# %time df.to_sql(con=engine, name='yellow_taxi_data', if_exists='append')

while True:
  t_start = time()
  df = next(df_iter)
  df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
  df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
  df.to_sql(con=engine, name='yellow_taxi_data', if_exists='append')
  t_end = time()

  print('inserted another chunk..., took %.3f second'%(t_end -t_start))
