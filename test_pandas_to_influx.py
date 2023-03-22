#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import dask.dataframe as dd
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime


# In[2]:


#cambiar por el bucket correspondiente
bucket = "trafico"
#cambiar por organizacion correspondiente
org = "Gemelo_BI"
#cambiar por token correspondiente
token = "vjcN-j4WSLz0cHaEAKh3KRxp_MO_fofkru0h2TiQTPKSxAlboTiNNcYZg-GG8_2whhdhKWZtHoQWKMCRv6GqIQ=="
#cambiar por URL del servidor
url="http://localhost:8086"
client = InfluxDBClient(url="http://localhost:8086", token=token)
write_api = client.write_api()


# In[3]:


df =pd.read_csv('accicentes_prueba.csv',header = 0,delimiter=';')

#print(df.head(1))



df['_time'] = df['fecha']+"T"+df['hora']+"Z"
print(df['_time'].head(1))

df['_time']=pd.to_datetime(df['_time'])
print(df['_time'].head(1))


df = df.replace(' ', '_', regex=True)
print(df['_time'].head(1))


# In[4]:



df=df.drop(["fecha","hora"],axis=1)
df.set_index(df["_time"])
columnas = list(df.drop(["_time"],axis=1).columns)
print(columnas)


# write_api = client.write_api()
# write_api.write(bucket=bucket, org=org, record=df,data_frame_measurement_name="_measurement",data_frame_tag_columns=columnas)

# In[5]:



write_api.write(bucket=bucket, record=df,data_frame_measurement_name="Accidente",data_frame_tag_columns=columnas,org=org)


# In[6]:


query = f'from(bucket: "trafico") |> range(start: 1900-01-01, stop: 2023-01-02) |> filter(fn: (r) => r._measurement == "Accidente")'
result = client.query_api().query(query, org=org)
results = []


# In[10]:


for table in result:
    for record in table.records:
        results.append((record.get_field(), record.get_value()))
        print(record.get_field()+": "+str(record.get_value()))
    print(table.records)
print(results)


# query = f'from (bucket: "trafico") |> range(start: -10000000000000000m, stop: now())'
# client = InfluxDBClient(url, token, org)
# traffic = client.query_api().query_data_frame()
# display(traffic.head())
# 
