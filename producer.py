from kafka import KafkaProducer
import time
import pandas as pd
import numpy as np
import json
producer = KafkaProducer(bootstrap_servers='localhost:9093')
df = pd.read_csv('datos_finales.csv',delimiter=';',on_bad_lines='skip')

print(df.columns)
print(df.shape)


for i in range(df.shape[0]):
	print("=========MENSAJE=========")
	data={
           'num_expediente ': df.loc[i,'num_expediente'],
           'fecha': df.loc[i,'fecha'],
           'hora':df.loc[i,'hora'],
           'coordenada_x_utm': df.loc[i,'coordenada_x_utm'],
           'coordenada_y_utm': df.loc[i,'coordenada_y_utm']    
        }
	m=json.dumps(data)
	producer.send('test', m.encode('utf-8'))
	time.sleep(10)

