
import influxdb_client
from influxdb_client import InfluxDBClient
from datetime import datetime


#cambiar por el bucket correspondiente
bucket = "a"
#cambiar por organizacion correspondiente
org = "a"
#cambiar por token correspondiente
token = "t6qN0FEc4oRghrT4452CeRADy35Snx9iGoIc6jMmOq6BfKfiDIVMg_050YnfzgoNbJqVdHI2mtMk4wP2b8y4EQ=="
#cambiar por URL del servidor
url="http://localhost:8086"
client = InfluxDBClient(url=url, token=token)


query = f'from(bucket: "a") |> range(start: 1900-01-01)'
result = client.query_api().query(query, org=org)
results = []


for table in result:
    for record in table.records:
        print(record.get_field()+": "+str(record.get_value()))

