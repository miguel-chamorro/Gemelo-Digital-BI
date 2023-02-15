import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS



#borrar el contenido del bucket en influx cada vez que se ejecute la prueba
bucket = "traffic"
#cambiar por organizacion correspondiente
org = "U-TAD"
#cambiar por token correspondiente
token = "X7HnGVsrQv3vMPs8mw04-0U-0Wic4xjYtSMfJqjkB9raSsExeDQ4iTepV__Po1i79qtuRFeuKvCXusXxfRHJsw=="
#cambiar por URL del servidor
url="http://localhost:8086"

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)
write_api = client.write_api(write_options=SYNCHRONOUS)
import csv
i=0
with open('accicentes_prueba.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
    for row in spamreader:

        print(row)
        if i == 0:
            column_names=row
        if i > 0:
            for j in range(0,len(column_names)):
                print(column_names[j]+": "+row[j])
                p = influxdb_client.Point("crashes").tag("crashes", "crash"+str(i)).field(column_names[j],row[j])
                write_api.write(bucket=bucket, org=org, record=p)

        i=i+1

query = f'from(bucket: "traffic") |> range(start: -1h) |> filter(fn: (r) => r["_measurement"] == "crashes") |> filter(fn: (r) => r["crashes"] == "crash1")'
result = client.query_api().query(query, org=org)
results = []
for table in result:
    for record in table.records:
        results.append((record.get_field(), record.get_value()))
print(results)
