# Import SparkSession
import pyspark
from pyspark.sql import SparkSession
from influxdb_client import InfluxDBClient
import influxdb_client
import ast
from pyproj import Transformer

#cambiar por el bucket correspondiente
bucket = "prueba_2"
#cambiar por organizacion correspondiente
org = "bi"
#cambiar por token correspondiente
token = "pMMHyiYPWiulRABtrDb2zLePfWJgHErev0h0fKscnOnnRRKMj4S4tUd7uujdztALsOGK91vtZSOhlaP_Qz7T4A=="
#cambiar por URL del servidor
url="http://proyectos_data-influxdb-1:8086"

crs_from="EPSG:4326"
crs_to="EPSG:25830"

transformer = Transformer.from_crs(crs_to,crs_from)

client = InfluxDBClient(url=url, token=token)
write_api = client.write_api()

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()
      
df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka1:19092") \
      .option("subscribe", "test") \
      .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")    

def f(df):
    print("============================================")
    dict_str = df.value.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    
    new_string = "."

    before_x = mydata['coordenada_x_utm']
    before_y = mydata['coordenada_y_utm']
    
    
    #before_x = -52.441155
    #before_y = -54.351
   
    transformer = Transformer.from_crs(crs_to,crs_from)
    after_x, after_y = transformer.transform(before_x,before_y)
    
    mydata['coordenada_x_utm'] = str(after_x)
    mydata['coordenada_y_utm'] = str(after_y)
    
    
    print(repr(mydata))
    dictionary = [{"measurement": "h2o_feet", "tags": {"location": "coyote_creek"}, "fields": mydata, "time": 1}]

    write_api.write(bucket=bucket, org=org, record=dictionary)
query = df.writeStream.foreach(f).start()

query.awaitTermination()
     


     

