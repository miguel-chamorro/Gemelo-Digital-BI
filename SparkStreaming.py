from pyspark import SparkContext
from pyspark.streaming import StreamingContext


import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import bucket_api



#cambiar por el bucket correspondiente
bucket = "traffic"
#cambiar por organizacion correspondiente
org = "UTAD"
#cambiar por token correspondiente
token = "XwT_iGgPpjf8Q_NJIntZ6Du4ur7mQT8EshF3QapJkSivzUMwFSHw2ceG2tW6I90XIVUZL0N_QrLbtijcJ94tWw=="
#cambiar por URL del servidor
url="http://localhost:8086"

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)

cubo=bucket_api.BucketsApi(client)
cubo.create_bucket(bucket_name="traffic",org=org)

write_api = client.write_api(write_options=SYNCHRONOUS)


# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
ssc.start()             # Start the computation
ssc.awaitTermination() 