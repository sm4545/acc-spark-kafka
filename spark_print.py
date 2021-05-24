from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

df = spark \
.read \
.format("kafka") \
.option("kafka.bootstrap.servers", "172.25.0.12:9092,172.25.0.13:9092") \
.option("startingOffsets", "earliest") \
.option("subscribe", "AAPL") \
.load()

df1 = df.selectExpr("CAST(key AS STRING)", "CAST(CAST(value AS STRING) AS FLOAT)", "timestamp")

df1.show()

df1.createOrReplaceTempView("stockprice_table")

# max_val = spark.sql("SELECT * FROM stockprice_table")
# max_val.show()

max_val = spark.sql("SELECT max(value), key FROM stockprice_table GROUP BY key")
max_val.show()

min_val = spark.sql("SELECT min(value), key FROM stockprice_table GROUP BY key")
min_val.show()



                     
