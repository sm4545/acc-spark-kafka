from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
# sc = SparkContext('local')
spark = SparkSession(sc)

df = spark \
.read \
.format("kafka") \
.option("kafka.bootstrap.servers", "172.25.0.12:9092,172.25.0.13:9092") \
.option("startingOffsets", "earliest") \
.option("subscribe", "GME_TOPIC_2") \
.load()

df1 = df.selectExpr("CAST(key AS STRING)", "CAST(CAST(value AS STRING) AS FLOAT)", "timestamp")
df1.show()

df1.registerTempTable("stockprice_table")

open_val = spark.sql("SELECT value as OPENING_STOCK_PRICE, key as TIME FROM stockprice_table WHERE value IN (select value FROM stockprice_table WHERE key = '09:31')")
open_val.show()

close_val = spark.sql("SELECT value as CLOSING_STOCK_PRICE, key as TIME FROM stockprice_table WHERE value = (select value FROM stockprice_table WHERE key = '15:59')")
close_val.show()

# std_val = spark.sql("SELECT stddev(value) as STANDARD_DEVIATION FROM stockprice_table")
# std_val.show()
                   
