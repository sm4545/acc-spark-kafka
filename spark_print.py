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

max_val = spark.sql("SELECT key as TIME, value as HIGHEST_PRICE FROM stockprice_table WHERE value IN (select max(value) FROM stockprice_table)")
max_val.show()

min_val = spark.sql("SELECT key as TIME, value as LOWEST_PRICE FROM stockprice_table WHERE value IN (select min(value) FROM stockprice_table)")
min_val.show()

std_val = spark.sql("SELECT stddev(value) as STANDARD_DEVIATION FROM stockprice_table")
std_val.show()


                     
