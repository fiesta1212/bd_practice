from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

df = spark.read.csv("world-cities.csv", header=True, inferSchema=True)

df = df.select("country", "geonameid", "name", "subcountry")

df.printSchema()

grouped_result = df.groupBy("country", "subcountry") \
    .agg(
        count("geonameid").alias("city_count")
    )

final_result = grouped_result.groupBy("country") \
    .agg(
        count("subcountry").alias("subcountry_count"),
        sum("city_count").alias("city_count")
    ) \
    .orderBy(col("city_count").desc())

final_result.show()