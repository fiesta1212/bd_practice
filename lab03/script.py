from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum #<put your code here>

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

df = spark.read.csv("world-cities.csv", header=True, inferSchema=True)

df = df.select("country", "geonameid", "name", "subcountry")

df.printSchema()

# Группируем данные по странам и подстранным и выполняем агрегацию
grouped_result = df.groupBy("country", "subcountry") \
    .agg(
        count("geonameid").alias("city_count")  # Подсчитываем общее количество городов для каждой подстраны
    )

# Теперь группируем по стране и считаем уникальные подстраны и общее количество городов
final_result = grouped_result.groupBy("country") \
    .agg(
        count("subcountry").alias("subcountry_count"),  # Уникальные подстраны
        sum("city_count").alias("city_count")                   # Общее количество городов
    ) \
    .orderBy(col("city_count").desc())                        # Сортируем по количеству городов

final_result.show()