from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg
from pyspark.sql.window import Window

# Создаем SparkSession
spark = SparkSession.builder \
    .appName("test population") \
    .getOrCreate()

# Читаем данные из CSV файла
df = spark.read.csv("population.csv", header=True, inferSchema=True)

# Создаем окно для использования функций lag
window_spec = Window.partitionBy("Country Name").orderBy("Year")

# Вычисляем прирост населения от года к году
df = df.withColumn("Population Change", col("Value") - lag("Value", 1).over(window_spec))

# Вычисляем средний прирост за период с 1990 по 2018 год 
average_growth = df.filter((col("Year") >= 1990) & (col("Year") <= 2018)) \
                   .groupBy("Country Name") \
                   .agg(avg("Population Change").alias("trend"))

# Выводим средний прирост
# average_growth.show()

# Находим страны с трендом на убыль населения с 1990 по 2018 годы
declining_trend = df.filter((col("Year") >= 1990) & (col("Year") <= 2018)) \
                    .groupBy("Country Name") \
                    .agg(avg("Population Change").alias("trend")) \
                    .filter(col("trend") < 0)

# Выводим страны с убылью населения
declining_trend.show()