from pyspark.sql import SparkSession
# Создаем сессию Spark на локальном компьютере
spark = SparkSession.builder.master("local[*]").getOrCreate()

import collections
rdd = spark.sparkContext.textFile("/opt/spark/work-dir/u.data")
# Преобразуем данные в пары (film_id, mark)
film_marks_rdd = rdd.map(lambda line: line.split("\t")).map(lambda x: (x[1], int(x[2])))

# Агрегируем оценки по фильмам
aggPairRDD = film_marks_rdd.groupByKey()

# Функция для вывода статистики для каждого фильма
def printStat(ind, marks):
    print(f'Marks for film {ind}: 1 -> {marks[1]}, 2 -> {marks[2]}, 3 -> {marks[3]}, 4 -> {marks[4]}, 5 -> {marks[5]}')

# Вывод статистики для каждого фильма
for film_id, marks in aggPairRDD.mapValues(lambda x: dict(collections.Counter(x))).collect():
    # Создаем массив для хранения количества оценок от 1 до 5
    marks_count = [0] * 6  # Индексы 1-5 будут использоваться, 0 не используется
    for mark in marks:
        marks_count[mark] = marks.get(mark, 0)
    printStat(film_id, marks_count)

# Вычисляем статистику для всех фильмов
all_marks = film_marks_rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

# Создаем массив для хранения общего количества оценок от 1 до 5
total_marks_count = [0] * 6  # Индексы 1-5 будут использоваться

# Суммируем оценки для всех фильмов
for mark, count in all_marks.collect():
    total_marks_count[mark] += count

# Выводим статистику для всех фильмов
print("Marks for films ALL: ", end="")
print(f'1 -> {total_marks_count[1]}, 2 -> {total_marks_count[2]}, 3 -> {total_marks_count[3]}, 4 -> {total_marks_count[4]}, 5 -> {total_marks_count[5]}')