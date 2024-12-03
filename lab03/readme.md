# Lab 03
Модернизируйте заготовку заменив все участки ```<put your code here>``` на ваш код для того, что бы:
* выводить на экран схему кадра данных
* вычислять и выводить на экран в порядке убывания количества городов страны сгруппированные по стране и региону

Пример вывода на экран:
```
root
 |-- country: string (nullable = true)
 |-- geonameid: long (nullable = true)
 |-- name: string (nullable = true)
 |-- subcountry: string (nullable = true)

+--------------+----------+----+
|       country|subcountry| cnt|
+--------------+----------+----+
| United States|        51|2699|
|         India|        35|2443|
|        Brazil|        27|1200|
|        Russia|        82|1093|
|       Germany|        16|1055|
|         China|        31| 799|
|         Japan|        47| 736|
|        France|        13| 633|
|         Italy|        20| 571|
|         Spain|        19| 569|
|        Mexico|        32| 561|
|United Kingdom|         4| 513|
|   Philippines|        17| 439|
|        Turkey|        81| 383|
|     Indonesia|        35| 372|
|        Poland|        16| 327|
|      Pakistan|         6| 312|
|   Netherlands|        12| 258|
|       Ukraine|        27| 257|
|       Algeria|        47| 247|
+--------------+----------+----+
only showing top 20 rows
```