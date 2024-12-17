from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType
from pyspark.sql.functions import col

# Инициализация SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Определение схемы
schema = StructType([
    StructField("ts", LongType(), True),
    StructField("id", StringType(), True),
    StructField("op", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True),  # Это поле будет использоваться только при op = 'c'
    StructField("set", MapType(StringType(), StringType()), True)  # Это поле будет использоваться только при op = 'u'
])

df = spark.read.json("/accounts/*.json", schema=schema)

df = df.orderBy("ts")

final_df = spark.createDataFrame([], schema=StructType([
    StructField("ts", LongType(), True),
    StructField("id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("address", StringType(), True),
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("savings_account_id", StringType(), True),
    StructField("card_id", StringType(), True)
]))

# Обработка событий
for row in df.collect():
    #print(f"Processing row: {row}")  # Отладочное сообщение

    if row.op == 'c':
        # Создание новой записи
        new_row = {
            "ts": row.ts,
            "id": row.id,
            "account_id": row.data.get('account_id'),
            "address": row.data.get('address'),
            "email": row.data.get('email'),
            "name": row.data.get('name'),
            "phone_number": row.data.get('phone_number'),
            "savings_account_id": row.data.get('savings_account_id'),
            "card_id": row.data.get('card_id')
        }
        #print(f"Creating new row: {new_row}")  # Отладочное сообщение

        new_row_df = spark.createDataFrame([new_row], schema=final_df.schema)
        final_df = final_df.union(new_row_df)

    if row.op == 'u':
        # Получаем текущие значения для обновляемой записи
        current_row = final_df.filter(col("id") == row.id).orderBy(col("ts").desc()).first()
        #print(f"Current row for update: {current_row}")  # Отладочное сообщение
        
        if current_row:
            # Создаем новую запись с обновленными значениями
            updated_row = {
                "ts": row.ts,
                "id": row.id,
                "account_id": current_row.account_id,
                "address": current_row.address,
                "email": current_row.email,
                "name": current_row.name,
                "phone_number": current_row.phone_number,
                "savings_account_id": current_row.savings_account_id,
                "card_id": current_row.card_id
            }
            #print(f"Initial updated row: {updated_row}")  # Отладочное сообщение
            
            # Обновляем только те поля, которые указаны в row.set
            if row.set:
                for key, value in row.set.items():
                    if key in updated_row:
                        updated_row[key] = value
                        #print(f"Updated {key} to {value} in updated row")  # Отладочное сообщение
            
            # Добавляем новую запись в финальный DataFrame
            updated_row_df = spark.createDataFrame([updated_row], schema=final_df.schema)
            final_df = final_df.union(updated_row_df)

final_df = final_df.select(
    col("ts"),
    col("account_id"),
    col("address"),
    col("email"),
    col("name"),
    col("phone_number"),
    col("card_id"),
    col("savings_account_id")
)

# Вывод результата в табличном формате, сортируя сначала по account_id, затем по ts
final_df.orderBy(col("account_id"), col("ts")).show(truncate=False)

# Завершение работы
spark.stop()