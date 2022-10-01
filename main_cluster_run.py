import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

#os.environ["PYTHON_HOME"] = r"C:\Map-Reduce\spark\venv\Scripts"
#os.environ["PYSPARK_PYTHON"] = r"C:\Map-Reduce\spark\venv\Scripts\python.exe"
#os.environ["PYSPARK_HOME"] = r"C:\Map-Reduce\spark\venv\Lib\site-packages\pyspark"
#os.environ["HADOOP_HOME"] =r"C:\Map-Reduce\spark\venv\hadoop"
#os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk1.8.0_341"


# MySql
MYSQL_HOST = 'rc1c-v4f7g6yol4srzrqf.mdb.yandexcloud.net'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'lab4'
MYSQL_TABLE = 'mega_datamart'
MYSQL_USER = 'spark'
MYSQL_PASSWORD = 'sparkspark'

# СОЗДАЕМ СПРАВОЧНИКИ как набора tuple
# Определяем поля справочников, header
dim_columns = ['id', 'name']
#Наполняем справочники данными
payment_rows = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
]

# СОЗДАЕМ СТРУКТУРУ Spark DataFrame ФАКТОВ
# объявим структуру StructType как наиболее детерминированную для загрузки данных из файла csv (название,тип данных,пусто/нет)
# каким образом и скаким типом данных читать csv чтобы получить типизированный DataFrame
trips_schema = StructType([
    StructField('vendor_id', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('ratecode_id', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pulocation_id', IntegerType(), True),
    StructField('dolocation_id', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType()),
])

# ФУНКЦИЯ загрузки данных в DataFrame ФАКТОВ и их РАСЧЕТЫ в итоговый DataFrame с агрегированными данными

def agg_calc(spark: SparkSession) -> DataFrame:
    # 1. загружаем данные из CSV
    #!путь на hdfs
    data_path = '/user/root/2020/yellow_tripdata_2020-01.csv'
    #data_path = os.path.join(Path(__name__).parent, './2020/', '*.csv')

    # 2. читаем CSV через спарк контекст В Spark DataFrame
    # (передаем несколько параметров, в т.ч. переопределенную схему данных)
    # читаем ВСЕ .csv file по данному пути
    trip_fact = spark.read \
        .option("header", "true") \
        .schema(trips_schema) \
        .csv(data_path)

    # КОД DAG SPARK ВЫЧИСЛЕНИЙ
    # создаем агрегат загруженных данных, витрину из созданного DataFrame
    # применяя к нему различные методы
    # !импортируем функции датафрейма из библиотеки pyspark.sql как "f"
    datamart = trip_fact \
        .where(trip_fact['vendor_id'].isNotNull()) \
        .where(f.to_date(trip_fact['tpep_pickup_datetime']).between('2020-01-01', '2020-01-31')) \
        .groupBy(trip_fact['payment_type'],
                 f.to_date(trip_fact['tpep_pickup_datetime']).alias('dt')
                 ) \
        .agg(f.avg(trip_fact['fare_amount']).alias("avg_trip_cost"),
             f.avg(trip_fact['trip_distance']).alias("avg_trip_km_cost") ) \
        .select(f.col('dt'),
                f.col('payment_type'),
                f.col('avg_trip_cost'),
                f.col('avg_trip_km_cost')
                ) \
        #.orderBy(f.col('dt').desc(), f.col('payment_type'))

    return datamart

# ФУНКЦИЯ програмного создания DataFrame из массивов СПРАВОЧНИКОВ
def create_dict(spark: SparkSession, header: list, data: list):
    """создание словаря"""
    df = spark.createDataFrame(data=data, schema=header)
    return df

# ФУНКЦИЯ сохранения в mysql
def save_to_mysql(host: str, port: int, db_name: str, username: str, password: str, df: DataFrame, table_name: str):
    props = {
        'user': f'{username}',
        'password': f'{password}',
        'driver': 'com.mysql.cj.jdbc.Driver',
        "ssl": "true",
        "sslmode": "none",
    }

    df.write.mode("append").jdbc(
        url=f'jdbc:mysql://{host}:{port}/{db_name}',
        table=table_name,
        properties=props)


# main ОСНОВНАЯ ФУНКЦИЯ (ргумент "spark" в def main - параметры SparkSession)
def main(spark: SparkSession):

    # Создаем словари по трем измерениям, передавая в параметрах их массивы
    #vendor_dim = create_dict(spark, dim_columns, vendor_rows)
    payment_dim = create_dict(spark, dim_columns, payment_rows)
    #rates_dim = create_dict(spark, dim_columns, rates_rows)

    # Первый шаг вычислений вызов функции agg_calc
    # Делаем основной расчет над данными и кешируем для переиспользования
    datamart = agg_calc(spark).cache()
    #datamart = agg_calc(spark)

    # вывод на экран результата
    #datamart.show(truncate=False, n=10)

    #ИТОГ ВИТРИНА - джойним к датафрейму фактов - датафреймы словарей, сортируем выборку
    joined_datamart = datamart \
        .join(other=payment_dim, on=payment_dim['id'] == f.col('payment_type'), how='inner') \
        .select(payment_dim['name'].alias('Payment type'),
                f.col('dt').alias('Date'),
                f.col('avg_trip_cost').alias('Average trip cost'),
                f.col('avg_trip_km_cost').alias('Average trip km cost')
                ) \
        .orderBy(f.col('Date').desc(), f.col('Payment type'))

    # вывод на экран результата
    joined_datamart.show(truncate=False, n=100)

    # СОХРАНЯЕМ результат в csv на hdfs в один файл, с заголовком и разделителем
    joined_datamart \
        .coalesce(1)\
        .write\
        .format('csv')\
        .mode('overwrite')\
        .option('header', 'true')\
        .option('delimiter', ',')\
        .save('output3')

    # save_to_mysql(
    #     host=MYSQL_HOST,
    #     port=MYSQL_PORT,
    #     db_name=MYSQL_DATABASE,
    #     username=MYSQL_USER,
    #     password=MYSQL_PASSWORD,
    #     df=joined_datamart,
    #     table_name=f'{MYSQL_DATABASE}.{MYSQL_TABLE}'
    # )

    print('end')


# ЗАПУСК main
# аргумент "spark" в def main - параметры SparkSession

if __name__ == '__main__':
    main(SparkSession
         .builder
         .appName('Spark job - datamart taxi trip')
         .getOrCreate())

# .config("spark.jars", "./practice4/jars/mysql-connector-java-8.0.25.jar")
