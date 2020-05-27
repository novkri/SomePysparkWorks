import pyspark as spark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("1lab") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv('brooklyn_sales_map.csv', header=True, inferSchema=True)
    # df.printSchema()

    # # 1 ------------------------------------------------------------------------------------------------------------
    # # Найдите среднюю стоимость жилья (sale_price) и выведите новую таблицу, содержащую две колонки – стоимость
    # # жилья и процент отклонения стоимости от среднего значения.
    #
    # mean_sale_price = df.agg(avg("sale_price")).first()[0]
    # print(mean_sale_price)
    # sale_price_mean = df.select("sale_price").summary("mean").show()
    #
    # def findPercentage(temp):
    #      return ((temp/mean_sale_price)*100)
    #      #((temp-mean_sale_price)/mean_sale_price)*100
    # df0 = df.select("sale_price")
    # tempDf = df0.withColumn('percentage', findPercentage(df0.sale_price)).show()

    # # 2 ------------------------------------------------------------------------------------------------------------
    # # Выведите таблицу, содержащую все категории класса зданий (building_class_category) и количество записей,
    # # которые к ним относятся.
    #
    # df.groupBy("building_class_category").count().show()
    #
    # # 3 ------------------------------------------------------------------------------------------------------------
    # # Выведите таблицу, содержащую средние значения по каждому столбцу в датафрейме
    #
    # df0 = df.summary('mean').show()
    #
    # # 4------------------------------------------------------------------------------------------------------------
    # # В исходном датафрейме заполните все нулевые значения средними по столбцу.

    # print(df.dtypes)
    #
    # def get_dtype(df, colname):
    #     return [dtype for name, dtype in df.dtypes if name == colname][0]
    #
    # columns_ = []
    # print(get_dtype(df, '_c0'))
    # for x in df.columns:
    #     if (get_dtype(df, x) == 'int'):
    #         columns_.append(x)
    # print(columns_)
    #
    # for x in columns_:
    #     meanValue = df.agg(avg(x)).first()[0]
    #     print("column: ", x)
    #     print("mean value: ", meanValue)
    #     df.replace(0, meanValue, [x]).show()
    # df.show()