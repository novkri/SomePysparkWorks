import pyspark as spark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("4lab") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv('FIFA.csv', header=True, inferSchema=True, ignoreLeadingWhiteSpace=False,
                           ignoreTrailingWhiteSpace=False, dateFormat="dd/MM/yyyy", mode = "DROPMALFORMED")
    # df.show()

    # ------------------------------------------------------------------------------------------------------------
    # 1. Получить десять наиболее упоминаемых хештегов.
    separated_hashtags = df.select(explode(split(df.Hashtags, ",")).alias("Separated"), "Place")

    print("1. Получить десять наиболее упоминаемых хештегов.")
    top10Hashtags = separated_hashtags.select('Separated').groupBy('Separated').count().orderBy('count', ascending=False)
    top10Hashtags.show(10)

    # ------------------------------------------------------------------------------------------------------------
    # 2. Получить десять столиц государств, из которых наиболее часто посылались твиты.
    # Список столиц нужно хранить в отдельном файле.
    PlaceHashtags_df = separated_hashtags.select("Place").filter(separated_hashtags.Place.isNotNull()).groupBy('Place')\
        .count().orderBy('count', ascending=False)

    print("2. Получить десять столиц государств, из которых наиболее часто посылались твиты.")
    PlaceHashtags_df.show(10)

    PlaceHashtags_df.coalesce(1).write.csv("C:/Users/chris/PycharmProjects/pyspark101/PlaceHashtags_df", header=True,
                                           dateFormat="dd/MM/yyyy", mode="overwrite")

    # ------------------------------------------------------------------------------------------------------------
    # 3. Определить наиболее часто употребляющийся хештег для каждой столицы из топ-10.
    listOfPlaces = ['Lagos, Nigeria', 'London, England', 'Nigeria', 'London', 'India', 'Indonesia', 'Nairobi, Kenya', 'Malaysia', 'South Africa', 'United States']
    listOfHashtags = ['WorldCup', 'FRA', 'CRO', 'WorldCupFinal', 'worldcup', 'ENG', 'FRAARG', 'FRABEL', 'FRACRO', 'BEL']

    topdict = {"Top_10_Place": [], "Hashtag": []}
    topPlaces = {"Place": [], "Hashtag_From_Top_10": []}

    for x in listOfPlaces:
        df3 = separated_hashtags.select("Place", "Separated").groupBy(['Place', separated_hashtags.Separated]).count().orderBy('count', ascending=False).filter(
            separated_hashtags.Place == x).take(2)
        topdict["Top_10_Place"].append(df3[1][0])
        topdict["Hashtag"].append(df3[1][1])
        # topdict["Top_10_Place"].append(df3[0][0])
        # topdict["Hashtag"].append(df3[0][1])
    print("3. Определить наиболее часто употребляющийся хештег для каждой столицы из топ-10.")
    dfObj = pd.DataFrame(topdict)
    print(dfObj)

    # ------------------------------------------------------------------------------------------------------------
    # 4. Определить столицы, в которых преобладающие хештеги из десяти наиболее упоминаемых.
    for y in listOfHashtags:
        df3 = separated_hashtags.select("Place", "Separated").filter(separated_hashtags.Place.isNotNull()).groupBy(
            ['Place', separated_hashtags.Separated]).count().orderBy('count', ascending=False).filter(
            separated_hashtags.Separated == y).take(2)
        topPlaces["Place"].append(df3[0][0])
        topPlaces["Hashtag_From_Top_10"].append(df3[0][1])

    print("\n")
    print("4. Определить столицы, в которых преобладающие хештеги из десяти наиболее упоминаемых.")
    dfObj2 = pd.DataFrame(topPlaces)
    print(dfObj2)