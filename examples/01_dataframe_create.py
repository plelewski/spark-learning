from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# session create
spark = SparkSession.builder \
    .appName("fundament-sparka") \
    .master("local") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# variable for Dataframe and Dataframe create
dataDF = [("Marek", "Markowski", 25), ("Anna", "Annowska", 30), ("Tomek", "Tomkowicz", 12)]
peopleDF = spark.createDataFrame(dataDF, ["firstname", "lastname", "age"])

peopleDF.show()
# do not need to use col word to show data, but to make changes on columns, use col
peopleDF.select(col("firstname"),col("age") + 1).show()
