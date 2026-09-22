from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
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
peopleDF.select("firstname", "age").show()
peopleDF.select(col("firstname"),col("age") + 1).alias("age+1").show()
