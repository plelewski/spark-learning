from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType


# session create
spark = (SparkSession.builder
         .appName("UDF")
         .master("local")
         .getOrCreate()
         )


def age_category(age):
    if age <= 25:
        return "Young"
    elif age <= 50:
        return "Medium age"
    else:
        return "Old :)"


age_cat_udf = udf(age_category, StringType())

data = [
    ("Marek", "Markowski", 25),
    ("Anna", "Annowska", 30),
    ("Tomek", "Tomkowicz", 12)
]

people_df = spark.createDataFrame(
    data,
    ["firstname", "lastname", "age"]
)


result = people_df.withColumn(
    "category",
    age_cat_udf(col("age"))
)

result.show()


# but much better to write in this way
result2 = people_df.withColumn(
    "category",
    when(col("age") <= 25, "Young")
    .when(col("age") <= 50, "Medium age")
    .otherwise("Old")
)

result2.show()
