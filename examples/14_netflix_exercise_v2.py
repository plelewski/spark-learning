from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, length
from pyspark.sql.types import IntegerType


def give_chars_num(desc):
    return len(desc) if desc is not None else 0


def main():
    spark = SparkSession.builder \
        .appName('netflix') \
        .master('local') \
        .getOrCreate()

    df = spark.read.csv(
        '../data/netflix_titles.csv',
        header=True,
        inferSchema=True,
        sep=','
    )

    num_of_chars = udf(give_chars_num, IntegerType())

    # value N.A instead of NULL values - it works for all fields
    df_without_null = df.na.fill('N.A.')
    result = (
        df_without_null
        .withColumn("number_of_chars", num_of_chars(col("description")))
    )

    result.select("title", "number_of_chars").show()


if __name__ == "__main__":
    main()
