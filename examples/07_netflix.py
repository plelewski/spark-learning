from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim


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

    # value N.A instead of NULL values - it works for all fields
    df_without_null = df.na.fill('N.A.')
    df_without_null.show(truncate=False)

    # split string for table and explode that changes each item to single row
    # cannot use trim(explode()), because trim needs string, but explode gives special column type
    df_splitted1 = df_without_null.select('show_id', explode(split('listed_in', ',')).alias("kind"))\
        .select('show_id', trim('kind'))
    df_splitted1.show(truncate=False)

    # better to use
    df_splitted2 = df_without_null.select('show_id', explode(split('listed_in', ',')).alias("kind"))\
        .withColumn('kind', trim('kind'))
    df_splitted2.show(truncate=False)


if __name__ == "__main__":
    main()
