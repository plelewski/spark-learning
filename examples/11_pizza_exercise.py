from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, regexp_replace, round


def main():
    spark = SparkSession.builder \
        .appName('logika') \
        .master('local') \
        .getOrCreate()

    df = spark.read.csv(
        '../data/pizza_data.csv',
        header=True,
        inferSchema=True,
        sep=','
    )

    # get Medium pizzas and get min/max/avg
    (
        df
        .filter(df
                .Size
                .like('%Medium%')
                )
        .withColumn(
            'Price',
            regexp_replace(col('Price'), r'\$', '').cast('double')
        )
        .agg(
            min('Price').alias('min_price'),
            max('Price').alias('max_price'),
            round(avg('Price'), 2).alias('max_price')
        )
        .show()
    )

    # different way to show the same
    df \
        .filter(df
                .Size
                .like('%Medium%')
                ) \
        .withColumn(
            'Price',
            regexp_replace(col('Price'), r'\$', '').cast('double')
        ) \
        .agg(
            min('Price').alias('min_price'),
            max('Price').alias('max_price'),
            round(avg('Price'), 2).alias('max_price')
        ) \
        .show()


if __name__ == "__main__":
    main()
