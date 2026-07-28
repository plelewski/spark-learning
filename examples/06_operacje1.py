from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, count, desc, countDistinct


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

    df.printSchema()

    df1 = df.withColumn(
        'price',    # use the same name means exchange values only
        regexp_replace(col('price'), r'\$', "").cast(dataType='double')
    )

    df1.printSchema()

    # average pizza price in all companies
    df1.groupBy('Company')\
        .avg('price').show()

    # count would count the same values in the csv if exists (in this file exists)
    df1.groupBy('Company')\
        .agg(countDistinct('Pizza Name').alias('cnt'))\
        .orderBy(desc('cnt'))\
        .show()


if __name__ == "__main__":
    main()
