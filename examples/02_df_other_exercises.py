from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count


def main():
    # 1.Spark session
    spark = SparkSession.builder \
        .appName('fundament-sparka') \
        .master('local') \
        .getOrCreate()

    # 2.CSV reading
    df = spark.read.csv(
        '../data/people.csv',
        header=True,
        inferSchema=True,
        sep=';'
    )

    # 3.View data
    df.show()

    # 4.view schema
    df.printSchema()

    # 5.Simple filter
    #   plus additional column with predefined value
    #   plus another column using when()...otherwise()
    # adults = df.filter(df.age >= 18).withColumn('country', lit('Poland'))
    # adults = adults.withColumn('status', when(col('age') >= 30, 'Adult Premium').otherwise('Only Adult'))

    # different way to do the same
    adults = (
        df.filter(col('age') >= 18)
          .withColumn('country', lit('Poland'))
          .withColumn('status',  when(col('age') >= 35, 'Adult premium').otherwise('Only Adult'))
    )

    # select column transformation
    adults.select('first_name','age').show()

    # 6.Write parquet format file
    adults.write.mode('overwrite').parquet('/Users/przemek/PycharmProjects/spark-learning/data/adults')

    # 7.Simple SQL
    adults.createOrReplaceTempView('people')

    spark.sql('''
        SELECT *
        FROM   people
    ''').show()

    # 8.Sparka stopping
    spark.stop()


# START the App
if __name__ == "__main__":
    main()
