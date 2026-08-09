from pyspark.sql import SparkSession


def run():
    spark = SparkSession.builder \
        .appName('spotify') \
        .master('local') \
        .getOrCreate()

    df = spark.read.csv(
        '../data/spotify_churn.csv',
        header=True,
        inferSchema=True,
        sep=',')

    df.write.mode('overwrite').parquet('/Users/przemek/PycharmProjects/spark-learning/data/spot_churn')

    df_parq = spark.read.parquet('../data/spot_churn').select('user_id', 'country')
    df_parq.printSchema()


if __name__ == '__main__':
    run()
