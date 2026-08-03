from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, when


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

    # who spend more time Female or Male in age periods
    result = df.filter(col('gender').isin('Male', 'Female')) \
        .groupBy(col('gender'),
                 when(col('age') <= 20, '-20')
                 .when(col('age') <= 30, '21-30')
                 .when(col('age') <= 40, '31-40')
                 .otherwise('41+').alias('age_period')
                 ) \
        .agg(avg(col('listening_time')).alias('avg_listening_time')) \
        .orderBy(col('age_period'),col('gender'))

    result.show()

    # who skip songs split by subscription type
    result = df.groupBy(col('subscription_type')) \
        .agg(avg(col('skip_rate')).alias('avg_skip_rate')) \
        .orderBy(col('avg_skip_rate'))

    result.show()


if __name__ == '__main__':
    run()