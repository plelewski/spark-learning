from pyspark.sql.functions import explode, col, desc


def calculate_source_data(df):
    return df.\
        groupBy('tweet_source').\
        count()


def calculate_hashtags(df):
    return df.\
        select(explode(col('hashtags')).alias('hashtags')).\
        groupBy('hashtags').\
        count().\
        orderBy(desc('count'))

