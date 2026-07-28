from pyspark.sql.functions import lit, regexp_replace, split, col


def remove_columns(df, columns):
    return df.drop(*columns)


def clean_field(df, col_name):
    return df.\
        withColumn(col_name, regexp_replace(col_name, "[\\[\\]']", "")).\
        withColumn(col_name, split(col_name, ','))

def remove_nulls(df):
    return df.na.fill('N.A.')


def add_tweet_source(df, source):
    return df.withColumn('tweet_source', lit(source))


def clean_tweets(df, source):
    df = remove_nulls(df)
    df = add_tweet_source(df, source)

    return df
