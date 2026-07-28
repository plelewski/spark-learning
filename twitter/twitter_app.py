from os import environ

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lit
from loaders.tweet_loader import load_tweets
from cleaners.tweet_cleaner import clean_tweets, remove_columns, clean_field
from analysers.tweet_analyser import calculate_hashtags, calculate_source_data
from analysers.tweet_search import search_by_key_word, search_by_key_words


def main():
    environ['SPARK_LOCAL_IP'] = '127.0.0.1'

    spark = SparkSession.builder \
        .appName('tweets_analyser') \
        .master('local') \
        .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # load and clean Financial tweets
    df_fin = load_tweets(spark, 'fin_tweets.csv')
    df_fin = remove_columns(df_fin, ['id', 'symbols', 'company_names', 'url', 'verified'])
    df_fin = clean_tweets(df_fin, 'financial')
    df_fin = df_fin.\
        filter(col('timestamp') != 'N.A.').\
        withColumn('tweet_datetime', to_timestamp(col('timestamp'), 'EEE MMM dd HH:mm:ss Z yyyy')).\
        drop(col('timestamp')).\
        withColumn('hashtags', lit('no hashtags'))

    # load and clean Grammy tweets
    df_grs = load_tweets(spark, 'grs_tweets.csv')
    df_grs = clean_tweets(df_grs, 'grammy')
    df_grs = df_grs.filter(~col('date').rlike("[a-zA-Z]")).\
        withColumn('tweet_datetime', col('date').cast('timestamp')).\
        select('text', 'source', 'tweet_source', 'tweet_datetime', 'hashtags')

    # load and clean Covid-19 tweets
    df_c19 = load_tweets(spark, 'c19_tweets.csv')
    df_c19 = clean_tweets(df_c19, 'covid')
    df_c19 = df_c19.filter(~col('date').rlike("[a-zA-Z]")). \
        withColumn('tweet_datetime', col('date').cast('timestamp')). \
        select('text', 'source', 'tweet_source', 'tweet_datetime', 'hashtags')

    # merge all dataframes
    df_all = df_fin.unionByName(df_grs).unionByName(df_c19)
    df_all = clean_field(df_all, 'hashtags')
    df_all.show()

    # analyser section
    df_t_s = calculate_source_data(df_all)
    df_t_s.show()

    df_c_h = calculate_hashtags(df_all)
    df_c_h.show()

    df_search_word = search_by_key_word(df_all, 'price')
    df_search_word.show(truncate=False)

    df_search_words = search_by_key_words(df_all, "price, offer")
    df_search_words.show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
