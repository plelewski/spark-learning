
def load_tweets(spark, path):
    return spark.read.csv(f'../data/{path}', header=True, inferSchema=True, sep=',')
