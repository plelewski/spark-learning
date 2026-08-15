
def prepare_target_df(spark, df):
    df = df.withColumnRenamed('Close', 'close')