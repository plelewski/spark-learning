import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round, desc, max as spark_max
from loaders.stock_loader import load_stocks

def run():
    spark = SparkSession.builder \
        .appName('stock') \
        .master('local') \
        .getOrCreate()

    files = glob.glob('/Users/przemek/Downloads/spark_stock/D/DA*.csv')
    df_stocks = load_stocks(spark, files)
    print(df_stocks.count())

    df_filtered = (
        df_stocks.filter(
            (col('timestamp') >= 1746057600) &
            (col('timestamp') <= 1746921599)
        )
        .withColumn('timestamp', to_timestamp(col('timestamp')))
        .withColumn('day_result', round(col('open') - col('close'),10))
        .filter(col('day_result') > 0)
    )

    df_top10 = (
        df_filtered
        .groupBy(col('company_name'))
        .agg(spark_max('day_result').alias('max_day_result'))
        .orderBy(col('max_day_result').desc())
        .limit(5)
    )

    df_top10.show()


if __name__ == '__main__':
    run()
