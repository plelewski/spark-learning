from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName('unions') \
        .master('local') \
        .getOrCreate()

    df1 = spark.read.csv(
        '../data/pizza_data_half.csv',
        header=True,
        inferSchema=True,
        sep=','
    )

    df2 = spark.read.csv(
        '../data/pizza_data_half2.csv',
        header=True,
        inferSchema=True,
        sep=','
    )

    # lepiej stosować unionByName gdyby kolumny w obu dataframach były w innej kolejności
    df_all = df1.unionByName(df2)

    # bez przypisania do zmiennej
    print(df1.count())
    print(df2.count())
    print(df_all.count())


if __name__ == "__main__":
    main()
