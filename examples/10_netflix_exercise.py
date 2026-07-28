from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split, trim


def main():
    spark = SparkSession.builder \
        .appName('netflix') \
        .master('local') \
        .getOrCreate()

    # Seven steps to complete
    # 1.Import the file
    df = spark.read.csv(
        '../data/netflix_titles.csv',
        header=True,
        inferSchema=True,
        sep=','
    )

    # 2.Check the schema and number of rows
    df.printSchema()
    num_of_rows_in_df = df.count()
    print("Number of rows in netflix file: ", num_of_rows_in_df)

    # 3.Change NULL value to NILL value
    df_stage1 = df.na.fill('NILL')

    # 4.Check number of movies/type
    df_stage1.filter(col('type') != 'NILL').groupBy('type').count().show()

    # different way to check the same
    result = (
        df_stage1
        .filter(col('type') != 'NILL')
        .groupBy('type')
        .count()
    )

    result.show()

    # 5.Check number of movies/director
    result = (
        df_stage1
        .filter(col('director') != 'NILL')
        .groupBy('director')
        .count()
    )

    result.orderBy('count', ascending=False).show(truncate=False)

    # 6.Check number of movies/release year ascending order
    # I did similar above

    # 7.Check number of movies/film genre
    df_film_genre = df_stage1.select(
        'show_id',
        explode(split('listed_in', ',')).alias('genre')
    ).select(
        'show_id',
        trim('genre').alias('film_genre')
    )

    df_film_genre.groupBy('film_genre').count().orderBy('count', ascending=False).show()

    # more readable way, more "pythonic" to do the same
    df_film_genre2 = (
        df_stage1
        .select(
            'show_id',
            explode(split(col('listed_in'), ',')).alias('film_genre')
        )
        .withColumn('film_genre', trim('film_genre'))
    )

    (
        df_film_genre2
        .groupBy('film_genre')
        .count()
        .orderBy(col('count').desc())
        .show()
    )


if __name__ == "__main__":
    main()
