from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list


def main():
    spark = SparkSession.builder.appName("Joins").getOrCreate()

    employees = spark.createDataFrame(
        [(1, "Jan", 30), (2, "Anna", 40), (3, "Piotr", 20), (4, "Waldemar", 30)],
        ["emp_id", "name", "age"]
    )

    professions = spark.createDataFrame(
        [(20, "programmer"), (40, "HR director"), (40, "Finance director")],
        ["min_age", "proffesion"]
    )

    # lista pasujących zawodów jest typu array
    result = employees.join(professions, employees["age"] >= professions["min_age"], "left") \
        .groupBy("emp_id", "name", "age") \
        .agg(collect_list("proffesion"))

    result.show(truncate=False)
    result.printSchema()


if __name__ == '__main__':
    main()
