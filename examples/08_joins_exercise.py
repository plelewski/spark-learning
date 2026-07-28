from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col


def main():
    spark = SparkSession.builder.appName("Joins").getOrCreate()

    employees = spark.createDataFrame(
        [(1, "Jan", "Kowalski", 30), (2, "Anna", "Wisniewska", 40), (3, "Piotr", "Kiper", 20)],
        ["emp_id", "fname", "sname", "age"]
    )

    professions = spark.createDataFrame(
        [(20, "programmer"), (40, "HR director"), (40, "Finance director")],
        ["min_age", "proffesion"]
    )

    employees2 = employees.withColumn("lenn", length("fname") + length("sname"))

    # result = employees2.join(
    #     professions,
    #     (employees2["lenn"] <= professions["min_age"]) &
    #     (employees2["age"] >= professions["min_age"]),
    #     "inner"
    # )

    e = employees2.alias("e")
    p = professions.alias("p")

    result = e.join(
        p,
        (col("e.lenn") <= col("p.min_age")) &
        (col("e.age") >= col("p.min_age"))
    )

    result.show()


if __name__ == '__main__':
    main()
