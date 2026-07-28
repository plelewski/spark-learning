from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, concat_ws


def main():
    spark = SparkSession.builder.appName("Joins").getOrCreate()

    employees = spark.createDataFrame(
        [(1, "Jan", "Kowalski", 30), (2, "Anna", "Wisniewska", 40), (3, "Piotr", "Kiper", 20)],
        ["emp_id", "fname", "sname", "age"]
    )

    # employees2 = employees.withColumn("fullname", concat(col("fname"), lit(" "), col("sname"))).drop("fname", "sname")
    employees2 = employees.withColumn("fullname", concat_ws(" ", "fname", "sname")).drop("fname", "sname")
    employees2.filter(col("age") >= 30).show()


if __name__ == '__main__':
    main()
