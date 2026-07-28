from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, lit


def is_adult(df):
    return df.withColumn("isAdult",
                         when(col("age") >= 18, "Y")
                         .otherwise("N"))

def add_full_name(df):
    return df.withColumn("full name",
                         concat(col("fname"), lit(" "), col("sname"))
                         )


def main():
    spark = (SparkSession.builder
             .appName("UDF")
             .master("local")
             .getOrCreate()
             )

    employees = spark.createDataFrame(
        [
            (1, "Jan", "Kowalski", 30),
            (2, "Anna", "Wisniewska", 40),
            (3, "Piotr", "Kiper", 12)
        ],
        ["emp_id", "fname", "sname", "age"]
    )

    # transofrm method allow joining more than one functions
    df_employees = (
        employees
        .transform(is_adult)
        .transform(add_full_name)
    )

    df_employees.show()


if __name__ == '__main__':
    main()
