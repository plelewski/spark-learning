from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Joins").getOrCreate()

    employees = spark.createDataFrame(
        [(1, "Jan"), (2, "Anna"), (3, "Piotr")],
        ["emp_id", "name"]
    )

    departments = spark.createDataFrame(
        [(1, "IT"), (2, "HR"), (4, "Finance")],
        ["emp_id", "department"]
    )

    # różne rodzaje joinów
    inner_df = employees.join(departments, on='emp_id', how='inner')
    inner_df.show()

    left_df = employees.join(departments, on='emp_id', how='left')
    left_df.show()

    right_df = employees.join(departments, on='emp_id', how='right')
    right_df.show()

    full_df = employees.join(departments, on='emp_id', how='full')
    full_df.show()

    # cross_df = employees.join(departments, how='cross')
    cross_df = employees.crossJoin(departments)
    cross_df.show()

    # semi join jest odpowiednikiem
    # SELECT * FROM tabela_a AS a WHERE EXISTS (... tabela_b AS b ... WHERE a.emp_id = b.emp_id)
    semi_df = employees.join(departments, on='emp_id', how='left_semi')
    semi_df.show()

    # SELECT * FROM tabela_a AS a WHERE NOT EXISTS (... tabela_b AS b ... WHERE a.emp_id = b.emp_id)
    anti_df = employees.join(departments, on='emp_id', how='left_anti')
    anti_df.show()


if __name__ == '__main__':
    main()
