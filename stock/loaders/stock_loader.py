from pyspark.sql.functions import regexp_extract, input_file_name


def load_stocks(spark, files):
    dfs = [spark.read
           .csv(file, header=True, inferSchema=True)
           .withColumn('company_name',
                       regexp_extract(input_file_name(), r"([^/]+)\.csv$",1)
                       )
           for file in files
           ]
    df = dfs[0]
    for next_df in dfs[1:]:
        df = df.unionByName(next_df, allowMissingColumns=True)

    return df
