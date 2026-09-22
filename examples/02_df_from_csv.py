from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum
import os


os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# session create
spark = SparkSession.builder \
    .appName('fundament-sparka') \
    .master('local') \
    .getOrCreate()

# header in the first line
# inferSchema Spark recognize what kind of data
# sep has to be added, because ";" as separator (not ",")
df = spark.read.csv('../data/people.csv', header=True, inferSchema=True, sep=';')
df.show()
df.printSchema()

adults = df.filter((df.age >= 18) & (df.age < 60))
adults2 = df.filter(df.age.between(18, 60))


# liczba oznacza ilość wyświetlanych wierszy a truncate pokazuje pełne wartości każdego pola
adults.show(30, truncate=True)
adults2.show()

people_cnt = df.filter(col('last_name').contains(lit('Ko'))).count()
print('ilość po odfiltrowaniu: ' + str(people_cnt))

df.select(sum('age')).show()
