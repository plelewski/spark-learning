from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum


# session create
spark = SparkSession.builder \
    .appName('fundament-sparka') \
    .master('local') \
    .getOrCreate()

# header in the first line
# inferSchema Spark recognize kind of data
# sep has to be added, because ";" as separator (not ",")
df = spark.read.csv('../data/people.csv', header=True, inferSchema=True, sep=';')
df.show()
df.printSchema()

adults = df.filter(df.age >= 18)

# liczba oznacza ilość wyświetlanych wierszy a truncate pokazuje pełne wartości każdego pola
adults.show(30, truncate=True)

people_cnt = df.filter(col('last_name').contains(lit('Ko'))).count()
print('ilość po odfiltrowaniu: ' + str(people_cnt))

df.select(sum('age')).show()
