from pyspark.sql import SparkSession
import minsait.constants.constants as c


def main():
    spark = SparkSession.builder.appName("RDD").master(c.MODE).getOrCreate()
    sc = spark.sparkContext

    pokemon_rdd = sc.textFile(c.INPUT_PATH)

    pokemon_rdd.foreach()

    pokemon_rdd \
        .amp(lambda line: line.split(",")) \
        .filter(lambda array: re.match("^[0-9]+"))

if __name__ == "__main__":
    main()