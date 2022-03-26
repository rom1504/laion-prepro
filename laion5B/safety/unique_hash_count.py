from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import mmh3
from pyspark.sql.functions import udf


def main():
  spark = SparkSession.builder.config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate()
  df = spark.read.parquet("/media/hd2/allmeta/2Ben_hashes")
  df = df.select('hash')
  print(df.count())
  print(df.drop_duplicates().count())

if __name__ == "__main__":
  main()