from pyspark.sql import SparkSession

def main():
  spark = SparkSession.builder.config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").config("spark.driver.memory", "16G").appName('rep').getOrCreate()
  df = spark.read.parquet("/media/nvme/safety_parquet/laion1B-nolang")
  df.repartition(16).write.mode("overwrite").parquet("/media/nvme/safety_parquet_repartitionned/laion1B-nolang")

if __name__ == "__main__":
  main()
