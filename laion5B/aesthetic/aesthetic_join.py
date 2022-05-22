from pyspark.sql import SparkSession
import mmh3
from pyspark.sql.functions import rand
import fire


def main(initial_path, aesthetic_path, output_path):
  spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", "/media/hd2/spark-tmp").config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate()
  
  aesthetic = spark.read.parquet(aesthetic_path + "/*.parquet")
  aesthetic = aesthetic.filter(aesthetic["aesthetic"] > 7)
  aesthetic = aesthetic.drop_duplicates(['hash'])
  
  
  initial = spark.read.parquet(initial_path + "/*.parquet")
  initial = initial.filter(initial["punsafe"] < 0.5)
  initial = initial.filter(initial["pwatermark"] < 0.8)


  joined = initial.join(aesthetic.hint("broadcast"), initial.hash == aesthetic.hash).drop(aesthetic.hash)
  
  joined = joined.orderBy(rand())
  joined.repartition(128).write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
  fire.Fire(main)
