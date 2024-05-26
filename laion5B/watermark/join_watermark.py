from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import mmh3
from pyspark.sql.functions import udf, col
from pyspark.sql.types import LongType
import hashlib

def compute_hash(url, text):
  if url is None:
    url = ''

  if text is None:
    text = ''
  
  total = (url + text).encode("utf-8")
  return mmh3.hash64(total)[0]
  #return hashlib.md5(total).digest()


def main():
  spark = SparkSession.builder.config("spark.local.dir", "/media/nvme/spark-tmp").config("spark.driver.maxResultSize", "26GB").config("spark.driver.memory", "28G") .master("local[16]").appName('spark-stats').getOrCreate()
  df = spark.read.parquet("/media/hd2/allmeta/2Ben_really_safe")
  #udf_compute_hash = udf(compute_hash, LongType())
  #df = df.withColumn("hash", udf_compute_hash(df["url"], df["caption"]))
  df = df.select("hash", "url")
  dfpred = spark.read.parquet("/media/nvme/watermark_tags")
  dfpred = dfpred.drop_duplicates(['hash'])
  # with broadcast join hint
  join = df.join(dfpred.hint('broadcast'), df.hash == dfpred.hash)
  join = join.drop(df.hash)
  join = join.select('url', 'pwatermark')
  join.repartition(64).write.mode("overwrite").parquet("/media/nvme/small_water")

if __name__ == "__main__":
  main()
