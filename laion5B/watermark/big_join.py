from pyspark.sql import SparkSession
import mmh3
from pyspark.sql.functions import udf, rand
from pyspark.sql.types import LongType
import fire

def compute_hash(url, text):
  if url is None:
    url = ''

  if text is None:
    text = ''
  
  total = (url + text).encode("utf-8")
  return mmh3.hash64(total)[0]
  #return hashlib.md5(total).digest()


# post is optional
def main(initial_path, post_path, safety_path, watermark_path, output_path, multi=False):
  spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", "/media/nvme/spark-tmp").config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate()
  
  safety = spark.read.parquet(safety_path + "/*.parquet")
  safety = safety.drop_duplicates(['hash'])
  safety = safety.withColumnRenamed("prediction", "punsafe")

  watermark = spark.read.parquet(watermark_path + "/*.parquet")
  watermark = watermark.drop_duplicates(['hash'])


  watermark_safety = watermark.join(safety, watermark.hash == safety.hash, "outer")
  
  watermark_safety = watermark_safety.drop(safety.hash)

  """
  # use that only if your spark temporary folder is not big enough
  watermark_safety.repartition(64).write.mode("overwrite").parquet("/media/nvme/watermark_safety_multi")
  watermark_safety = spark.read.parquet("/media/nvme/watermark_safety_multi")
  """
  
  
  initial = spark.read.parquet(initial_path)
  init_cols = ["URL", "TEXT", "WIDTH", "HEIGHT", "similarity"]
  if multi:
    init_cols += [initial.LANGUAGE]
  initial = initial.select(*init_cols)
  initial = initial.withColumnRenamed("WIDTH", "h")
  initial = initial.withColumnRenamed("HEIGHT", "w")
  initial = initial.withColumnRenamed("h", "HEIGHT")
  initial = initial.withColumnRenamed("w", "WIDTH")

  # to join as well with exif
  #post = spark.read.parquet(post_path)
  #post.select("url", "caption", "exif")
  #joined = initial.join(post, (initial["URL"] == post["url"]) & (initial["TEXT"] == post["caption"]), "left_outer")
  joined = initial
  cols = [initial.URL, initial.TEXT, initial.WIDTH, initial.HEIGHT, initial.similarity] # , post.exif

  if multi:
    cols += [initial.LANGUAGE]

  joined = joined.select(*cols)

  udf_compute_hash = udf(compute_hash, LongType())
  joined = joined.withColumn("hash", udf_compute_hash(joined["URL"], joined["TEXT"]))

  """
  # use that only if your spark temporary folder is not big enough
  joined.repartition(128).write.mode("overwrite").parquet("/media/nvme/multi_tmp")

  joined = spark.read.parquet("/media/nvme/multi_tmp")
  """


  joined = joined.join(watermark_safety, joined.hash == watermark_safety.hash, "left_outer")
  joined = joined.drop(watermark_safety.hash)
  
  joined = joined.orderBy(rand())
  joined.repartition(128).write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
  fire.Fire(main)
