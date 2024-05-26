from pyspark.sql import SparkSession

from pyspark.sql.functions import rand

spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.sql.files.ignoreCorruptFiles", "true").config("spark.local.dir", "/media/nvme/spark-tmp").config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate()


blip = spark.read.parquet("/media/hd2/laion-synthetic-shuffled")

laion2B = spark.read.parquet("/media/hd2/laion2B-en-joined/*.parquet")

joined = blip.join(laion2B, (blip["url"] == laion2B["URL"]) & (blip["original_caption"] == laion2B["TEXT"]))

joined = joined.select(laion2B["URL"], "TEXT", "top_caption", "all_captions", "all_similarities", "WIDTH", "HEIGHT", "similarity", "hash", "pwatermark", "punsafe")

shuffled = joined.orderBy(rand())

shuffled.repartition(128).write.mode("overwrite").parquet("/media/hd2/laion-synthetic-shuffled-joined")