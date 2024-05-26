from pyspark.sql import SparkSession

from pyspark.sql.functions import rand

spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.sql.files.ignoreCorruptFiles", "true").config("spark.local.dir", "/media/nvme/spark-tmp").config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate()


df = spark.read.parquet("/media/hd2/laion-synthetic-out")

uniques = df.drop_duplicates(["url", "original_caption"])

#uniques_shuffled = uniques.orderBy(rand())

uniques.repartition(128).write.mode("overwrite").parquet("/media/hd2/laion-synthetic-shuffled")