

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

en_filtered = spark.read.parquet("/media/hd2/hrw/laion2B-en-md5-hrw").select("url", "caption")
multi_filtered = spark.read.parquet("/media/hd2/hrw/laion2B-multi-md5-hrw").select("url", "caption")
nolang_filtered = spark.read.parquet("/media/hd2/hrw/laion1B-nolang-md5-hrw").select("url", "caption")


print("en_filtered", en_filtered.count())
print("multi_filtered", multi_filtered.count())
print("nolang_filtered", nolang_filtered.count())

union = en_filtered.union(multi_filtered).union(nolang_filtered)

union.repartition(1).write.mode("overwrite").csv("/media/hd2/hrw/laion5B-hrw.csv", header=True)