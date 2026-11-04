import pandas as pd 
from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand, sha1
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet").withColumn("sha1", sha1("url"))
multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet").withColumn("sha1", sha1("url"))
nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet").withColumn("sha1", sha1("url"))


david = broadcast(spark.read.option("header", True).option("delimiter", ",").csv("/media/hd2/david_thiel/sha1s-no-newline.txt"))

en_filtered = en.join(david, ["sha1"])
multi_filtered = multi.join(david, ["sha1"])
nolang_filtered = nolang.join(david, ["sha1"])

en_filtered.write.mode("overwrite").parquet("/media/hd2/david/laion2B-en-md5-david")
multi_filtered.write.mode("overwrite").parquet("/media/hd2/david/laion2B-multi-md5-david")
nolang_filtered.write.mode("overwrite").parquet("/media/hd2/david/laion1B-nolang-md5-david")

print("en_filtered", spark.read.parquet("/media/hd2/david/laion2B-en-md5-david").count())
print("multi_filtered", spark.read.parquet("/media/hd2/david/laion2B-multi-md5-david").count())
print("nolang_filtered", spark.read.parquet("/media/hd2/david/laion1B-nolang-md5-david").count())