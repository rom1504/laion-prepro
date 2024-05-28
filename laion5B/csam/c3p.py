import pandas as pd 
from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet")
multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet")
nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet")


c3p = broadcast(spark.read.option("header", True).option("delimiter", ",").csv("/media/hd2/c3p/arachnid-md5-hashes.csv"))

en_filtered = en.join(c3p, ["md5"])
multi_filtered = multi.join(c3p, ["md5"])
nolang_filtered = nolang.join(c3p, ["md5"])

en_filtered.write.parquet("/media/hd2/c3p/laion2B-en-md5-c3p")
multi_filtered.write.mode("overwrite").parquet("/media/hd2/c3p/laion2B-multi-md5-c3p")
nolang_filtered.write.parquet("/media/hd2/c3p/laion1B-nolang-md5-c3p")
