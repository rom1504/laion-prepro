import pandas as pd 
from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand, sha2
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()


en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet").withColumn("SHA256", sha2("url", 256))
multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet").withColumn("SHA256", sha2("url", 256))
nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet").withColumn("SHA256", sha2("url", 256))


iwf = broadcast(spark.read.option("header", True).csv("/media/hd2/iwf_urls/Actioned URLs"))

en_filtered = en.join(iwf, ["SHA256"])
multi_filtered = multi.join(iwf, ["SHA256"])
nolang_filtered = nolang.join(iwf, ["SHA256"])

en_filtered.write.parquet("/media/hd2/iwf_urls/laion2B-en-md5-iwf_urls")
print("en_filtered", spark.read.parquet("/media/hd2/iwf_urls/laion2B-en-md5-iwf_urls").count())
multi_filtered.write.parquet("/media/hd2/iwf_urls/laion2B-multi-md5-iwf_urls")
print("multi_filtered", spark.read.parquet("/media/hd2/iwf_urls/laion2B-multi-md5-iwf_urls").count())
nolang_filtered.write.parquet("/media/hd2/iwf_urls/laion1B-nolang-md5-iwf_urls")
print("nolang_filtered", spark.read.parquet("/media/hd2/iwf_urls/laion1B-nolang-md5-iwf_urls").count())