import pandas as pd 
from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet")
multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet")
nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet")


iwf = broadcast(spark.read.parquet("/media/hd2/iwf2/uniques.parquet"))

en_filtered = en.join(iwf, ["md5"])
multi_filtered = multi.join(iwf, ["md5"])
nolang_filtered = nolang.join(iwf, ["md5"])

en_filtered.write.parquet("/media/hd2/iwf2/laion2B-en-md5-iwf")
multi_filtered.write.parquet("/media/hd2/iwf2/laion2B-multi-md5-iwf")
nolang_filtered.write.parquet("/media/hd2/iwf2/laion1B-nolang-md5-iwf")
