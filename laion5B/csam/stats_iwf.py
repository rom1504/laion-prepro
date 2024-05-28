import pandas as pd 
from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

en_filtered = spark.read.parquet("/media/hd2/iwf2/laion2B-en-md5-iwf")
multi_filtered = spark.read.parquet("/media/hd2/iwf2/laion2B-multi-md5-iwf")
nolang_filtered = spark.read.parquet("/media/hd2/iwf2/laion1B-nolang-md5-iwf")

def compute_stats(name, df):
    print(name)
    print("Count", df.count())
    print("punsafe stats")
    df[["punsafe"]].summary().show()
    print("severity stats")
    df[["severity"]].groupBy("severity").count().show()

compute_stats("laion2B-en iwf", en_filtered)
compute_stats("laion2B-multi iwf", multi_filtered)
compute_stats("laion2B-nolang iwf", nolang_filtered)
