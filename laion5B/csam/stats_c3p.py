import pandas as pd 
from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

en_filtered = spark.read.parquet("/media/hd2/c3p/laion2B-en-md5-c3p")
multi_filtered = spark.read.parquet("/media/hd2/c3p/laion2B-multi-md5-c3p")
nolang_filtered = spark.read.parquet("/media/hd2/c3p/laion1B-nolang-md5-c3p")

def compute_stats(name, df):
    print(name)
    print("Count", df.count())
    print("punsafe stats")
    df[["punsafe"]].summary().show()
    print("type classification stats")
    df[["type"]].groupBy("type").count().show()

compute_stats("laion2B-en c3p", en_filtered)
compute_stats("laion2B-multi c3p", multi_filtered)
compute_stats("laion2B-nolang c3p", nolang_filtered)
