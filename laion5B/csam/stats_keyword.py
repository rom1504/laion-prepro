import pandas as pd 
from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

en_filtered = spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion2B-en-md5-keyword_punsafe_research_filtered")
multi_filtered = spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion2B-multi-md5-keyword_punsafe_research_filtered")
nolang_filtered = spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion1B-nolang-md5-keyword_punsafe_research_filtered")

def compute_stats(name, df):
    print(name)
    print("Count", df.count())

compute_stats("laion2B-en keyword", en_filtered)
compute_stats("laion2B-multi keyword", multi_filtered)
compute_stats("laion2B-nolang keyword", nolang_filtered)
