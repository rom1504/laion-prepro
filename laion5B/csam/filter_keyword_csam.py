from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel
import pyspark.sql.functions as sf

PUNSAFE_THRESHOLD = .95

import os
import json

banned_words_all_languages = json.load(open(os.getcwd() + "/banned_words.json", "r"))

banned_regex = '(?i)' + '|'.join([f'(?:$|^|\W){w}(?:$|^|\W)' for w in banned_words_all_languages])

spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

def filter_efficient(df):
    df_orig = df
    df = df.filter(sf.col("punsafe") > 0.95)
    df = df.withColumn("banned", sf.regexp_like("caption", sf.lit(banned_regex)))
    return df.filter((sf.col("banned"))).drop("banned").union(df_orig.filter(sf.col("punsafe").isNull()))

en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet")
filter_efficient(en).write.mode('overwrite').parquet("/media/hd2/keyword_punsafe_filtered/laion2B-en-md5-keyword_punsafe_research_filtered")
print("en", spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion2B-en-md5-keyword_punsafe_research_filtered").count())

multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet")
filter_efficient(multi).write.mode('overwrite').parquet("/media/hd2/keyword_punsafe_filtered/laion2B-multi-md5-keyword_punsafe_research_filtered")
print("multi", spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion2B-multi-md5-keyword_punsafe_research_filtered").count())

nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet")
filter_efficient(nolang).write.mode('overwrite').parquet("/media/hd2/keyword_punsafe_filtered/laion1B-nolang-md5-keyword_punsafe_research_filtered")
print("nolang", spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion1B-nolang-md5-keyword_punsafe_research_filtered").count())


