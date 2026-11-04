from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel
import pyspark.sql.functions as sf

spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

def filter(df):
    df = df.filter((sf.col("punsafe") > 0.45) | sf.col('punsafe').isNull())
    return df

en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet")
filter(en).write.mode('overwrite').parquet("/media/hd2/relaion_safe_unsafe/laion2B-en-md5-safe_unsafe")
print("en", spark.read.parquet("/media/hd2/relaion_safe_unsafe/laion2B-en-md5-safe_unsafe").count())

multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet")
filter(multi).write.mode('overwrite').parquet("/media/hd2/relaion_safe_unsafe/laion2B-multi-md5-safe_unsafe")
print("multi", spark.read.parquet("/media/hd2/relaion_safe_unsafe/laion2B-multi-md5-safe_unsafe").count())

nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet")
filter(nolang).write.mode('overwrite').parquet("/media/hd2/relaion_safe_unsafe/laion1B-nolang-md5-safe_unsafe")
print("nolang", spark.read.parquet("/media/hd2/relaion_safe_unsafe/laion1B-nolang-md5-safe_unsafe").count())



