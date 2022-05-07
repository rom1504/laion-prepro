from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand

spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/hd2/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate() 


multi = spark.read.parquet("/media/hd2/laion2B-multi-joined/*.parquet")
nolang = spark.read.parquet("/media/hd2/laion1B-nolang-joined/*.parquet")
en = spark.read.parquet("/media/hd2/laion2B-en-joined/*.parquet")
multi_filtered = multi.filter((multi["WIDTH"] >= 1024) & (multi["HEIGHT"] >= 1024))
en_filtered = en.filter((en["WIDTH"] >= 1024) & (en["HEIGHT"] >= 1024))
nolang_filtered = nolang.filter((nolang["WIDTH"] >= 1024) & (nolang["HEIGHT"] >= 1024))

en_filtered = en_filtered.withColumn("LANGUAGE", lit("en"))
nolang_filtered = nolang_filtered.withColumn("LANGUAGE", lit("nolang"))

en_filtered = en_filtered.select("URL", "TEXT", "WIDTH", "HEIGHT", "similarity", "LANGUAGE", "hash", "pwatermark", "punsafe")
nolang_filtered = nolang_filtered.select("URL", "TEXT", "WIDTH", "HEIGHT", "similarity", "LANGUAGE", "hash", "pwatermark", "punsafe")

all = en_filtered.union(nolang_filtered).union(multi_filtered)

all = all.orderBy(rand())

all.repartition(128).write.mode("overwrite").parquet("/media/hd2/laion-high-resolution")