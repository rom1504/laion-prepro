from pyspark.sql import SparkSession
import mmh3
from pyspark.sql.functions import rand, lit
import fire


def main():
    spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", "/media/hd2/spark-tmp").config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate()

    nolang = spark.read.parquet("/media/hd2/laion1B-nolang-aesthetic" + "/*.parquet")
    en = spark.read.parquet("/media/hd2/laion2B-en-aesthetic" + "/*.parquet")
    multi = spark.read.parquet("/media/hd2/laion2B-multi-aesthetic" + "/*.parquet")
    nolang = nolang.filter(nolang["aesthetic"] > 8)
    en = en.filter(en["aesthetic"] > 8)
    multi = multi.filter(multi["aesthetic"] > 8)

    en = en.withColumn("LANGUAGE", lit("en"))
    nolang = nolang.withColumn("LANGUAGE", lit("nolang"))

    en = en.select("URL", "TEXT", "WIDTH", "HEIGHT", "similarity", "LANGUAGE", "hash", "pwatermark", "punsafe", "aesthetic")
    nolang = nolang.select("URL", "TEXT", "WIDTH", "HEIGHT", "similarity", "LANGUAGE", "hash", "pwatermark", "punsafe", "aesthetic")
    multi = multi.select("URL", "TEXT", "WIDTH", "HEIGHT", "similarity", "LANGUAGE", "hash", "pwatermark", "punsafe", "aesthetic")

    all = en.union(nolang).union(multi)

    all = all.orderBy(rand())

    all.repartition(1).write.mode("overwrite").parquet("/media/hd2/laion-art")

if __name__ == "__main__":
  fire.Fire(main)
