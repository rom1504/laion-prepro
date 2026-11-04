

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

human_rights_watch = spark.read.option("header", True).csv("/media/hd2/human_rights_watch/merged_human_rights_watch.csv").select("url")

en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet")
multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet")
nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet")

hrw = broadcast(human_rights_watch)

en_filtered = en.join(hrw, ["url"])
multi_filtered = multi.join(hrw, ["url"])
nolang_filtered = nolang.join(hrw, ["url"])

en_filtered.write.mode("overwrite").parquet("/media/hd2/hrw/laion2B-en-md5-hrw")
multi_filtered.write.mode("overwrite").parquet("/media/hd2/hrw/laion2B-multi-md5-hrw")
nolang_filtered.write.mode("overwrite").parquet("/media/hd2/hrw/laion1B-nolang-md5-hrw")

en_filtered = spark.read.parquet("/media/hd2/hrw/laion2B-en-md5-hrw")
multi_filtered = spark.read.parquet("/media/hd2/hrw/laion2B-multi-md5-hrw")
nolang_filtered = spark.read.parquet("/media/hd2/hrw/laion1B-nolang-md5-hrw")


print("en_filtered", en_filtered.count())
print("multi_filtered", multi_filtered.count())
print("nolang_filtered", nolang_filtered.count())
