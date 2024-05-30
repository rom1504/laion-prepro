

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

human_rights_watch = spark.read.option("header", True).csv("/media/hd2/human_rights_watch/filter_human_rights_watch.csv").select("url")

en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet").select("url")
multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet").select("url")
nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet").select("url")


hrw = broadcast(human_rights_watch)

en_filtered = en.join(hrw, ["url"])
multi_filtered = multi.join(hrw, ["url"])
nolang_filtered = nolang.join(hrw, ["url"])

print("en_filtered", en_filtered.count())
print("multi_filtered", multi_filtered.count())
print("nolang_filtered", nolang_filtered.count())
