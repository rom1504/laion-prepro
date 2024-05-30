# read iwf
# read c3p
# read keyword
# merge and dedup
# save

# do this for 2B-en; 2B-multi; 1B-nolang

from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()


iwf_2b_en = spark.read.parquet("/media/hd2/iwf2/laion2B-en-md5-iwf").select("url")
iwf_2b_multi = spark.read.parquet("/media/hd2/iwf2/laion2B-multi-md5-iwf").select("url")
iwf_1b_nolang = spark.read.parquet("/media/hd2/iwf2/laion1B-nolang-md5-iwf").select("url")

iwf_url_2b_en = spark.read.parquet("/media/hd2/iwf_urls/laion2B-en-md5-iwf_urls").select("url")
iwf_url_2b_multi = spark.read.parquet("/media/hd2/iwf_urls/laion2B-multi-md5-iwf_urls").select("url")
iwf_url_1b_nolang = spark.read.parquet("/media/hd2/iwf_urls/laion1B-nolang-md5-iwf_urls").select("url")

david_thiel_2b_en = spark.read.parquet("/media/hd2/david/laion2B-en-md5-david").select("url")
david_thiel_2b_multi = spark.read.parquet("/media/hd2/david/laion2B-multi-md5-david").select("url")
david_thiel_1b_nolang = spark.read.parquet("/media/hd2/david/laion1B-nolang-md5-david").select("url")

c3p_2b_en = spark.read.parquet("/media/hd2/c3p/laion2B-en-md5-c3p").select("url")
c3p_2b_multi = spark.read.parquet("/media/hd2/c3p/laion2B-multi-md5-c3p").select("url")
c3p_1b_nolang = spark.read.parquet("/media/hd2/c3p/laion1B-nolang-md5-c3p").select("url")

all_unsafe_2b_en = iwf_2b_en.union(c3p_2b_en).union(iwf_url_2b_en).union(david_thiel_2b_en)
all_unsafe_2b_multi = iwf_2b_multi.union(c3p_2b_multi).union(iwf_url_2b_multi).union(david_thiel_2b_multi)
all_unsafe_1b_nolang = iwf_1b_nolang.union(c3p_1b_nolang).union(iwf_url_1b_nolang).union(david_thiel_1b_nolang)

all_unsafe_2b_en = all_unsafe_2b_en.drop_duplicates()
all_unsafe_2b_multi = all_unsafe_2b_multi.drop_duplicates()
all_unsafe_1b_nolang = all_unsafe_1b_nolang.drop_duplicates()

print("Count 2B-en", all_unsafe_2b_en.count())
print("Count 2B-multi", all_unsafe_2b_multi.count())
print("Count 1B-nolang", all_unsafe_1b_nolang.count())