

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

all_unsafe_2b_en = iwf_2b_en.union(c3p_2b_en).union(iwf_url_2b_en)
all_unsafe_2b_multi = iwf_2b_multi.union(c3p_2b_multi).union(iwf_url_2b_multi)
all_unsafe_1b_nolang = iwf_1b_nolang.union(c3p_1b_nolang).union(iwf_url_1b_nolang)

david_thiel_2b_en_filter = david_thiel_2b_en.join(all_unsafe_2b_en, ["url"], "left_anti")
david_thiel_2b_multi_filter = david_thiel_2b_multi.join(all_unsafe_2b_multi, ["url"], "left_anti")
david_thiel_1b_nolang_filter = david_thiel_1b_nolang.join(all_unsafe_1b_nolang, ["url"], "left_anti")

print("Count 2B-en", david_thiel_2b_en_filter.count())
print("Count 2B-multi", david_thiel_2b_multi_filter.count())
print("Count 1B-nolang", david_thiel_1b_nolang_filter.count())


iwf_url_2b_en_filter = iwf_url_2b_en.join(iwf_2b_en, ["url"], "left_anti")

print("Count iwf_url_2b_en_filter", iwf_url_2b_en_filter.count())

human_rights_watch = spark.read.option("header", True).csv("/media/hd2/human_rights_watch/filter_human_rights_watch.csv").select("url")

print(human_rights_watch.count())

all_unsafe = all_unsafe_2b_en.union(all_unsafe_2b_multi).union(all_unsafe_1b_nolang).union(david_thiel_2b_en_filter).union(david_thiel_2b_multi_filter).union(david_thiel_1b_nolang_filter)

human_rights_watch_filtered = human_rights_watch.join(all_unsafe, ["url"], "left_anti")

print("Count human_rights_watch_filtered", human_rights_watch_filtered.count())