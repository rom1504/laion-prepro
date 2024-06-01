# read iwf
# read c3p
# read keyword
# merge and dedup
# save

# do this for 2B-en; 2B-multi; 1B-nolang

from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

def run(research_safe):
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

    if research_safe:
        keyword_research_2b_en = spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion2B-en-md5-keyword_punsafe_research_filtered").select("url")
        keyword_research_2b_multi = spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion2B-multi-md5-keyword_punsafe_research_filtered").select("url")
        keyword_research_1b_nolang = spark.read.parquet("/media/hd2/keyword_punsafe_filtered/laion1B-nolang-md5-keyword_punsafe_research_filtered").select("url")
    else:
        keyword_research_2b_en = spark.read.parquet("/media/hd2/relaion_safe_unsafe/laion2B-en-md5-safe_unsafe").select("url")
        keyword_research_2b_multi = spark.read.parquet("/media/hd2/relaion_safe_unsafe/laion2B-multi-md5-safe_unsafe").select("url")
        keyword_research_1b_nolang = spark.read.parquet("/media/hd2/relaion_safe_unsafe/laion1B-nolang-md5-safe_unsafe").select("url")


    human_rights_watch = spark.read.option("header", True).csv("/media/hd2/human_rights_watch/merged_human_rights_watch.csv").select("url")

    all_unsafe_2b_en = iwf_2b_en.union(c3p_2b_en).union(keyword_research_2b_en).union(human_rights_watch).union(iwf_url_2b_en).union(david_thiel_2b_en)
    all_unsafe_2b_multi = iwf_2b_multi.union(c3p_2b_multi).union(keyword_research_2b_multi).union(human_rights_watch).union(iwf_url_2b_multi).union(david_thiel_2b_multi)
    all_unsafe_1b_nolang = iwf_1b_nolang.union(c3p_1b_nolang).union(keyword_research_1b_nolang).union(human_rights_watch).union(iwf_url_1b_nolang).union(david_thiel_1b_nolang)

    all_unsafe_2b_en = all_unsafe_2b_en.drop_duplicates()
    all_unsafe_2b_multi = all_unsafe_2b_multi.drop_duplicates()
    all_unsafe_1b_nolang = all_unsafe_1b_nolang.drop_duplicates()

    if research_safe:
        all_unsafe_2b_en.write.mode("overwrite").parquet("/media/hd2/relaion_research_unsafe/laion2B-en-md5-unsafe")
        all_unsafe_2b_multi.write.mode("overwrite").parquet("/media/hd2/relaion_research_unsafe/laion2B-multi-md5-unsafe")
        all_unsafe_1b_nolang.write.mode("overwrite").parquet("/media/hd2/relaion_research_unsafe/laion1B-nolang-md5-unsafe")
    else:
        all_unsafe_2b_en.write.mode("overwrite").parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-en-md5-safe_unsafe_merged")
        all_unsafe_2b_multi.write.mode("overwrite").parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-multi-md5-safe_unsafe_merged")
        all_unsafe_1b_nolang.write.mode("overwrite").parquet("/media/hd2/relaion_safe_unsafe_merged/laion1B-nolang-md5-safe_unsafe_merged")

run(True)
run(False)