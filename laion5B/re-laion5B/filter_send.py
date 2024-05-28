# 1. read datasets
# 2. anti join with broadcast
# 3. write datasets

from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G").config("spark.sql.shuffle.partitions", "1000") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

# True is research safe, False is safe
research_safe = False

en = spark.read.parquet("/media/hd2/backup/laion2B-en-md5/*.parquet")
multi = spark.read.parquet("/media/hd2/backup/laion2B-multi-md5/*.parquet")
nolang = spark.read.parquet("/media/hd2/backup/laion1B-nolang-md5/*.parquet")

if research_safe:
    all_unsafe_en = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-en-md5-unsafe")
    all_unsafe_multi = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-multi-md5-unsafe")
    all_unsafe_nolang = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion1B-nolang-md5-unsafe")
else:
    all_unsafe_en = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-en-md5-safe_unsafe_merged")
    all_unsafe_multi = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-multi-md5-safe_unsafe_merged")
    all_unsafe_nolang = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion1B-nolang-md5-safe_unsafe_merged")

en_filtered = en.join(all_unsafe_en, ["url"], "leftanti").orderBy(rand()).repartition(128)
multi_filtered = multi.join(all_unsafe_multi, ["url"], "leftanti").orderBy(rand()).repartition(128)
nolang_filtered = nolang.join(all_unsafe_nolang, ["url"], "leftanti").orderBy(rand()).repartition(128)

if research_safe:
    output_path_en = "/media/hd2/relaion_research_safe/relaion2B-en-research-safe"
    output_path_multi = "/media/hd2/relaion_research_safe/relaion2B-multi-research-safe"
    output_path_nolang = "/media/hd2/relaion_research_safe/relaion1B-nolang-research-safe"
else:
    output_path_en = "/media/hd2/relaion_safe/relaion2B-en-safe"
    output_path_multi = "/media/hd2/relaion_safe/relaion2B-multi-safe"
    output_path_nolang = "/media/hd2/relaion_safe/relaion1B-nolang-safe"

en_filtered.write.mode("overwrite").parquet(output_path_en)
multi_filtered.write.mode("overwrite").parquet(output_path_multi)
nolang_filtered.write.mode("overwrite").parquet(output_path_nolang)

from huggingface_hub import HfApi
api = HfApi()
api.upload_folder(
    folder_path=output_path_en,
    repo_id="laion/relaion2B-en-research-safe",
    repo_type="dataset",
)
api.upload_folder(
    folder_path=output_path_multi
    repo_id="laion/relaion2B-multi-research-safe",
    repo_type="dataset",
)
api.upload_folder(
    folder_path=output_path_nolang,
    repo_id="laion/relaion1B-nolang-research-safe",
    repo_type="dataset",
)