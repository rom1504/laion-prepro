# 1. read datasets
# 2. anti join with broadcast
# 3. shuffle, repartition
# 4. write datasets
# 5. upload to huggingface hub

from pyspark.sql import SparkSession
from  pyspark.sql.functions import rand
from huggingface_hub import HfApi
from huggingface_hub.commands.large_upload import large_upload
import uuid
import shutil


root_tmp_path="/media/nvme/root_tmp_path"

def create_spark_session():
    spark = SparkSession.builder.config("spark.driver.memory", "16G").config("spark.sql.shuffle.partitions", "1000") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()
    return spark


def write_temp_read_recreate_spark(spark, df, root_tmp_path):
    tmp_path=root_tmp_path+"/"+uuid.uuid4().hex
    df.write.mode("overwrite").parquet(tmp_path)
    output_df = spark.read.parquet(tmp_path)
    return [spark, output_df, tmp_path]


def filter_send(input_en,
                input_multi,
                input_nolang,
                research_safe,
                output_path_en,
                output_path_multi,
                output_path_nolang,
                repo_id_en,
                repo_id_multi,
                repo_id_nolang,
                compute,
                upload):
    
    if compute:
        spark = create_spark_session()
        en = spark.read.parquet(input_en + "/*.parquet")
        multi = spark.read.parquet(input_multi + "/*.parquet")
        nolang = spark.read.parquet(input_nolang + "/*.parquet")

        if research_safe:
            all_unsafe_en = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-en-md5-unsafe")
            all_unsafe_multi = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-multi-md5-unsafe")
            all_unsafe_nolang = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion1B-nolang-md5-unsafe")
        else:
            all_unsafe_en = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-en-md5-safe_unsafe_merged")
            all_unsafe_multi = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-multi-md5-safe_unsafe_merged")
            all_unsafe_nolang = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion1B-nolang-md5-safe_unsafe_merged")

        [spark, pre_en, tmp_path_en] = write_temp_read_recreate_spark(spark, en.join(all_unsafe_en, ["url"], "leftanti"), root_tmp_path)
        en_filtered = pre_en.orderBy(rand()).repartition(128)
        en_filtered.write.mode("overwrite").parquet(output_path_en)
        shutil.rmtree(tmp_path_en)
        [spark, pre_multi, tmp_path_multi] = write_temp_read_recreate_spark(spark, multi.join(all_unsafe_multi, ["url"], "leftanti"), root_tmp_path)
        multi_filtered = pre_multi.orderBy(rand()).repartition(128)
        multi_filtered.write.mode("overwrite").parquet(output_path_multi)
        shutil.rmtree(tmp_path_multi)
        [spark, pre_nolang, tmp_path_nolang] = write_temp_read_recreate_spark(spark, nolang.join(all_unsafe_nolang, ["url"], "leftanti"), root_tmp_path)
        nolang_filtered = pre_nolang.orderBy(rand()).repartition(128)
        nolang_filtered.write.mode("overwrite").parquet(output_path_nolang)
        shutil.rmtree(tmp_path_nolang)


    if upload:
        api = HfApi()

        large_upload(
            folder_path=output_path_en,
            repo_id=repo_id_en,
            repo_type="dataset",
            api=api,
        )
        large_upload(
            folder_path=output_path_multi,
            repo_id=repo_id_multi,
            repo_type="dataset",
            api=api,
        )
        large_upload(
            folder_path=output_path_nolang,
            repo_id=repo_id_nolang,
            repo_type="dataset",
            api=api,
        )

# relaion-research-safe
filter_send(input_en="/media/hd2/backup/laion2B-en-md5",
            input_multi="/media/hd2/backup/laion2B-multi-md5",
            input_nolang="/media/hd2/backup/laion1B-nolang-md5",
            research_safe=True,
            output_path_en="/media/hd2/relaion_research_safe/relaion2B-en-research-safe",
            output_path_multi="/media/hd2/relaion_research_safe/relaion2B-multi-research-safe",
            output_path_nolang="/media/hd2/relaion_research_safe/relaion1B-nolang-research-safe",
            repo_id_en="laion/relaion2B-en-research-safe",
            repo_id_multi="laion/relaion2B-multi-research-safe",
            repo_id_nolang="laion/relaion1B-nolang-research-safe",
            compute=True,
            upload=False)

# relaion-safe
filter_send(input_en="/media/hd2/backup/laion2B-en-md5",
            input_multi="/media/hd2/backup/laion2B-multi-md5",
            input_nolang="/media/hd2/backup/laion1B-nolang-md5",
            research_safe=False,
            output_path_en="/media/hd2/relaion_safe/relaion2B-en-safe",
            output_path_multi="/media/hd2/relaion_safe/relaion2B-multi-safe",
            output_path_nolang="/media/hd2/relaion_safe/relaion1B-nolang-safe",
            repo_id_en="laion/relaion2B-en-safe",
            repo_id_multi="laion/relaion2B-multi-safe",
            repo_id_nolang="laion/relaion1B-nolang-safe",
            compute=True,
            upload=False)