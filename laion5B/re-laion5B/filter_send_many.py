from pyspark.sql import SparkSession
from  pyspark.sql.functions import rand
from huggingface_hub import HfApi
from huggingface_hub.commands.large_upload import large_upload
import uuid
import shutil


root_tmp_path="/media/nvme/root_tmp_path"

def create_spark_session():
    spark = SparkSession.builder.config("spark.driver.memory", "16G").config("spark.executor.memory", "1G").config("spark.storage.decommission.enabled", "true").config("spark.storage.cleanupFilesAfterExecutorExit", "true").config("spark.sql.shuffle.partitions", "1000") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()
    return spark


def write_temp_read_recreate_spark(spark, df, root_tmp_path):
    tmp_path=root_tmp_path+"/"+uuid.uuid4().hex
    df.write.mode("overwrite").parquet(tmp_path)
    output_df = spark.read.parquet(tmp_path)
    return [spark, output_df, tmp_path]


def upload(repo_name, folder_path):
    # Create an API object
    api = HfApi()

    print(f"Uploading large files in '{folder_path}' to repository '{repo_name}'.")
    # Perform the large upload
    large_upload(
        folder_path=folder_path,
        repo_id=repo_name,
        repo_type="dataset",
        api=api,
        revision='relaion',
    )

def join_url_URL(df, to_remove):
    if "url" in df.columns:
        return df.join(to_remove, ["url"], "leftanti")
    elif "URL" in df.columns:
        renamed_to_remove = to_remove.withColumnRenamed("url", "URL")
        return df.join(renamed_to_remove, ["URL"], "leftanti")
    else:
        raise ValueError("No URL or URL column in dataframe")

def filter_send(input_folder,
                output_folder,
                repo_id,
                type_of_dataset,
                compute,
                do_upload):
    
    if compute:
        spark = create_spark_session()
        try:
            df = spark.read.parquet(input_folder + "/*.parquet")
        except Exception as e:
            try:
                print(f"Error reading without wildcard '*': {e}")
                df = spark.read.parquet(input_folder + "/**/*.parquet")
            except Exception as e:
                print(f"Error reading with double wildcard '**': {e}")
                df = spark.read.parquet(input_folder + "/**/**/*.parquet")

        if type_of_dataset == "en":
            all_unsafe = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-en-md5-safe_unsafe_merged")
        elif type_of_dataset == "multi":
            all_unsafe = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-multi-md5-safe_unsafe_merged")
        elif type_of_dataset == "nolang":
            all_unsafe = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion1B-nolang-md5-safe_unsafe_merged")

        [spark, pre, tmp_path] = write_temp_read_recreate_spark(spark, join_url_URL(df, all_unsafe), root_tmp_path)
        en_filtered = pre.orderBy(rand()).repartition(128)
        en_filtered.write.mode("overwrite").parquet(output_folder)
        shutil.rmtree(tmp_path)


    if do_upload:
        upload(
            folder_path=output_folder,
            repo_name=repo_id,
        )


to_process = [
    {
        "repo":"laion/laion400m",
        "type": "en",
        "input_folder": "/media/hd2/backup/laion400m",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion400m",
        "compute": False,
    },
    {
        "repo":"laion/laion400m_old", # field URL
        "type": "en",
        "input_folder": "/media/hd2/backup/laion400m_old",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion400m_old",
    },
    {
        "repo": "laion/laion2B-multi-aesthetic",# field URL
        "type": "multi",
        "input_folder": "/media/hd2/backup/laion2B-multi-aesthetic",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion2B-multi-aesthetic",
    },
    {
        "repo": "laion/laion2B-en-aesthetic",
        "type": "en",
        "input_folder": "/media/hd2/backup/laion2B-en-aesthetic",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion2B-en-aesthetic",
    },
    {
        "repo": "laion/laion1B-nolang-aesthetic",
        "type": "nolang",
        "input_folder": "/media/hd2/backup/laion1B-nolang-aesthetic",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion1B-nolang-aesthetic",
    },
    {
        "repo": "laion/laion1B-nolang-joined-translated-to-en", # field URL
        "type": "nolang",
        "input_folder": "/media/hd2/backup/laion1B-nolang-joined-translated-to-en",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion1B-nolang-joined-translated-to-en",
    },
    {
        "repo": "laion/laion2B-multi-joined-translated-to-en",
        "type": "multi",
        "input_folder": "/media/hd2/backup/laion2B-multi-joined-translated-to-en",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion2B-multi-joined-translated-to-en",
    },
    {
        "repo": "laion/laion-art", # field URL
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-art",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-art",
    },
    {
        "repo": "laion/laion-coco", # field URL
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-coco",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-coco",
    },
    {
        "repo": "laion/laion-pop",
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-pop",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-pop",
    },
    {
        "repo": "laion/laion-high-resolution", # URL
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-high-resolution",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-high-resolution",
    },
    {
        "repo": "laion/laion-synthetic-115m",
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-synthetic-115m",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-synthetic-115m",
    },
]


# breaks because of shitty files
to_process = [
    #{
    #    "repo": "laion/aesthetics_v2_4.5",
    #    "type": "en",
    #    "input_folder": "/media/hd2/backup_30802/aesthetics_v2_4.5_fixed",
    #    "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/aesthetics_v2_4.5",
    #},
    {
        "repo": "laion/aesthetics_v2_4.75",
        "type": "en",
        "input_folder": "/media/hd2/backup_30802/aesthetics_v2_4.75_fixed",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/aesthetics_v2_4.75",
    },
]

#weirder stuff
    #"laion/laion-30k-fid", # no parquet
    #"laion/laion5B-aesthetic-tags-kv", # no parquet
    #"laion/laion5B-watermark-safety-ordered", # no parquet
    #"laion/aesthetics_v2_4.75", # URL and subfolder
    #"laion/aesthetics_v2_4.5"

# hash
    #"laion/laion2B-multi-aesthetic-tags", # no url, only hash
    #"laion/laion2B-en-aesthetic-tags",
    #"laion/laion1B-nolang-aesthetic-tags",
    #"laion/laion1b-nolang-inappropriate", # no url, only hash
    #"laion/laion2b-en-inappropriate",
    #"laion/laion2b-multi-inappropriate",
    #"laion/laion1B-nolang-watermark", # no url, only hash
    #"laion/laion2B-multi-watermark",
    #"laion/laion2B-en-watermark",
    #"laion/laion2B-multi-safety", # hash no url
    #"laion/laion2B-en-safety",
    #"laion/laion1B-nolang-safety",

# same as main
    #"laion/laion2B-multi-md5", # same as main
    #"laion/laion2B-en-md5",
    # "laion/laion1B-nolang-md5",
    #"laion/laion2B-multi-joined", # same as main, no point
    #"laion/laion1B-nolang-joined",
    #"laion/laion2B-en-joined",


for process in to_process:
    filter_send(input_folder=process["input_folder"],
                output_folder=process["output_folder"],
                repo_id=process["repo"],
                type_of_dataset=process["type"],
                compute=process["compute"] if "compute" in process else True,
                do_upload=process["do_upload"] if "do_upload" in process else True)