from static_ondisk_kv import sort_parquet, parquet_to_file, OnDiskKV
from tqdm import tqdm
import random
from glob import glob

from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel
step = 2


if step == 0:
    spark = (
        SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", "1000")
        .config("spark.driver.memory", "16G")
        .config("spark.local.dir", "/media/hd2/spark-tmp")
        .master("local[16]")
        .appName("spark-stats")
        .getOrCreate()
    )
    sort_parquet(
        input_collection=list(glob("/media/hd2/laion2B-multi-aesthetic-tags/*.parquet")+glob("/media/hd2/laion2B-en-aesthetic-tags/*.parquet")+glob("/media/hd2/laion1B-nolang-aesthetic-tags/*.parquet")),
        key_column="hash",
        value_columns=["aesthetic"],
        output_folder="/media/hd2/laion5B-aesthetic-tags-ordered",
    )

elif step == 1:
    parquet_to_file(
        input_collection="/media/hd2/laion5B-aesthetic-tags-ordered",
        key_column="hash",
        value_columns=["aesthetic"],
        output_file="/media/hd2/laion5B-aesthetic-tags-kv",
        key_format="q",
        value_format="e",
    )
elif step == 2:
    import mmh3
    def compute_hash(url, text):
        if url is None:
            url = ''
        if text is None:
            text = ''
        total = (url + text).encode("utf-8")
        return mmh3.hash64(total)[0]

    # https://huggingface.co/datasets/laion/laion5B-watermark-safety-ordered

    kv = OnDiskKV(file="/media/hd2/laion5B-aesthetic-tags-kv", key_format="q", value_format="e")
    score = kv[compute_hash("http://1.bp.blogspot.com/-4nSga1sbduI/UDT3Q1H9dzI/AAAAAAAABWE/O7_lyFGjmU0/s1600/Vintage+Wedding+Table+Decorations+Pictures.jpg", "Cake Table Decoration Ideas")]

    print(score)
