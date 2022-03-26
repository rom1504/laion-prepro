# coding=utf-8

from autofaiss import build_index
from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel

from pyspark import SparkConf, SparkContext
import os

def create_spark_session():
    # this must be a path that is available on all worker nodes
    
    os.environ['PYSPARK_PYTHON'] = "/home/ubuntu/autofaiss.pex"
    spark = (
        SparkSession.builder
        .config("spark.submit.deployMode", "client") \
        .config("spark.executorEnv.PEX_ROOT", "./.pex")
        #.config("spark.executor.cores", "16")
        #.config("spark.cores.max", "48") # you can reduce this number if you want to use only some cores ; if you're using yarn the option name is different, check spark doc
        .config("spark.task.cpus", "8")
        .config("spark.driver.port", "5678")
        .config("spark.driver.blockManager.port", "6678")
        .config("spark.driver.host", "172.31.38.232")
        .config("spark.driver.bindAddress", "172.31.38.232")
        .config("spark.executor.memory", "18G") # make sure to increase this if you're using more cores per executor
        .config("spark.executor.memoryOverhead", "8G")
        .config("spark.task.maxFailures", "100")
        .master("spark://172.31.38.232:7077") # this should point to your master node, if using the tunnelling version, keep this to localhost
        .appName("spark-stats")
        .getOrCreate()
    )
    return spark

spark = create_spark_session()

index, index_infos = build_index(
    embeddings=["s3://laion-us-east-1/embeddings/vit-l-14/laion2B-en/img_emb","s3://laion-us-east-1/embeddings/vit-l-14/laion2B-multi/img_emb","s3://laion-us-east-1/embeddings/vit-l-14/laion1B-nolang/img_emb"],
    distributed="pyspark",
    max_index_memory_usage="1600G",
    current_memory_available="55G",
    nb_indices_to_keep=160,
    file_format="npy",
    temporary_indices_folder="s3://laion-us-east-1/mytest/my_tmp_folder42",
    index_path="s3://laion-us-east-1/indices/vit-l-14/image_PQ256/knn.index",
    index_infos_path="s3://laion-us-east-1/indices/vit-l-14/image_PQ256/infos.json"
)