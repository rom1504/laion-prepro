from clip_retrieval import clip_inference
import shutil
import os
from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel

from pyspark import SparkConf, SparkContext


def create_spark_session():
    # this must be a path that is available on all worker nodes

    os.environ["PYSPARK_PYTHON"] = "/home/ubuntu/clip_retrieval.pex/__main__.py"
    spark = (
        SparkSession.builder.config("spark.submit.deployMode", "client")
        .config("spark.executorEnv.PEX_ROOT", "./.pex")
        .config("spark.task.resource.gpu.amount", "1")
        .config("spark.executor.resource.gpu.amount", "8")
        # .config("spark.executor.cores", "16")
        # .config("spark.cores.max", "48") # you can reduce this number if you want to use only some cores ; if you're using yarn the option name is different, check spark doc
        .config("spark.driver.port", "5678")
        .config("spark.driver.blockManager.port", "6678")
        .config("spark.driver.host", "172.31.44.42")
        .config("spark.driver.bindAddress", "172.31.44.42")
        .config("spark.executor.memory", "16G")  # make sure to increase this if you're using more cores per executor
        .config("spark.executor.memoryOverhead", "8G")
        .config("spark.task.maxFailures", "100")
        .master(
            "spark://172.31.44.42:7077"
        )  # this should point to your master node, if using the tunnelling version, keep this to localhost
        .appName("spark-stats")
        .getOrCreate()
    )
    return spark


spark = create_spark_session()

input_dataset = "pipe:aws s3 cp --quiet s3://laion-us-east-1/laion-data/laion2B-multi-data/{000000..226687}.tar -"
clip_inference(
    input_dataset=input_dataset,
    output_folder="s3://laion-us-east-1/embeddings/vit-l-14/laion2B-multi",
    input_format="webdataset",
    enable_metadata=True,
    write_batch_size=1000000,
    num_prepro_workers=8,
    batch_size=512,
    cache_path=None,
    enable_wandb=True,
    distribution_strategy="pyspark",
    clip_model="ViT-L/14",
)
