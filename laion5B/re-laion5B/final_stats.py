from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

print("laion2B-en-md5-unsafe", spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-en-md5-unsafe").count())
print("laion2B-multi-md5-unsafe", spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-multi-md5-unsafe").count())
print("laion1B-nolang-md5-unsafe", spark.read.parquet("/media/hd2/relaion_research_unsafe/laion1B-nolang-md5-unsafe").count())