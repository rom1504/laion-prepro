from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()

#print("laion2B-en-md5-unsafe", spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-en-md5-unsafe").count())
#print("laion2B-multi-md5-unsafe", spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-multi-md5-unsafe").count())
#print("laion1B-nolang-md5-unsafe", spark.read.parquet("/media/hd2/relaion_research_unsafe/laion1B-nolang-md5-unsafe").count())

print("relaion2B-en-research", spark.read.parquet("/media/hd2/relaion_research/relaion2B-en-research").count())
print("relaion2B-multi-research", spark.read.parquet("/media/hd2/relaion_research/relaion2B-multi-research").count())
print("relaion1B-nolang-research", spark.read.parquet("/media/hd2/relaion_research/relaion1B-nolang-research").count())

print("relaion2B-en-research-safe", spark.read.parquet("/media/hd2/relaion_research_safe/relaion2B-en-research-safe").count())
print("relaion2B-multi-research-safe", spark.read.parquet("/media/hd2/relaion_research_safe/relaion2B-multi-research-safe").count())
print("relaion1B-nolang-research-safe", spark.read.parquet("/media/hd2/relaion_research_safe/relaion1B-nolang-research-safe").count())