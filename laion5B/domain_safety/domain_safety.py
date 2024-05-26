
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import unicodedata
from urllib.parse import urlparse
def normalize_domain(domain):
  domain = domain.lower()
  if domain.startswith("www."):
        domain = domain[4:]
  return domain

def normalize_url(url):
    normalized_url = unicodedata.normalize('NFKC', url)
    return normalized_url

def extract_domain(url):
    if url is None:
        return None
    parsed_url = urlparse(normalize_url(url))
    domain = parsed_url.netloc
    return normalize_domain(domain)


spark = SparkSession.builder.config("spark.local.dir", "/media/nvme/spark-tmp").config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate() 

extract_domain_udf = F.udf(extract_domain)

df = spark.read.parquet("/media/hd2/backup/laion2B-en-joined/*.parquet").select("URL", "punsafe").union(spark.read.parquet("/media/hd2/backup/laion2B-multi-joined/*.parquet").select("URL", "punsafe")).union(spark.read.parquet("/media/hd2/backup/laion1B-nolang-joined/*.parquet").select("URL", "punsafe"))

domains = df.select("URL", "punsafe").withColumn("domain", extract_domain_udf(df['URL'])).select("domain", "punsafe").groupBy("domain")

domains = domains.agg(F.count("domain").alias("count"), F.avg("punsafe").alias("avg_punsafe")).select("domain", "count", "avg_punsafe")

domains = domains.orderBy(["avg_punsafe", "count"], ascending=False)

domains.repartition(100).repartition(1).write.mode("overwrite").parquet("/media/hd2/laion-domains-punsafe")

result = spark.read.parquet("/media/hd2/laion-domains-punsafe")

result.show(100, False)

print(result.count())