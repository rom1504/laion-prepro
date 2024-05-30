
from pyspark.sql import SparkSession
from  pyspark.sql.functions import lit, rand
from pyspark.sql.functions import broadcast


spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()


en_filtered =spark.read.parquet("/media/hd2/david/laion2B-en-md5-david")
multi_filtered = spark.read.parquet("/media/hd2/david/laion2B-multi-md5-david")
nolang_filtered = spark.read.parquet("/media/hd2/david/laion1B-nolang-md5-david")
def compute_stats(name, df):
    print(name)
    print("Count", df.count())
    print("punsafe null", df.filter(df.punsafe.isNull()).count())
    print("punsafe < 0.45", df.filter(df.punsafe < 0.45).count())

compute_stats("laion2B-en david", en_filtered)
compute_stats("laion2B-multi david", multi_filtered)
compute_stats("laion2B-nolang david", nolang_filtered)
