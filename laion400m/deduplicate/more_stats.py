
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import from_json, col



spark = SparkSession.builder.config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate() 

df = spark.read.parquet("cah400M_embs/metadata")
exif_only = df.select("exif").filter(F.col("exif") != "{}")
exif_only_caption = df.select("exif", "caption").filter(F.col("exif") != "{}")

exif_only.count()

gps_latitude = exif_only.filter(exif_only["exif"].contains("\"GPS GPSLatitude\""))

schema = StructType([ StructField("GPS GPSLatitude",StringType(),True),])
geo = gps_latitude.withColumn("jsonData",from_json(col("exif"),schema)).select("jsonData.*")


schema = StructType([ StructField("Image Copyright",StringType(),True),])
copyright = exif_only.withColumn("jsonData",from_json(col("exif"),schema)).select("jsonData.*")



exif_only[exif_only["exif"].contains("\"Image ImageDescription\"")].count()

exif_only[exif_only["exif"].contains("\"Image Model\"")].count()

exif_only[exif_only["exif"].contains("\"Image DateTime\"")].count()


schema = StructType([ StructField("Image ImageDescription",StringType(),True),])
description = exif_only_caption[exif_only_caption["exif"].contains("\"Image ImageDescription\"")].withColumn("jsonData",from_json(col("exif"),schema)).select("jsonData.*", "caption")

domains = df.select("url").withColumn("domain", F.split(df['url'], '/').getItem(2)).select("domain")

domains.agg(F.countDistinct("domain")).collect()

