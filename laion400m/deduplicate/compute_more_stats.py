'''
Once you computed the parquet files with unique items,
let's compute more stats
'''
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
def main():
  spark = SparkSession.builder.config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate() 
  df = spark.read.parquet("cah_dataframe_unique")
  df.printSchema()
  df.show(truncate=False)
  df[(df["WIDTH"] >= 1024) & (df["HEIGHT"] >= 1024)].select("URL", "TEXT").show(truncate=False)
  df[(df["similarity"] >= 0.4) & (df["similarity"] < 1.0)].show(truncate=False)
  print("width quantiles", df.approxQuantile("WIDTH", [0.1*x for x in range(1,10)], 0.1))
  print("height quantiles", df.approxQuantile("HEIGHT", [0.1*x for x in range(1,10)], 0.1))
  print("similarity quantiles", df.approxQuantile("similarity", [0.1*x for x in range(1,10)], 0.1))
  df = df.withColumn("lentext", F.length("TEXT"))  
  print("text length quantiles", df.approxQuantile("lentext", [0.1*x for x in range(1,10)], 0.1))
  print("Number of uniques", df.count())
  print("Number with height or width >= 1024", df[(df["WIDTH"] >= 1024) | (df["HEIGHT"] >= 1024)].count())
  print("Number with height and width >= 1024", df[(df["WIDTH"] >= 1024) & (df["HEIGHT"] >= 1024)].count())
  print("Number with height or width >= 512", df[(df["WIDTH"] >= 512) | (df["HEIGHT"] >= 512)].count())
  print("Number with height and width >= 512", df[(df["WIDTH"] >= 512) & (df["HEIGHT"] >= 512)].count())
  print("Number with height or width >= 256", df[(df["WIDTH"] >= 256) | (df["HEIGHT"] >= 256)].count())
  print("Number with height and width >= 256", df[(df["WIDTH"] >= 256) & (df["HEIGHT"] >= 256)].count())
  print("Number text length >= 100", df[df["lentext"] >= 100].count())
  print("Number with similarity >= 0.4", df[(df["similarity"] >= 0.4) & (df["similarity"] < 1.0)].count())

  
main()
