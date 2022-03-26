'''
Compute some stats on cah collection
First get the files with:
https://gist.github.com/rom1504/f427b1c82df26c9993daa36fca7f9881
Then pip install pyspark
Then run this file. It also takes a few minutes

The main thing this script is doing is adding/removing/reordering csv columns and converting to fewer parquet files
The end result is easy to use in spark, pandas or anything else
'''

from glob import glob
from multiprocessing import Pool
from collections import defaultdict
from pathlib import Path
import json

def f(w):
  return open(w, "r").readline().rstrip().split("|")

def main():
  p = Pool(128)
  # necessary because the schema changed

  print("Retrieving columns of all csv files")
  fs = [str(x) for x in Path('/media/hd/cah/drive').glob("**/*.csv")] + [str(x) for x in Path('/media/hd/cah/theeye/output/cah').glob("**/*.csv")]
  #fs = [x for x in fs if "202108"in x]
  #fs = [x for x in fs if x.replace("/media/hd/cah/theeye/output/cah/", "").split("/")[0] >= "20210818"]
  headers = p.map(f, fs)
  all = list(zip(headers,fs))
  print("Grouping files by columns")
  d = defaultdict(list)
  for cols, path in all:
    d[",".join(cols)].append(path)
  
  d = {k:v for k, v in d.items() if "similarity" in k}
  with open('/media/hd/testing/columns.json', 'w') as fp:
    json.dump(d, fp,  indent=4)

  print("Starting spark session")
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import lit, rand
  # You can open http://localhost:4040 to follow progress on the spark operations
  spark = SparkSession.builder.config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate() 

  ref_cols = ['SAMPLE_ID','URL','TEXT','HEIGHT','WIDTH','LICENSE','NSFW','similarity']
  total = None
  print("Reading all collections of csv, removing, adding and reordering columns as needed")
  for cols, paths in d.items():
    cols = cols.split(",")
    incols = [x for x in cols if x in ref_cols]
    print("incols", incols)
    w = spark.read.options(delimiter="|", header=True).csv(paths).select(*incols)
    addcols = [x for x in ref_cols if x not in cols]
    print("addcols", addcols)
    for c in addcols:
      w = w.withColumn(c, lit(""))
    w = w.select(*ref_cols)
    if total is None:
      total = w
    else:
      total = total.union(w)

  print("Casting columns to the right types")
  total = total.withColumn("SAMPLE_ID", total["SAMPLE_ID"].cast("bigint"))
  total = total.withColumn("WIDTH", total["WIDTH"].cast("int"))
  total = total.withColumn("HEIGHT", total["HEIGHT"].cast("int"))
  total = total.withColumn("similarity", total["similarity"].cast("double"))
  
  print("Repartitionning and writing to 32 parquet files to cah_dataframe")
  total.repartition(32).write.mode("overwrite").parquet("cah_dataframe")

  #old_unique = spark.read.parquet("cah_dataframe_unique")
  #old_unique.write.mode("overwrite").parquet("old_cah_unique")

  #old_unique = spark.read.parquet("old_cah_unique")

  ok = spark.read.parquet("cah_dataframe")
  #ok = ok.union(old_unique)
  print("Rereading the parquet and computing some basic stats")
  print("Size of collection", ok.count())
  uniques = ok.drop_duplicates(["URL", "TEXT"])
  uniques_shuffled = uniques.orderBy(rand())
  uniques_shuffled.repartition(32).write.mode("overwrite").parquet("cah_dataframe_unique")
  ok_unique = spark.read.parquet("cah_dataframe_unique")
  print("Number of uniques", ok_unique.count())

main()
