'''
Once you computed the parquet files with unique items,
let's compute more stats
'''
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
def main():
  spark = SparkSession.builder.config("spark.driver.memory", "16G") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate() 

  root_path = "/media/hd/metadata"

  # laion1B-nolang, laion2B-en, laion2B-multi
  datasets = [(False, True), (False, False), (True, False)]

  for (multilingual, nolang) in datasets:
    sub_path = "laion1B-nolang" if nolang else ( "/laion2B-multi" if multilingual else "/laion2B-en")
    print("Compute stats for {}".format(sub_path))
    path = root_path + "/" + sub_path
    df = spark.read.parquet(path)
    df = df.withColumnRenamed("WIDTH", "h")
    df = df.withColumnRenamed("HEIGHT", "w")
    df = df.withColumnRenamed("h", "HEIGHT")
    df = df.withColumnRenamed("w", "WIDTH")
    df = df.drop('h', 'w')

    def nm(n):
      m = int(n / (10**6))
      return str(m) + "M ("+str(n)+")"

    print("Number of uniques", nm(df.count()))
    """
    if multilingual:
      counts = df.groupBy("LANGUAGE").count().sort(-F.col("count")).toPandas()
      t = counts["count"].sum()
      counts["proportion"]= counts["count"].map(lambda a: float(a)/t)
      counts = counts[:101]
      print(counts)
      counts.to_csv('language_stats.csv', sep ='\t')
    """
    #df.show(truncate=False)
    #df[(df["WIDTH"] >= 1024) & (df["HEIGHT"] >= 1024)].select("URL", "TEXT").show(truncate=False)
    #df[(df["similarity"] >= 0.4) & (df["similarity"] < 1.0)].show(truncate=False)
    """
    print("Number with height or width >= 1024", nm(df[(df["WIDTH"] >= 1024) | (df["HEIGHT"] >= 1024)].count()))
    print("Number with height and width >= 1024", nm(df[(df["WIDTH"] >= 1024) & (df["HEIGHT"] >= 1024)].count()))
    print("Number with height or width >= 512", nm(df[(df["WIDTH"] >= 512) | (df["HEIGHT"] >= 512)].count()))
    print("Number with height and width >= 512", nm(df[(df["WIDTH"] >= 512) & (df["HEIGHT"] >= 512)].count()))
    print("Number with height and width >= 336", nm(df[(df["WIDTH"] >= 336) & (df["HEIGHT"] >= 336)].count()))
    print("Number with height or width >= 256", nm(df[(df["WIDTH"] >= 256) | (df["HEIGHT"] >= 256)].count()))
    print("Number with height and width >= 256", nm(df[(df["WIDTH"] >= 256) & (df["HEIGHT"] >= 256)].count()))
    print("Number with height and width <= 128", nm(df[(df["WIDTH"] <= 128) & (df["HEIGHT"] <= 128)].count()))
    print("Number with height and width <= 64", nm(df[(df["WIDTH"] <= 64) & (df["HEIGHT"] <= 64)].count()))
    print("Number with height and width <= 32", nm(df[(df["WIDTH"] <= 32) & (df["HEIGHT"] <= 32)].count()))
    """
    #print("similarity quantiles", df.approxQuantile("similarity", [0.1*x for x in range(1,10)], 0.1))
    #print("height quantiles", df.approxQuantile("HEIGHT", [0.05*x for x in range(1,20)], 0.01))
    #print("width quantiles", df.approxQuantile("WIDTH", [0.05*x for x in range(1,20)], 0.01))


    size_range = [[0,128], [128,256],[256,512], [512, 1024], [1024, None]]
    text_range = [[0,25], [25,50],[50,100], [100,150], [150, None]]

    def compute_range_stats(field_name, ranges):
      selected = df.select(field_name).persist()
      for r in ranges:
        if r[1] is None:
          print("Number with "+field_name+" >=", r[0], nm(selected[(selected[field_name] >= r[0])].count()))
        else:
          print("Number with "+field_name+" >=", r[0], "and "+field_name+" <=", r[1], \
            nm(selected[(selected[field_name] >= r[0]) & (selected[field_name] <= r[1])].count()))

    df = df.withColumn("lentext", F.length("TEXT"))
    compute_range_stats("WIDTH", size_range)
    compute_range_stats("HEIGHT", size_range)
    compute_range_stats("lentext", text_range)

    """
    df = df.withColumn("lentext", F.length("TEXT")).select("lentext").persist()
    print("average text length", df.agg(F.avg("lentext")).collect()[0][0])
    print("text length quantiles", df.approxQuantile("lentext", [0.05*x for x in range(1,20)], 0.01))
    print("Number text length >= 100", df[df["lentext"] >= 100].count())
    """

  
main()
