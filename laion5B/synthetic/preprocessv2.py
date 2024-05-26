from pyspark.sql import SparkSession
import mmh3
from pyspark.sql.functions import rand, lit, udf
import fire
import os
import glob

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType, ArrayType, MapType



import pandas as pd
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import ArrayType, StringType, MapType
import json

spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", "/media/hd2/spark-tmp").config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate()

files = list(glob.glob("/media/hd2/laion-synthetic-json/*.json"))

df = (
    spark.sparkContext.parallelize(
        [
            (file,) for file in files
        ]
    )
    .toDF(["file"])
)

output = "/media/hd2/laion-synthetic-out"


def your_udf_func(file):
    output_file = output + "/" + file.split("/")[-1].replace(".json", ".parquet")
    if os.path.exists(output_file):
        return 1
    try:
        with open(file) as f:
            captioning_result = json.load(f)
            values = []
            for s in captioning_result.values():
                try:
                    metadata = json.loads(list(s.values())[0])
                    url = metadata['url']
                    text = metadata['caption']
                    top_caption = list(s.values())[1]
                    all_captions = list(s.values())[2]
                    all_similarities = list(s.values())[3]

                    if url and top_caption:
                        values.append({'url': url, 'original_caption': text, 'top_caption': top_caption, 
                        'all_captions': all_captions, 'all_similarities': all_similarities})
                except json.decoder.JSONDecodeError:
                    print("JSONDecodeError", list(s.values())[0])
                    continue

            df = pd.DataFrame(values)
            df.to_parquet(output_file, index=False)

            return 1
    except json.decoder.JSONDecodeError:
        print("JSONDecodeError", file)
        return 0

result = df.withColumn("a", udf(your_udf_func)("file"))

result.repartition(128).write.mode('overwrite').parquet("/media/hd2/laion-synthetic-nope")
