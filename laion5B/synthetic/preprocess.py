from pyspark.sql import SparkSession
import mmh3
from pyspark.sql.functions import rand, lit, udf
import fire
import glob

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType, ArrayType, MapType



import pandas as pd
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import ArrayType, StringType, MapType
import json

spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", "/media/hd2/spark-tmp").config("spark.driver.memory", "16G") .master("local[1]").appName('spark-stats').getOrCreate()

files = list(glob.glob("/media/hd2/laion-synthetic-json/*.json"))

df = (
    spark.sparkContext.parallelize(
        [
            (file,) for file in files
        ]
    )
    .toDF(["file"])
)

return_type = ArrayType(MapType(StringType(), StringType()))


@udf(returnType=return_type)
def your_udf_func(file):
    captioning_result = json.load(open(file))
    values = []
    for s in captioning_result.values():
        metadata = json.loads(list(s.values())[0])
        print(metadata)
        url = metadata['url']
        text = metadata['caption']
        best_caption = list(s.values())[1]
        all_captions = list(s.values())[2]
        all_similarities = list(s.values())[3]

        if url and best_caption:
            values.append({'url': url, 'original_caption': text, 'best_caption': best_caption,
            'all_captions': all_captions, 'all_similarities': all_similarities})

    return list(values)

extracted = your_udf_func("file")
exploded = explode(extracted).alias("exploded")
expanded = [
    col("exploded").getItem(k).alias(k) for k in ["url", 'original_caption', "best_caption", "all_captions", "all_similarities"]
]

result = df.select(exploded).select(*expanded) 

result.show()
#result.repartition(128).write.mode('overwrite').parquet("/media/hd2/laion-synthetic-parquet")
