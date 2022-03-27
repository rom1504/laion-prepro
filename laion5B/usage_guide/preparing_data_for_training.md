# Preparing data for training

So you want to use laion5B for training image/text models ? Great idea!

First read the laion5B post at ...


## Guides

You may want to check these guides:
* to run img2dataset https://github.com/rom1504/img2dataset/blob/main/dataset_examples/laion5B.md
* to run clip inference https://github.com/rom1504/clip-retrieval/blob/main/docs/distributed_clip_inference.md
* to index the resulting embeddings https://github.com/criteo/autofaiss/blob/master/docs/distributed/distributed_autofaiss.md


## Plan

1. Selecting the data
2. Filtering the metadata
3. Downloading the data
4. Data loader


## Selecting the data

The first thing you should decide is what data you want to train with, what resolution and what format.

### Subset selection

Subsets to choose among:
* laion2B-en : only english captions
* laion2B-multi and laion1B-nolang : multilingual captions

Once you've chosen your language subset, you can further subselect data:
* restrict by resolution using the width and height fields
* keep only the safe images by using the safety tags
* keep only the image without watermarks

### Format and resolution

Resolution 224 and 384 are popular choice. Be aware that 384 is 3x bigger than 284.
You may also choose not to upsample images that are smaller than this threshold.
An example of choice can be summarized with these img2dataset options:
```
   image_size=384,
   resize_only_if_bigger=True,
   resize_mode="keep_ratio",
   skip_reencode=True,
```

Downloading the whole laion5B with these options requires 240TB.

For the choice of format, I recommend one of these 2 options:
* webdataset/tar if using pytorch
* tfrecord if using keras or jax

note that webdataset is also possible for keras or jax, but a bit less convenient

## Downloading the metadata

Once you're happy with your subset selection, the first step is downloading the metadata.
You can download it from huggingface at ...


## Filtering the metadata

I recommend using pyspark to do that, for example like this:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import rand
def main():
  spark = SparkSession.builder.config("spark.driver.memory", "16G") .master("local[16]").appName('spark-stats').getOrCreate() 
  df = spark.read.parquet("laion2B")

  df.filter((df.width >= 1024) & (df.height >= 1024))
  df = df.orderBy(rand()) # this line is important to have a shuffled dataset

  df.repartition(128).write("laion2B_big")
```

Note that pyspark is much faster if using a ssd drive, even better using a ssd nvme drive.

## Downloading the data

see https://github.com/rom1504/img2dataset/blob/main/dataset_examples/laion400m.md if using a single machine
or https://github.com/rom1504/img2dataset/blob/main/dataset_examples/laion5B.md if using multiple nodes

## Data loading

https://webdataset.github.io/webdataset/ is a great lib for data loading

I recommend a loader like this for pytorch https://github.com/rom1504/laion-prepro/blob/main/laion5B/usage_guide/dataloader_pytorch.py 

Note that you may choose to add additional filter to keep only some of the data, even after it has already been downloaded.

for jax, see this for example
https://github.com/crowsonkb/cloob-training/blob/136ca7dd69a03eeb6ad525da991d5d7083e44055/train.py#L299 
