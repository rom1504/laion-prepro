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

I recommend a loader like this.

Note that you may choose to add additional filter to keep only some of the data, even after it has already been downloaded.

```python
def create_webdataset(
    urls,
    image_transform,
    enable_text=True,
    enable_image=True,
    image_key="jpg",
    caption_key="txt",
    enable_metadata=False,
    cache_path=None,
):
    """Create a WebDataset reader, it can read a webdataset of image, text and json"""
    import clip  # pylint: disable=import-outside-toplevel
    import webdataset as wds  # pylint: disable=import-outside-toplevel


    dataset = wds.WebDataset(urls, cache_dir=cache_path, cache_size=10 ** 10, handler=wds.handlers.warn_and_continue)
    tokenizer = lambda text: clip.tokenize([text], truncate=True)[0]

    def filter_dataset(item):
        if enable_text and caption_key not in item:
            return False
        if enable_image and image_key not in item:
            return False
        if enable_metadata and "json" not in item:
            return False
        return True

    filtered_dataset = dataset.select(filter_dataset)

    def preprocess_dataset(item):
        output = {}
        if enable_image:
            image_data = item[image_key]
            image = Image.open(io.BytesIO(image_data))
            image_tensor = image_transform(image)
            output["image_filename"] = item["__key__"]
            output["image_tensor"] = image_tensor

        if enable_text:
            text = item[caption_key]
            caption = text.decode("utf-8")
            tokenized_text = tokenizer(caption)
            output["text_tokens"] = tokenized_text
            output["text"] = caption

        if enable_metadata:
            metadata_file = item["json"]
            metadata = metadata_file.decode("utf-8")
            output["metadata"] = metadata
        return output

    transformed_dataset = filtered_dataset.map(preprocess_dataset, handler=wds.handlers.warn_and_continue)
    return transformed_dataset


def dataset_to_dataloader(dataset, batch_size, num_prepro_workers, input_format):
    """Create a pytorch dataloader from a dataset"""

    def collate_fn(batch):
        batch = list(filter(lambda x: x is not None, batch))
        return default_collate(batch)

    data = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_prepro_workers,
        pin_memory=True,
        prefetch_factor=2,
        collate_fn=collate_fn if input_format == "files" else None,
    )
    return data


class WebdatasetReader:
    """WebdatasetReader is a reader that reads samples from a webdataset"""

    def __init__(
        self,
        preprocess,
        input_dataset,
        batch_size,
        num_prepro_workers,
        enable_text=True,
        enable_image=True,
        enable_metadata=False,
        wds_image_key="jpg",
        wds_caption_key="txt",
        cache_path=None,
    ):
        self.batch_size = batch_size
        dataset = create_webdataset(
            input_dataset,
            preprocess,
            enable_text=enable_text,
            enable_image=enable_image,
            image_key=wds_image_key,
            caption_key=wds_caption_key,
            enable_metadata=enable_metadata,
            cache_path=cache_path,
        )
        self.dataloader = dataset_to_dataloader(dataset, batch_size, num_prepro_workers, "webdataset")

    def __iter__(self):
        for batch in self.dataloader:
            yield batch

```

```python 
os.environ["CUDA_VISIBLE_DEVICES"] = ""
tar_folder = "test_tars"
input_dataset = [tar_folder + "/image1.tar", tar_folder + "/image2.tar"]
batch_size = 2
num_prepro_workers = 2
_, preprocess = load_clip()

output_partition_count = 2
actual_values = []
reader = WebdatasetReader(
    preprocess,
    input_dataset,
    batch_size,
    num_prepro_workers,
    enable_text=True,
    enable_image=True,
    enable_metadata=True,
)

for batch in reader:
    break
```