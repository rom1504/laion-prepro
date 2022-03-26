# Laion400m


### Preprocesing steps

This preprocessing pipeline is composed of 3 steps:
1. downloading the raw csv files
2. reading them with spark to align the columns and remove the duplicates
3. downloading the images and producing the final dataset
4. compute clip embeddings and indices

It is possible to skip step 1 and 2 by using directly the files provided at [laion 400m meta release](https://the-eye.eu/public/AI/cah/laion400m-met-release/laion400m-meta/)

### Download csv

This step takes about one hour.

Read more at [download_csv](laion400m/download_csv)

### Deduplicate

After a fast to run script to download the csv files, The first step of this post processing pipeline is to do deduplicated by url+caption. The first pipeline does some partial deduplication by using a bloom filter, but it is approximate and some duplicate remain. Doing that pyspark post processing also make it possible to reduce the number of metadata files from hundred of thousands to 32 parquet files of size 1GB. See this deduplication script there. Pyspark would be a good way to do any further filtering and an example is provided to compute some statistics.

The resulting output is 32 parquet files containing columns such as url, text, nsfw,..

This step takes about one hour.

Read more at [deduplicate](deduplicate)

### Download images

Once this set of 50GB parquet files has been produced, the img2dataset tool is used to download, resize and store as webdataset the images and captions. This tool can download 100M images in 20h in a single node, so anyone can run this for the whole dataset or a smaller subset.

The format this tool outputs is a collection of tar files (that dataset format is called webdataset) containing images, captions and metadata and corresponding parquet files containing the same metadata

* 00000.tar of size 270MB containing at most 10k samples
  * 0.jpg
  * 0.txt containing the caption
  * 0.json containing metadata such as the url, the original width, the exif data, whether the image is NSFW
* 00000.parquet of size 1.6MB containing the same metadata as the json file. Useful to compute statistics without reading all the tar files

The 400M dataset has 41455 tar and parquet files.

This step takes about 4 days.

Read more at [download_images](download_images)

### Compute clip embeddings and indices

Finally the tar dataset is used to compute and package clip embeddings and use these to compute a knn index over the clip embeddings. The clip-retrieval tool make it fast to compute 100M embeddings per 20h with a single 3080 gpu, so it's possible to rerun this part on the whole dataset or on subset at low cost.

The embeddings are stored in npy files next to parquet files in the same order. Since this dataset is much smaller than the image one, 1M sample are stored per npy file. Each npy file is 1GB, and each parquet files is 150MB. There are a total of 400 such files.

These embeddings are then used to build a text and an image knn index using the autofaiss tool which makes it possible to produce a quantized index of arbitrary file. The chosen index type is of size 16GB so it's cheap for anyone to load it and run fast (10ms) queries over the whole dataset. A simple web demo shows the results. Thanks to faiss memory mapping, it requires no ram to load the index.

This step takes about 2 days

Find the precise commands for inference, indexing and hosting at [embeddings-indices-hosting](embeddings-indices-hosting)

see https://github.com/rom1504/clip-retrieval
