# cah-prepro
Get hundred of million of image+url from the crawling at home dataset and preprocess them.

This repository can be run on one machine with 32GB of ram, 8TB of disk, 16 i7 core and a 1Gbps connection.

## What is crawling at home ?

The crawling at home project has for objective to use commoncrawl to retrieve billions of aligned image+text pairs.
It is composed of a central server that track the progress of decentralized (run by anyone) workers that process small chunks of commoncrawl.
Currently, 300M such pairs have already been retrieved.
Read more about it at the [reddit post](https://www.reddit.com/r/DataHoarder/comments/oyta8q/crawlinghome_help_build_the_worlds_largest/)

## Visualization of the dataset

Check the [colab](https://colab.research.google.com/drive/14Hc_fUUOrG9260VzD_XsTxWX7f5cptyL?usp=sharing)

## Preprocesing steps

This preprocessing pipeline is composed of 3 steps:
1. downloading the raw csv files
2. reading them with spark to align the columns and remove the duplicates
3. downloading the images and producing the final dataset

It is possible to skip step 1 and 2 by using directly the files provided at [cah unique](http://3080.rom1504.fr/cah/cah_dataframe_unique/)

## Download csv

This steps takes about one hour.

Read more at [download_csv](download_csv)

## Deduplicate

This steps takes about one hour.

Read more at [deduplicate](deduplicate)

## Download images

This steps takes about 2 days.

Read more at [download_images](download_images)
