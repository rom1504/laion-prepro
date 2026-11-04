from glob import glob
import pandas as pd

# this is used after calling iwf api

a = glob("/media/hd2/iwf2/*/*.parquet")

d = pd.concat([pd.read_parquet(e) for e in a])

uniques = d.drop_duplicates("md5")

uniques.to_parquet("/media/hd2/iwf2/uniques.parquet")