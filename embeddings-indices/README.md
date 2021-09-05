## Computing embeddings and indices

First run (takes a few days):
```
time clip-retrieval inference --input_dataset="http://the-eye.eu/eleuther_staging/cah/releases/laion400m/{00000..41455}.tar" --output_folder="/media/hd/testing/cah400M_embs" \
        --input_format "webdataset" --enable_metadata=True --write_batch_size=1000000 --batch_size=512 --cache_path=None --num_prepro_workers=8
```

Then run :
```
time clip-retrieval index --embeddings_folder="/media/hd/testing/cah400M_embs" --index_folder="/media/hd/testing/my_index"
```
