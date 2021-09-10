## Computing embeddings and indices

### inference

First run (takes a few days):
```
time clip-retrieval inference --input_dataset="http://the-eye.eu/eleuther_staging/cah/releases/laion400m/{00000..41455}.tar" --output_folder="/media/hd/testing/cah400M_embs" \
        --input_format "webdataset" --enable_metadata=True --write_batch_size=1000000 --batch_size=512 --cache_path=None --num_prepro_workers=8
```

### indexing

Then run :
```
time clip-retrieval index --embeddings_folder="/media/hd/testing/cah400M_embs" --index_folder="/media/hd/testing/my_index" --max_index_memory_usage="16G"
```

### hosting

with clip retrieval you can host the laion-400m index with these resources:
* 100GB of ssd space
* 4GB of ram
* low cpu usage

Follow these instructions:

First download the indices and metadata:
```
mkdir index/
wget http://3080.rom1504.fr/cah/cah_400M_16G_index/image.index -O index/image.index
wget http://3080.rom1504.fr/cah/cah_400M_16G_index/text.index -O index/text.index
wget http://3080.rom1504.fr/cah/url_caption_laion_400m.hdf5 -O index/metadata.hdf5
```

Then install clip retrieval:
```
python3 -m venv .env
source .env/bin/activate
pip install -U pip
pip install clip-retrieval
```

Then create the config file:
```
echo '{"laion_400m": "index"}'
```

Then start it:
```
clip-retrieval back --port 1234 --indices-paths indices_paths.json --enable_hdf5 True --enable_faiss_memory_mapping True
```

You will then be able to use that backend in the [web demo](https://rom1504.github.io/clip-retrieval/)
You may need to expose the backend over https (use nginx for example)

You can also run the front yourself with:
```
npm install -g clip-retrieval-front
clip-retrieval-front 3005
```
