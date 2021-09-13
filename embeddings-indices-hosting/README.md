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
time clip-retrieval index --embeddings_folder="laion400m-embeddings" --index_folder="index_folder" --max_index_memory_usage="16G" --current_memory_available="22G" --copy_metadata True
```

### scoring

If you want to know the recall score of the index, run this:
```
time autofaiss score_index --embeddings_path="laion400m-embeddings/img_emb" --index_path="index_folder/image.index" --is_local_index_path True --current_memory_available="22G"
```

### hosting

with clip retrieval you can host the laion-400m index with these resources:
* 100GB of ssd space
* 4GB of ram
* low cpu usage

Follow these instructions:

First download the indices and metadata:
```
mkdir index_folder/
wget https://the-eye.eu/public/AI/cah/laion400m-indexes/laion400m-16GB-index/image.index -O index_folder/image.index
wget https://the-eye.eu/public/AI/cah/laion400m-indexes/laion400m-16GB-index/text.index -O index_folder/text.index
wget https://the-eye.eu/public/AI/cah/laion400m-extras/url_caption_laion_400m.hdf5 -O index_folder/metadata.hdf5
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
echo '{"laion_400m": "index_folder"}' > indices_paths.json
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

Example nginx+certbot setup:
```
server {
        server_name subdomain.domain.com;
        location / {
                proxy_set_header Host $host;
                proxy_set_header X-Real-IP $remote_addr;
                proxy_pass http://127.0.0.1:1234;
                client_max_body_size 20M;
        }
}

in /etc/nginx/sites-available/clip and /etc/nginx/sites-enabled/clip
```
`sudo certbot --nginx -d subdomain.domain.com` to setup https
