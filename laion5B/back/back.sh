export CUDA_VISIBLE_DEVICES=
clip-retrieval back --provide_safety_model True  --clip_model="ViT-L/14" --default_backend="https://knn5.laion.ai/" --port 1234 --indices-paths indices.json --use_arrow True --enable_faiss_memory_mapping True --columns_to_return='["url", "caption", "md5"]'
