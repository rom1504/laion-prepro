#python big_join.py --initial_path "/media/hd/metadata/laion1B-nolang" --post_path "/media/hd2/allmeta/1Bnolang" --safety_path "/media/nvme/safety/laion1B-nolang-safety" --watermark_path "/media/nvme/watermark_hf/laion1B-nolang-watermark" --output_path "/media/hd2/final_laion_1B_nolang" --multi False
#python big_join.py --initial_path "/media/hd/metadata/laion2B-en" --post_path "/media/hd2/allmeta/2Ben" --safety_path "/media/nvme/safety/laion2B-en-safety" --watermark_path "/media/nvme/watermark_hf/laion2B-en-watermark" --output_path "/media/hd2/final_laion_2B_en" --multi False

cd /home/rom1504/cah-prepro/laion5B/watermark
python big_join.py --initial_path "/media/hd/metadata/laion2B-multi" --post_path "/media/hd2/allmeta/2Bmulti" --safety_path "/media/nvme/safety/laion2B-multi-safety" --watermark_path "/media/nvme/watermark_hf/laion2B-multi-watermark" --output_path "/media/hd2/final_laion_2B_multi" --multi True
cd /media/hd2/laion2B-multi-joined
mv /media/hd2/final_laion_2B_multi/* .
git add .
git commit -m "dataset"
git push