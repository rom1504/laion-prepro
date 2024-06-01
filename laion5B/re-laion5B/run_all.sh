set -e
python /home/rom1504/cah-prepro/laion5B/re-laion5B/merge.py
python /home/rom1504/cah-prepro/laion5B/csam/additional_stats.py
python /home/rom1504/cah-prepro/laion5B/csam/overlap_human_rights_watch.py
python /home/rom1504/cah-prepro/laion5B/re-laion5B/filter_send.py