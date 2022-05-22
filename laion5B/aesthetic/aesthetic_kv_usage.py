import mmh3
from static_ondisk_kv import OnDiskKV
def compute_hash(url, text):
    if url is None:
        url = ''
    if text is None:
        text = ''
    total = (url + text).encode("utf-8")
    return mmh3.hash64(total)[0]

# https://huggingface.co/datasets/laion/laion5B-aesthetic-tags-kv

kv = OnDiskKV(file="/media/hd2/laion5B-aesthetic-tags-kv", key_format="q", value_format="e")
score = kv[compute_hash("http://1.bp.blogspot.com/-4nSga1sbduI/UDT3Q1H9dzI/AAAAAAAABWE/O7_lyFGjmU0/s1600/Vintage+Wedding+Table+Decorations+Pictures.jpg", "Cake Table Decoration Ideas")]

print(score)