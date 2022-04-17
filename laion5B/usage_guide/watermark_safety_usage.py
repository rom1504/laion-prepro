import mmh3
from static_ondisk_kv import OnDiskKV

def compute_hash(url, text):
  if url is None:
    url = ''
  if text is None:
    text = ''
  total = (url + text).encode("utf-8")
  return mmh3.hash64(total)[0]

# https://huggingface.co/datasets/laion/laion5B-watermark-safety-ordered

kv = OnDiskKV(file="/media/nvme/laion5B-watermark-safety-ordered/laion5B-watermark-safety-ordered", key_format="q", value_format="ee")
pwatermark, punsafe = kv[compute_hash("http://i0.wp.com/ajournalofmusicalthings.com/wp-content/uploads/2016/05/Banjoe-the-Bulldog-copy.jpg?resize=290%2C166", "Banjoe the Bulldog copy")]

print(pwatermark)
print(punsafe)
print(pwatermark > 0.8)
print(punsafe > 0.5)
