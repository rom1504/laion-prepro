# aesthetic

See [aesthetic dataset](https://github.com/LAION-AI/laion-datasets/blob/main/laion-aesthetic.md) for details

aesthetic_join.py builds the aesthetic subset :
* it joins joined collection and aesthetic tags
* keep sample with aesthetic >= 7

aesthetic_ondisk.py builds the full ondisk kv for hash -> aesthetic score

Sizes of output
* en 52M
* multi 51M
* nolang 17M
