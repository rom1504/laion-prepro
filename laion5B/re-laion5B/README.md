relaion is built by
- getting all the urls to filter from various scripts (see csam folder)
- merging that in merge.py
- filtering the to-filter using filter.py and produce the final datasets

There are 2 variants of datasets
- safe that discards all punsafe>0.45
- research safe that discards all punsafe>0.95 and matching a flagged keyword

safe is more conservative than safe.