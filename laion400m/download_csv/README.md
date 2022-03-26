## download csv

This is about downloading http://the-eye.eu/eleuther_staging/cah/ which is a big dataset of 
image/text pairs filtered from common crawl

1. run `get_links.sh` ; this will produce a to_aria.txt file which contains all the urls to download and where to put them
2. run `download.sh` ; it will use aria2c to download files fast (takes about 1h)

Note if you only want one type of file, you may change this part `grep 'csv\|txt\|pkl\|tfrecord'`

