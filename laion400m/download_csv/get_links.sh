lynx -dump -hiddenlinks=listonly -nonumbers http://the-eye.eu/eleuther_staging/cah/ | rg "/cah/.*/$" > links.txt
# clean out file between runs
echo "" > links2.txt
# skip first line as the "presorted" url only has 1 level
for URL in $(tail links.txt -n+2)
do
    lynx -dump -hiddenlinks=listonly -nonumbers $URL | rg "$URL.*/$" >> links2.txt
done
# add the "presorted" url to the next level of crawling
#head -n1 links.txt >> links2.txt
# clean out file between runs
echo "" > download_urls_all.txt

#for URL in $(grep "202108" links2.txt)
for URL in $(ls links2.txt)
do
    lynx -dump -hiddenlinks=listonly -nonumbers $URL | rg cah | grep 'csv' >> download_urls_all.txt
done
#grep "202108" download_urls_all.txt > download_urls.txt
python3 to_aria.py