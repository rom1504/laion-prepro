lynx -dump -hiddenlinks=listonly -nonumbers https://deploy.laion.ai/0fed69941beeabaebbedc2671b929435/ | grep deploy | grep -v people | grep json > links.txt

python3 to_aria.py
