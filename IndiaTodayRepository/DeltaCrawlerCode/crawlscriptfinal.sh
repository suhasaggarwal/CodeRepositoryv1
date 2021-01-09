#!/bin/bash
timeout 3m java -jar /mnt/data/deltaurlgeneratorv2.jar itweben /mnt/data/indiatodaydeltaurls0.txt auto,movies,india,education,coronavirus,technology,sports,lifestyle,information,business,television,science,trending,world,story,news 300 3000
timeout 7m java -jar /mnt/data/deltaurlgeneratorv2.jar ajtk /mnt/data/ajtkdelta0.txt auto,india,entertainment,trending,lifestyle,youtube,crime,religion,business,coronavirus,sports,world,technology,elections,education,tez,news,story 300 3000
#split -l$((`wc -l < indiatodayurls0.txt`/6)) indiatodaydeltaurls0.txt indiatodaydeltaurls0 --numeric-suffixes
#split -l$((`wc -l < ajtkdelta0.txt`/6)) ajtkdelta0.txt ajtkdelta0 --numeric-suffixes
cd /mnt/data && split -n l/6 -d /mnt/data/indiatodaydeltaurls0.txt indiatodaydeltaurls0
cd /mnt/data && split -n l/6 -d /mnt/data/ajtkdelta0.txt ajtkdelta0
PATH=$PATH:/usr/local/bin
export PATH
cd /mnt/data/indiatoday0/indiatoday0/spiders && rm -f /mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta00.json
cd /mnt/data/indiatoday0/indiatoday0/spiders && rm -f /mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta01.json
cd /mnt/data/indiatoday0/indiatoday0/spiders && rm -f /mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta02.json
cd /mnt/data/indiatoday0/indiatoday0/spiders && rm -f /mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta03.json
cd /mnt/data/indiatoday0/indiatoday0/spiders && rm -f /mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta04.json
cd /mnt/data/indiatoday0/indiatoday0/spiders && rm -f /mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta05.json


cd /mnt/data/aajtak0/aajtak0/spiders && rm -f /mnt/data/aajtak0/aajtak0/spiders/ajtkdelta00.json
cd /mnt/data/aajtak0/aajtak0/spiders && rm -f /mnt/data/aajtak0/aajtak0/spiders/ajtkdelta01.json
cd /mnt/data/aajtak0/aajtak0/spiders && rm -f /mnt/data/aajtak0/aajtak0/spiders/ajtkdelta02.json
cd /mnt/data/aajtak0/aajtak0/spiders && rm -f /mnt/data/aajtak0/aajtak0/spiders/ajtkdelta03.json
cd /mnt/data/aajtak0/aajtak0/spiders && rm -f /mnt/data/aajtak0/aajtak0/spiders/ajtkdelta04.json
cd /mnt/data/aajtak0/aajtak0/spiders && rm -f /mnt/data/aajtak0/aajtak0/spiders/ajtkdelta05.json


cd /mnt/data/indiatoday0/indiatoday0/spiders && timeout 10m scrapy crawl indiatoday00 &
cd /mnt/data/indiatoday0/indiatoday0/spiders && timeout 10m scrapy crawl indiatoday01 &
cd /mnt/data/indiatoday0/indiatoday0/spiders && timeout 10m scrapy crawl indiatoday02 &
cd /mnt/data/indiatoday0/indiatoday0/spiders && timeout 10m scrapy crawl indiatoday03 &
cd /mnt/data/indiatoday0/indiatoday0/spiders && timeout 10m scrapy crawl indiatoday04 &
cd /mnt/data/indiatoday0/indiatoday0/spiders && timeout 10m scrapy crawl indiatoday05 &
cd /mnt/data/aajtak0/aajtak0/spiders && timeout 10m scrapy crawl aajtak00 &
cd /mnt/data/aajtak0/aajtak0/spiders && timeout 10m scrapy crawl aajtak01 &
cd /mnt/data/aajtak0/aajtak0/spiders && timeout 10m scrapy crawl aajtak02 &
cd /mnt/data/aajtak0/aajtak0/spiders && timeout 10m scrapy crawl aajtak03 &
cd /mnt/data/aajtak0/aajtak0/spiders && timeout 10m scrapy crawl aajtak04 &
cd /mnt/data/aajtak0/aajtak0/spiders && timeout 10m scrapy crawl aajtak05 &
wait
#sleep 10m
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta00.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta01.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta02.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta03.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta04.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday0/indiatoday0/spiders/indiatodaydelta05.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak0/aajtak0/spiders/ajtkdelta00.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak0/aajtak0/spiders/ajtkdelta01.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak0/aajtak0/spiders/ajtkdelta02.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak0/aajtak0/spiders/ajtkdelta03.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak0/aajtak0/spiders/ajtkdelta04.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak0/aajtak0/spiders/ajtkdelta05.json

cd /mnt/data && head -n 1500 /mnt/data/indiatodaydeltaurls0.txt > /mnt/data/itd0.txt && head -n 1500 /mnt/data/ajtkdelta0.txt > /mnt/data/ajtk0.txt 
cd /mnt/data && cat /mnt/data/itd0.txt /mnt/data/ajtk0.txt > /mnt/data/indiatodayurlListv1.txt
sshpass -p TnvGJWfTuz9e6fg6 scp /mnt/data/indiatodayurlListv1.txt root@192.168.106.124:/mnt/data
#cd /mnt/data && split -l 150 --additional-suffix=.txt /mnt/data/indiatodayurlList.txt /mnt/data/indiatodayurlList
#cd /mnt/data && timeout 60m java -jar /mnt/data/categorizerv1.jar &
#cd /mnt/data && timeout 60m java -jar /mnt/data/categorizerv2.jar &
#cd /mnt/data && timeout 60m java -jar /mnt/data/categorizerv3.jar &
#sleep 75m
#cd /mnt/data/indiatoday0/indiatoday0/spiders && mv indiatodaydelta0.json indiatodaydeltabk0.json 
#cd /mnt/data/aajtak0/aajtak0/spiders && mv ajtkdelta0.json ajtkdelta0bk.json
cd /mnt/data/indiatoday0/indiatoday0/spiders && mv indiatodaydelta00.json bk0.json && mv indiatodaydelta01.json bk1.json && mv indiatodaydelta02.json bk2.json && mv indiatodaydelta03.json bk3.json && mv indiatodaydelta04.json bk4.json && mv indiatodaydelta05.json bk5.json
cd /mnt/data/aajtak0/aajtak0/spiders && mv ajtkdelta00.json abk0.json && mv ajtkdelta01.json abk1.json && mv ajtkdelta02.json abk2.json && mv ajtkdelta03.json abk3.json && mv ajtkdelta04.json abk4.json && mv ajtkdelta05.json abk5.json
cd /mnt/data && mv indiatodaydeltaurls0.txt indiatodaydeltaurls0bk.txt && mv ajtkdelta0.txt ajtkdelta0bk.txt && mv indiatodayurlListv1.txt indiatodayurlListv1bk.txt
