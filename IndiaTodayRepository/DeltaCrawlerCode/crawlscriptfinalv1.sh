#!/bin/bash
timeout 3m java -jar /mnt/data/deltaurlgeneratorv2.jar itweben /mnt/data/indiatodaydeltaurls1.txt auto,movies,india,education,coronavirus,technology,sports,lifestyle,information,business,television,science,trending,world,news,story 300 3000
timeout 6m java -jar /mnt/data/deltaurlgeneratorv2.jar ajtk /mnt/data/ajtkdelta1.txt auto,india,entertainment,trending,lifestyle,youtube,crime,religion,business,coronavirus,sports,world,technology,elections,education,tez,news,story 300 3000
#cd /mnt/data/indiatoday1/indiatoday1/spiders && scrapy crawl indiatoday1 
#cd /mnt/data/aajtak1/aajtak1/spiders && scrapy crawl aajtak1 
#sleep 10m
#curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta1.json
#curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak1/aajtak1/spiders/ajtkdelta1.json
cd /mnt/data && split -n l/6 -d /mnt/data/indiatodaydeltaurls1.txt indiatodaydeltaurls1
cd /mnt/data && split -n l/6 -d /mnt/data/ajtkdelta1.txt ajtkdelta1
PATH=$PATH:/usr/local/bin
export PATH

cd /mnt/data/indiatoday1/indiatoday1/spiders && rm -f /mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta10.json
cd /mnt/data/indiatoday1/indiatoday1/spiders && rm -f /mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta11.json
cd /mnt/data/indiatoday1/indiatoday1/spiders && rm -f /mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta12.json
cd /mnt/data/indiatoday1/indiatoday1/spiders && rm -f /mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta13.json
cd /mnt/data/indiatoday1/indiatoday1/spiders && rm -f /mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta14.json
cd /mnt/data/indiatoday1/indiatoday1/spiders && rm -f /mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta15.json


cd /mnt/data/aajtak1/aajtak1/spiders && rm -f /mnt/data/aajtak1/aajtak1/spiders/ajtkdelta10.json
cd /mnt/data/aajtak1/aajtak1/spiders && rm -f /mnt/data/aajtak1/aajtak1/spiders/ajtkdelta11.json
cd /mnt/data/aajtak1/aajtak1/spiders && rm -f /mnt/data/aajtak1/aajtak1/spiders/ajtkdelta12.json
cd /mnt/data/aajtak1/aajtak1/spiders && rm -f /mnt/data/aajtak1/aajtak1/spiders/ajtkdelta13.json
cd /mnt/data/aajtak1/aajtak1/spiders && rm -f /mnt/data/aajtak1/aajtak1/spiders/ajtkdelta14.json
cd /mnt/data/aajtak1/aajtak1/spiders && rm -f /mnt/data/aajtak1/aajtak1/spiders/ajtkdelta15.json


cd /mnt/data/indiatoday1/indiatoday1/spiders && timeout 10m scrapy crawl indiatoday10 &
cd /mnt/data/indiatoday1/indiatoday1/spiders && tiemout 10m scrapy crawl indiatoday11 &
cd /mnt/data/indiatoday1/indiatoday1/spiders && timeout 10m scrapy crawl indiatoday12 &
cd /mnt/data/indiatoday1/indiatoday1/spiders && timeout 10m scrapy crawl indiatoday13 &
cd /mnt/data/indiatoday1/indiatoday1/spiders && timeout 10m scrapy crawl indiatoday14 &
cd /mnt/data/indiatoday1/indiatoday1/spiders && timeout 10m scrapy crawl indiatoday15 &
cd /mnt/data/aajtak1/aajtak1/spiders && timeout 10m scrapy crawl aajtak10 &
cd /mnt/data/aajtak1/aajtak1/spiders && timeout 10m scrapy crawl aajtak11 &
cd /mnt/data/aajtak1/aajtak1/spiders && timeout 10m scrapy crawl aajtak12 &
cd /mnt/data/aajtak1/aajtak1/spiders && timeout 10m scrapy crawl aajtak13 &
cd /mnt/data/aajtak1/aajtak1/spiders && timeout 10m scrapy crawl aajtak14 &
cd /mnt/data/aajtak1/aajtak1/spiders && timeout 10m scrapy crawl aajtak15 &
wait
#sleep 10m
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta10.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta11.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta12.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta13.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta14.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/indiatoday1/indiatoday1/spiders/indiatodaydelta15.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak1/aajtak1/spiders/ajtkdelta10.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak1/aajtak1/spiders/ajtkdelta11.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak1/aajtak1/spiders/ajtkdelta12.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak1/aajtak1/spiders/ajtkdelta13.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak1/aajtak1/spiders/ajtkdelta14.json
curl -XPOST 192.168.106.118:9200/entity/core2/_bulk --data-binary @/mnt/data/aajtak1/aajtak1/spiders/ajtkdelta15.json
cd /mnt/data && head -n 1500 /mnt/data/indiatodaydeltaurls1.txt > /mnt/data/itd1.txt && head -n 1500 /mnt/data/ajtkdelta1.txt > /mnt/data/ajtk1.txt
cd /mnt/data && cat /mnt/data/itd1.txt /mnt/data/ajtk1.txt > /mnt/data/indiatodayurlList.txt
sshpass -p TnvGJWfTuz9e6fg6 scp /mnt/data/indiatodayurlList.txt root@192.168.106.124:/mnt/data
#cd /mnt/data && split -l 150 --additional-suffix=.txt /mnt/data/indiatodayurlList.txt /mnt/data/indiatodayurlList
#cd /mnt/data && timeout 60m java -jar /mnt/data/categorizerv1.jar &
#cd /mnt/data && timeout 60m java -jar /mnt/data/categorizerv2.jar &
#cd /mnt/data && timeout 60m java -jar /mnt/data/categorizerv3.jar &
#sleep 75m
#cd /mnt/data/indiatoday1/indiatoday1/spiders && mv indiatodaydelta1.json indiatodaydelta1bk.json 
#cd /mnt/data/aajtak1/aajtak1/spiders && mv ajtkdelta1.json ajtkdelta1bk.json
cd /mnt/data/indiatoday1/indiatoday1/spiders && mv indiatodaydelta10.json bk0.json && mv indiatodaydelta11.json bk1.json && mv indiatodaydelta12.json bk2.json
 && mv indiatodaydelta13.json bk3.json && mv indiatodaydelta14.json bk4.json && mv indiatodaydelta15.json bk5.json
cd /mnt/data/aajtak1/aajtak1/spiders && mv ajtkdelta10.json abk0.json && mv ajtkdelta11.json abk1.json && mv ajtkdelta12.json abk2.json && mv ajtkdelta13.json abk3.json && mv ajtkdelta14.json abk4.json && mv ajtkdelta15.json abk5.json
cd /mnt/data && mv indiatodaydeltaurls1.txt indiatodaydeltaurls1bk.txt && mv ajtkdelta1.txt ajtkdelta1bk.txt && mv indiatodayurlList.txt indiatodayurlListbk.txt
