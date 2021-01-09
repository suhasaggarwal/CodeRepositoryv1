Delta Crawler Code Works in Sync with Segmentation Module and Entity Index is updated with period of 30 minutes.
Crawler is distributed in nature - each crawler crawls concurrently. 
Distributed Aajtak and IndiaToday Spiders.
Memory Consumption of a single scrapy spiders is less, so multiple scrapy spiders can run in multiple instances/processes to scale crawling.
Each spiders in an instance/process crawl concurrently but uses one CPU Core only. Multiple Scrapy Processes are started so that multiple cores of machine are utilised to fullest.
