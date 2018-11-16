# CodeRepository for Cuberoot

Code includes the following - 

1)Wittyfeed In-house DMP

Files - 

a)DemographicsClassifierFullscanWittyfeed

Demography Enhancement - Has been discussed. 

b)EngagementTimeComputation 

Engagement Time Computation - Solution which scales based on consecutive requests difference. Has been discussed.

c)EntityDataSync

EntityDataSync - This Module fetches Entity data for a url from Entity index and does enhancement of data points corresponding to url.

d)Publisherbigdatawitty

Publisher Dashboard which approximation support for queries which takes long time and trips Circuit Breaker, causes Out of Memory Errors on very large number of records.
This code is universal and will scale on very large number of records as it can be seen on wittyfeed code running at present.

e)enhanceddatabulkprocesswittyfeed

Does enhancement of raw data points - Has been discussed.


2)Proptiger In-house DMP - Done for Demo Purpose with support for local Property based segments.


3)Lenskart  In-house DMP - Demoed before with support for local lenskart segments.


4)Crawling is divided in two stages -

Initial Crawler
A)First scrapy crawls website for full list of urls and puts Entity data corresponding to urls in Entity index.

Dynamic Crawler -
B)Gets new urls from websites sitemap. New urls are added in websites sitemap on a daily basis and thus, new urls are added in Entity index.
Polling for new Urls occur every 2 hours.

Delta Crawler -
C)Generates url from which requests is logged in Elasticsearch.
Url Set is generated twice in a day.
If url has not already been crawled before and is not present in Entity index, it is crawled and its entity data is added in Entity index.




