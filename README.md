# Knowledge Graph Databases for Creative Enities/User Personas
Please kindly note - Actual Graph Database is very large, this is for Demo Purposes 
https://drive.google.com/drive/folders/1Qtcf90dO-Qj1FZ31fC3n1EHzZgu_YD6g?usp=sharing

# IAB Segments Using Sentence Transformers
Sentence Transformers Scaled on a 16-core machine. Scaling Architecture - 3 processes of Sentence Tranformer are spawned each using 5 workers to increase the throughput of Sentence Transformers. 15 workers - sentence transformer is able to do one batch of 1000 urls in 10 minutes. Splitting workers in multiple processes - increases throughput to 3 batches in 10 minute. Scaling is done without GPU node to keep server costs minimum.

# Publisher Analytic Signals/Segments 
Embedded in User Persona - 
1)Section Engagement Times 
2)IAB Categories Engagement Times 
3)Topic Engagement Times 
4)Total Engagement Time on website 
5)Most Active Quarter of the Day 
6)Total Number of Sessions 
7)Time Since Last Session 
8)Average Session Duration 
9)Loyalty Nature of User - User is New,Returning or Loyal.

These Data points are embedded in an array in custom signals key i.e csection and is supplied as a Key value pair to gampad Ad call


# Databases - ML signals/Segments
Databases (hourly Aajtak Data) (Actual Databases are generated on full cookie pool and updated on hourly data) which are used to generate ML signals and further, ingest in gampad ad call.

Databases Generated -
1)segmentPersonaAffinity

2)creativePersonaAffinity

3)demographicPersonaDatabase

4)emotionPersonaDatabase

5)segment segment affinity database

6)Persona Database

7)Creative Database

8)TopicSegmentAffinityDatabase

Classes Database used for classification

https://drive.google.com/drive/folders/1aSeywBe1cZHWSPHOLSGf4_pyi7eVK6dd?usp=sharing
https://drive.google.com/file/d/1R4GqxLxmK9ijFzSwll1vp5BmayUTASzJ/view?usp=sharing


# Segment Sync Node Jobs tuned with Frequency and Time Decay Settings

0,30 * * * * sh /mnt/data/itwebenpersonaAggregation.sh > /mnt/data/itwebenlogs.txt

#10,40 * * * * sleep 30; cat /mnt/data/Batch* >> /mnt/data/ITWEBENCookiePersonas.txt

12,42 * * * * curl http://localhost:8080/publisherv1/loadEhcache?cacheFile=/mnt/data/ITWEBENCookiePersonas.txt

19,49 * * * * mv /mnt/data/ITWEBENCookiePersonas.txt /mnt/data/logdirectory/ITWEBENCookiePersonas$(date +\%Y\%m\%d\%H\%M\%S).txt

15,45 * * * * sh /mnt/data/ajtkpersonaAggregation.sh > /mnt/data/ajtklogs.txt

#25,55 * * * * sleep 30; cat /mnt/data/Batch* >> /mnt/data/AJTKCookiePersonas.txt

27,57 * * * * curl http://localhost:8080/publisherv1/loadEhcache?cacheFile=/mnt/data/AJTKCookiePersonas.txt

4,34 * * * * mv /mnt/data/AJTKCookiePersonas.txt /mnt/data/logdirectory/AJTKCookiePersonas$(date +\%Y\%m\%d\%H\%M\%S).txt


# Middleware Node Jobs tuned with G1 GC Garbage Collector settings

*/5 * * * * /mnt/data/datamigration.sh > /mnt/data/logdirectory/logdatacopy.txt

0 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,23 * * * sh /mnt/data/crawlscriptfinal.sh > /mnt/data/logdirectory/crawllog.txt

30 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,23 * * * sh /mnt/data/crawlscriptfinalv1.sh > /mnt/data/logdirectory/crawllogv1.txt

1 * * * * cd /mnt/data && timeout 7m java -jar -Xms22g -Xmx22g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:+U
seStringDeduplication -Xloggc:/mnt/data/logdirectory/contentenhancer-gc$(date +\%Y\%m\%d\%H\%M\%S).txt /mnt/data/contentenhancer.jar "now-61m" "now-1m" "AJTK"
 23 > /mnt/data/logdirectory/contentenhancerAJTK.txt

9 * * * * cd /mnt/data && timeout 5m java -jar -Xms22g -Xmx22g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:+U
seStringDeduplication -Xloggc:/mnt/data/logdirectory/contentenhancer-gc$(date +\%Y\%m\%d\%H\%M\%S).txt /mnt/data/contentenhancer.jar "now-69m" "now-9m" "ITWEB
EN" 23 > /mnt/data/logdirectory/contentenhancerITWEBEN.txt

15 * * * * cd /mnt/data && timeout 12m java -jar -Xms22g -Xmx22g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:
+UseStringDeduplication -Xloggc:/mnt/data/logdirectory/entityenhancer-gc$(date +\%Y\%m\%d\%H\%M\%S).txt /mnt/data/entityenhancer.jar "now-80m" "now-10m" "AJTK
" > /mnt/data/logdirectory/entityenhancerAJTK.txt

28 * * * * cd /mnt/data && timeout 3m java -jar -Xms22g -Xmx22g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:+
UseStringDeduplication -Xloggc:/mnt/data/logdirectory/entityenhancer-gc$(date +\%Y\%m\%d\%H\%M\%S).txt /mnt/data/entityenhancer.jar "now-90m" "now-12m" "ITWEB
EN" > /mnt/data/logdirectory/entityenhancerITWEBEN.txt

32 * * * * cd /mnt/data && timeout 7m java -jar -Xms22g -Xmx22g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:+
UseStringDeduplication -Xloggc:/mnt/data/logdirectory/etcompute-gc$(date +\%Y\%m\%d\%H\%M\%S).txt /mnt/data/etcomputeenhancer.jar "now-89m" "now-29m" "AJTK" >
 /mnt/data/logdirectory/etComputeAJTK.txt

37 * * * * cd /mnt/data && timeout 5m java -jar -Xms22g -Xmx22g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:+
UseStringDeduplication -Xloggc:/mnt/data/logdirectory/etcompute-gc$(date +\%Y\%m\%d\%H\%M\%S).txt /mnt/data/etcomputeenhancer.jar "now-97m" "now-37m" "ITWEBEN
" > /mnt/data/logdirectory/etComputeITWEBEN.txt

43 * * * * cd /mnt/data && timeout 7m java -jar -Xms22g -Xmx22g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:+
UseStringDeduplication -Xloggc:/mnt/data/logdirectory/demographyenhancer-gc$(date +\%Y\%m\%d\%H\%M\%S).txt /mnt/data/demographyenhancer.jar "now-103m" "now-43
m" "AJTK" > /mnt/data/logdirectory/demographyAJTK.txt

51 * * * * cd /mnt/data && timeout 5m java -jar -Xms22g -Xmx22g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:+
UseStringDeduplication -Xloggc:/mnt/data/logdirectory/demographyenhancer-gc$(date +\%Y\%m\%d\%H\%M\%S).txt /mnt/data/demographyenhancer.jar "now-111m" "now-51
m" "ITWEBEN" > /mnt/data/logdirectory/demographyITWEBEN.txt

# Ehcache Upload APIs

Every Quarter Upload of IndiaToday and Aajtak Personas in EhCache

curl http://localhost:8080/publisherv1/loadEhcache?cacheFile=/mnt/data/ITWEBENCookiePersonas.txt

curl http://localhost:8080/publisherv1/loadEhcache?cacheFile=/mnt/data/AJTKCookiePersonas.txt



# Ehcache Statistics API
curl  http://localhost:8080/publisherv1/report/v1/getEhcacheStats

# Sample Response from the API

OnHeapTier hits:23170

onHeapTier misses:73190onHeapTier OccupiedByteSize:-1

onHeapTier AllocatedByteSize:-1

onHeapTier Occupancy Fraction:100

onHeapTier Evictions:0

onHeapTier Puts:0

offHeapTier Hits:3415

offHeapTier Misses:69775

offHeapTier OccupiedByteSize:2066739264

offHeapTier AllocatedByteSize:2287599616

offHeapTier Occupancy Fraction:0

offHeapTier Evictions:0

offHeapTier Puts:16288873

Cache hits:26556

Cache misses:69804

Cache Eviction:0

Cache Expirations:0

Hit %:27.559153

Miss %:72.44084

Cache Gets:96360

Cache Puts:16288873


# Ehcache.xml

# Uses - Heap as well as Direct Memory for Storing Cookie Personas

```
<?xml version="1.0" encoding="UTF-8"?>

<ehcache:config
  xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
  xmlns:ehcache='http://www.ehcache.org/v3'
  xsi:schemaLocation="http://www.ehcache.org/v3 http://www.ehcache.org/schema/ehcache-core-3.1.xsd">
 <!-- <ehcache:persistence directory="/mnt/data/cache"/> -->
  <ehcache:cache alias="basicCache">
    <ehcache:key-type>java.lang.String</ehcache:key-type>
    <ehcache:value-type>java.lang.String</ehcache:value-type>
     <ehcache:expiry>
     <ehcache:ttl unit="seconds">864000</ehcache:ttl>
    </ehcache:expiry>
    <ehcache:resources>
      <ehcache:heap unit="entries">100000</ehcache:heap>
      <ehcache:offheap unit="GB">16</ehcache:offheap>
    <!-- <ehcache:disk persistent="true" unit="GB">20</ehcache:disk> -->
    </ehcache:resources>
  </ehcache:cache>
</ehcache:config>
```

# Data Repository for Cuberoot
https://drive.google.com/file/d/1zsKX7qbMZSDNa1dyUrmH1Zcn0hxBmq3h/view?usp=sharing

Details of the Third Party Data Available is present in the presentation uploaded.

# Vector Store 
https://github.com/suhasaggarwal/VectorStore

# vectorstore
Tired of your custom word/sentence vectors taking too much RAM!!

# Scale it using Vector-Store

# Powered by LevelDB

Not excited about having all your RAM eaten up? Still want fast lookup?

# Key Features
Easily initialize custom word/sentence embedding databases on disk.
Lazy-load embeddings instead of reading the entire file on startup.
LRU caching (1024 most recent lookups by default)
Open Vectorstore word embeddings DB

```
>>> from emstore import Emstore
>>> e = Emstore('~/topicDataLevelDB')
>>> e['modi']
[0.27204,
 -0.06203,
 -0.1884,
 0.023225,
 -0.018158,
 0.0067192,
 ...
]
```

# CodeRepository for Cuberoot

Code includes the following - 

1)Wittyfeed In-house DMP

Files - 

a)EngagementTimeComputation 

Engagement Time Computation - Solution which scales by using consecutive requests from cookie difference.


b)DemographicsClassifierFullscanWittyfeed

Demography Enhancement 


c)EntityDataSync

EntityDataSync - This Module fetches Entity data for a url from Entity index and does enhancement of data points corresponding to url.

d)Publisherbigdatawitty

Publisher Dashboard with approximation support for queries which takes long time and trips Circuit Breaker, causes Out of Memory Errors on very large number of records.
This code is universal and will scale on very large number of records as it can be seen on wittyfeed code running at present.

e)enhanceddatabulkprocesswittyfeed

Does enhancement of raw data points


2)Proptiger In-house DMP - Done for Demo Purpose with support for local Property based segments.


3)Lenskart  In-house DMP - Demoed before with support for local lenskart Eyeglasses based segments.


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

Wittyfeed Functional Urls - 

Elasticsearch Index view - 

Enhanced index -
http://cuberootanalytics.dc.cuberoot.co:5602/#/settings/indices/enhanceduserdatabeta1?_g=()&_a=(tab:indexedFields)

Live index - 
http://cuberootanalytics.dc.cuberoot.co:5601/#/discover?_g=()&_a=(columns:!(_source),index:livedmpindex,interval:auto,query:(query_string:(analyze_wildcard:!t,query:'*')),sort:!(request_time,desc))

Publisher Dashboard view -
http://publisherplatform.dc.cuberoot.co/


ReadMe for Modules  - How to Run code on Servers
-------------------------------------------------

DemographyClassifier 

Generate a Runnable jar file using Following java Class having main method.
getDemographicsFullDataorInterval.java

EngagementTimeComputation 

Generate a Runnable jar file using Following java Class having main method.
ComputeEngagementTime.java

EnhanceUserData

Generate a Runnable jar file using Following java Class having main method.
EnhanceUserDataDaily.java

EntityDataSync
Generate a Runnable jar file using Following java Class having main method.
Does entity based enhancement in cookie's data points. 
SpecificFieldEnhancer.java

Channel names and date range can be modified in Java files for in-house DMP set up.

Delta Urls generator for Delta Crawling.
Set custom time range in code using Calendar. Example shown in code.
This code generates delta url file  4 times in a day as set in cron.
Scrapy crawls these urls in delta url file and fill Entity index in ES 
Generate a Runnable jar file using Following java Class having main method.
GetMiddlewareData.java

Publisherbigdatawitty
Main publisherDashboard with BigData and Approximation Support.
Generates a war file using maven build which is deployed in tomcat and APIs will start.
 
Wittyfeed Categories data Sync uses Machine Learning APIs which are set up on Machine Learning server to generate IAB categories corresponding to url and store it in Entity index.
Delta url generator for delta crawling - (incremental according to new urls added on daily basis) is also present in this module. Filenames are self explainatory - DeltaUrls*
 
These jars are then to be put in cron.
Sample cron file is provided in cron folder.
 
 
 
 
 Python/JS scripts
--------------------

 Spider Set up 
 For spiders set up in scrapy please refer to wittyfeed Middleware Server.
 
 Scraping server code is present which crawls website and generates a list of urls.
 
 Data Collection 
 For Wittyfeed Data Collection set up, please refer to wittyfeed Data Collection Server.
 
 
----------------------------------------------------------------------------------------------------


Adserver Support - This involves scaled Adserver with support for Elasticsearch and new Tracker - Response based Tracker for Chatbot/QA module which takes user response corresponding to questions asked from users.

----------------------------------------------------------------------------------------------------------

Semantic Engine is set up in ML Servers and Python code can be obtained from there. 

Machine Learning APIs being used in Entity Index generator modules can be seen here for reference -
http://semantics.dc.cuberoot.co/CuberootSemantics.html

How to use ML APIs - 

Fetch Topics from ML server - 
http://101.53.130.215:8080/SemanticClassifierv2/getTextAnalysis?text=<text>
 
Fetch Categories from ML server - 
http://101.53.130.215/getTextCategory?url=<text>
 

1)IAB based classifier - Python 

2)Topic Miner - java 

3)Recommendation module in Python 

4)Custom Taxonomy based Classifier - Python

5)Emotion Recognition From Facial Expressions - Python

-------------------------------------------------------------------------------------------------------------------------------
//Javascript based scripts 

<script src="https://cuberoottagmanager.dc.cuberoot.co/dcode2/dmpbasedc.js" defer></script>



<script src="https://segmentsync.dc.cuberoot.co/cookiedatav5.js"></script>

    
    
    
    <script type="text/javascript">

        getCookieData(function(cookiedatav1) {
      
        var cubecity;
        var cubemobile;
        var cubetags;
        var cubeinmarket;
        var cubeaffinity;
        var cubeage;
        var cubegender;
        var cubeincomelevel;
        var cubesection;

         cubecity       = JSON.parse(cookiedatav1).city;
         cubemobile     = JSON.parse(cookiedatav1).mobileDevice;
         cubetags       = JSON.parse(cookiedatav1).tags;
         cubeinmarket   = JSON.parse(cookiedatav1).inMarketSegments;
         cubeaffinity   = JSON.parse(cookiedatav1).AffinitySegments;
         cubeage        = JSON.parse(cookiedatav1).age;
         cubegender     = JSON.parse(cookiedatav1).gender;
         cubeincomelevel= JSON.parse(cookiedatav1).incomelevel;
         cubesection    = JSON.parse(cookiedatav1).section;

         window.cubeRootTargetingSlot = [
           [['cinma', cubeinmarket], ['caffin', cubeaffinity],['ccity', cubecity],['cmobile',cubemobile], ['ctags',cubetags],['cgen',cubegender], ['cag', cubeage],['cinc', cubeincomelevel],['csection', cubesection]] ];
       });
    </script>





Servers 
---------------------

Node name : WittyFeedLoadBalancer
IP : 101.53.137.16 / 172.16.105.233
Username : root
Password : P3KvWEe5z8ah

Node name : WittyfeedDataCollector
IP : 101.53.136.167 / 172.16.105.228
Username : root
Password : 9510@23ce

Node name : WittyFeedLiveES
IP : 101.53.137.126 / 172.16.105.229
Username : root
Password : d477@32ab

Node name : WittyFeedMiddlewareNode
IP : 101.53.137.134 / 172.16.105.230
Username : root
Password : d9f5@8032

Node name : WittyFeedEnhancedIndex
IP : 101.53.137.155 / 172.16.105.231
Username : root
Password : fa38@efcb

Node name : WittyFeedInsightsServer
IP : 101.53.137.158 / 172.16.105.232
Username : root
Password : ab07@e6bc

Node name : WittyfeedMetadataServer
IP : 101.53.137.240 / 172.16.105.247
Username : root
Password : 058d@4222

Node name : Classifier-BigData/ Machine Learning Server
IP : 101.53.130.215 / 172.16.103.138
Username : root
Password : 6wAPasO89LyB

Node name : WittyFeedPreparationServer/Web Crawling Server 
IP : 101.53.137.127 / 172.16.104.87
Username : root
Password : 5582@5928

Node name : SegmentSync/Real Time Persona Fetch Server
IP : 101.53.138.39 / 172.16.106.17
Username : root
Password : fbbd@9751

Node name : RecommendationEngine
IP : 101.53.137.61 / 172.16.102.107
Username : root
Password : a6ce@e9b6

Node name : CuberootWebApplication/ISP Server code Hosted.
IP : 205.147.103.82 / 172.16.102.251
Username : root
Password : dzr0w0qmEnd8
