# CodeRepository for Cuberoot

Code includes the following - 

1)Wittyfeed In-house DMP

Files - 

a)DemographicsClassifierFullscanWittyfeed

Demography Enhancement - Has been discussed. 

b)EngagementTimeComputation 

Engagement Time Computation - Solution which scales based on consecutive requests from cookie difference. Has been discussed.

c)EntityDataSync

EntityDataSync - This Module fetches Entity data for a url from Entity index and does enhancement of data points corresponding to url.

d)Publisherbigdatawitty

Publisher Dashboard which approximation support for queries which takes long time and trips Circuit Breaker, causes Out of Memory Errors on very large number of records.
This code is universal and will scale on very large number of records as it can be seen on wittyfeed code running at present.

e)enhanceddatabulkprocesswittyfeed

Does enhancement of raw data points - Has been discussed.


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


Delta Urls generator for Delta Crawling.
Set custom time range in code using Calendar. Example shown in code.
This code generates delta url file  4 times in a day as set in cron.
Scrapy crawls these urls in delta url file and fill Entity index in ES 
Generate a Runnable jar file using Following java Class having main method.
GetMiddlewareData.java


Publisherbigdatawitty
Main publisherDashboard with BigData and Approximation Support.
Generates a war file using maven build which is deployed in tomcat and APIs will start.
 

 Spider Set up 
 For spiders set up in scrapy please refer to wittyfeed servers.
 
----------------------------------------------------------------------------------------------------


Adserver Support - This involves scaled Adserver with support for Elasticsearch and new Tracker - Response based Tracker for Chatbot module which takes user response corresponding to questions asked from users.

----------------------------------------------------------------------------------------------------------




Semantic Engine is set up in Servers and code can be obtained from there. 

1)IAB based classifier - Python 

2)Topic Miner - java 

3)Recommendation module in Python 




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




