package com.spidio.UserSegmenter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import util.GetMiddlewareData;

import com.publisherdata.Daos.AggregationModule;
import com.spidio.dataModel.DeviceObject;

public class SpecificFieldEnhancer
{
	
public static Set<String> refurlset = new HashSet<String>();
	
  public static void main(String[] args)
    throws Exception
  {
   
	  
	  Settings settings = ImmutableSettings.settingsBuilder()
             .put("cluster.name", "elasticsearch").build();
 
	  
    Client client = new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress(
      "localhost", 9300));
    
    
    String keywords = null;
    String description = null;
    String[] searchkeyword = null;
    String[] finalsearchkeyword = null;
    SearchHit[] matchingsegmentrecords = null;
    String city  = null;
    String isp = null;
    String org = null;
    String mobiledevice  = null;
    String os = null;
    String browser = null;
    String country = null;
    String userAgent = null;
  
    DeviceObject deviceProperties = null;
    

    try {
     //   System.setOut(new PrintStream(new File("TagCleaner1.txt")));
    } catch (Exception e) {
         e.printStackTrace();
    }
    
    /*
    String channel_name = args[0];
    
    String startDate = args[1];
	
  	String endDate = args[2];
  	*/
  	
  	String channel_name = "lenskart";
     
    String startDate = "now-1000d";
 	
   	String endDate = "now";
   	
  	
  	
  /*
  	SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta1")
				.setTypes("core2").setSearchType(SearchType.QUERY_THEN_FETCH)
				.setScroll(new TimeValue(600000))
				.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchQuery("channel_name","North_East_Today"),FilterBuilders.rangeFilter("request_time").from(startDate).to(endDate))).setSize(1000).execute()
				.actionGet();

		// Scroll until no hits are returned
		 ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		 
		 
		 BulkProcessor bulkprocessor = BulkProcessorModule.GetInstance(client);
		 
		 try{
		 
		while (true) {
			// Scroll until no hits are returned

			for (SearchHit hit : response1.getHits().getHits()) {

				Runnable worker = new WorkerThread(bulkprocessor,hit,client,"North_East_Today");

				executor.execute(worker);
				
				// Break condition: No hits are returned
			}
			// System.out.println("Number of scrolls:"+count3);
			response1 = client.prepareSearchScroll(response1.getScrollId())
					.setScroll(new TimeValue(600000)).execute().actionGet();
			
			if (response1.getHits().getHits().length == 0) {
				break;
			}

		}
		
	}		
		finally {
	            
			 executor.shutdown();
			 bulkprocessor.close();
	        }
		 */
  	
  	        SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta1")
			.setTypes("core2").setSearchType(SearchType.QUERY_THEN_FETCH)
			.setScroll(new TimeValue(600000))
			.setQuery(QueryBuilders.matchQuery("channel_name","lenskart")).setSize(1000).execute()
			.actionGet();

	// Scroll until no hits are returned

  	
    BufferedReader br = null;
	
    String authorName = null;
    FileReader fr = null;

    AggregationModule mod = AggregationModule.getInstance();
    try {
		mod.setUp();
	} catch (Exception e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
			
    ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	 
	 
	 BulkProcessor bulkprocessor = BulkProcessorModule.GetInstance(client);
	 
	 try{
		 
	 
	while (true) {
		// Scroll until no hits are returned

		for (SearchHit hit : response1.getHits().getHits()) {

			Runnable worker = new WorkerThread(bulkprocessor,hit,client,channel_name,mod, startDate,endDate);

			executor.execute(worker);
			
			// Break condition: No hits are returned
		}
		// System.out.println("Number of scrolls:"+count3);
		response1 = client.prepareSearchScroll(response1.getScrollId())
				.setScroll(new TimeValue(600000)).execute().actionGet();
		
		if (response1.getHits().getHits().length == 0) {
			break;
		}

	}
	
}		
	finally {
           
		 executor.shutdown();
		 bulkprocessor.close();
       }
	
  }
  
}