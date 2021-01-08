package com.personaCache;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
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
import com.publisherdata.Daos.AggregationModule;

import util.EhcacheKeyCodeRepository;

public class EnhanceUserDataDailyv1 {

	// Enhances existing data points such as geo-based,device-based,Time of the
	// day patterns and update it in corresponding Elasticsearch Document.
	// These enhanced data points are later used to generate publisher based
	// report codes which gives full profile of publishers Audience.

private static TransportClient client;
static Integer recordCount = 0;

	public static void setUp()
		    throws Exception
		  {
		     if (client == null)
	          {
	           
		        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "personaq").build();
	    	    client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("192.168.106.118", 9300))
	    		                                  .addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300)) 
	    	                                      .addTransportAddress(new InetSocketTransportAddress("192.168.106.120", 9300));
	           
		    //	 Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		    //	 client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		    	 
	          }
		      System.out.println("Finished the setup process...");
		  }

	private static InetSocketTransportAddress getTransportAddress()
	  {
	    String host = null;
	    String port = null;
	    if (host == null)
	    {
	      host = "192.168.106.118";
	     // System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default '172.16.105.231'");
	    }
	    if (port == null)
	    {
	      port = "9300";
	    //  System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
	    }
	    System.out.println(String.format("Connection details: host: %s. port:%s.", new Object[] { host, port }));
	    return new InetSocketTransportAddress(host, Integer.parseInt(port));
	  }
	
	
	
	
	
	
	 public static String changeString(String s)
	{
	   int rand = (int)(Math.random() * s.length());
	 
	   s = s.replace(s.charAt(rand),'a');
	   s= s.replace('_','b');
	   
	   return s;
	}
	 
	
	
	
	
	
	//Uses Executor Framework, available threads equal to number of Processors
	
	public static boolean cachePersonas(String startdate, String enddate, String referrerType, String affinitySegments, String sections, String tags, String channel_name, String file_name) throws Exception {
		// TODO Auto-generated method stub

	//	Client client = new TransportClient()
		
		//lock.lock();
			    
		/*
		SearchResponse response1 = client.prepareSearch("userprofilestore")
				.setTypes("core2").setSearchType(SearchType.QUERY_THEN_FETCH)
				.setScroll(new TimeValue(600000))
				.setQuery(QueryBuilders.matchQuery("channel_name","wittyfeed")).setSize(100000).execute()
				.actionGet();
              */
		//Configures index,channel,request_time for which enhancement is to be done.. 
		//Uses scan and scroll, 1000 records fetched per shard
	
		 setUp();
		 AggregationModule mod = AggregationModule.getInstance();
	     mod.setUp();
		 List<String> cookieList = mod.getCookieList(startdate, enddate, channel_name);
	//	 EhCacheKeyCodeRepository ehcache = EhCacheKeyCodeRepository.getInstance();
		// Scroll until no hits are returned

		 ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()-1);
		 List<Callable<String>> lst = new ArrayList<Callable<String>>();
		 FileWriter writer = null;
			try {
				writer = new FileWriter(file_name,true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		 
		 
		try{
			 
			//while (true) {
				// Scroll until no hits are returned

				for (String cookie : cookieList) {
					System.out.println(cookie);
					lst.add(new WorkerThread(client,cookie, referrerType, affinitySegments, sections, tags, writer));
				}
				//System.out.println("Records:"+response1.getHits());
				 List<Future<String>> maps = executor.invokeAll(lst);
				 	// System.out.println("Number of scrolls:"+count3)

				 lst.clear();
				 maps.clear();
	                
			//	recordCount++;
			//	System.out.println("RecordCount"+recordCount);
		//	done.signal();
	         //	}	
			}	
		     catch (Exception e1) {

			// TODO Auto-generated catch block
			e1.printStackTrace();
		   }
		  finally {
			
			System.out.println("FinalRecordCount"+recordCount);
			writer.close();
			executor.shutdown();
		}
		

       return true;
		
		
  }	

}
