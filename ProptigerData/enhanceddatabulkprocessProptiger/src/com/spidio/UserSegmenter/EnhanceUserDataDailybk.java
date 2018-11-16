package com.spidio.UserSegmenter;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.spidio.dataModel.DeviceObject;
import com.spidio.dataModel.LocationObject;
import com.spidio.util.GetWurflData;

public class EnhanceUserDataDailybk {

	// Enhances existing data points such as geo-based,device-based,Time of the
	// day patterns and update it in corresponding Elasticsearch Document.
	// These enhanced data points are later used to generate publisher based
	// report codes which gives full profile of publishers Audience.

private static TransportClient client;
	
	public static void setUp()
		    throws Exception
		  {
		    if (client == null)
		    {
		      client = new TransportClient();
		      client.addTransportAddress(getTransportAddress());
		      
		      NodesInfoResponse nodeInfos = (NodesInfoResponse)client.admin().cluster().prepareNodesInfo(new String[0]).get();
		      String clusterName = nodeInfos.getClusterName().value();
		      System.out.println(String.format("Found cluster... cluster name: %s", new Object[] { clusterName }));
		      
		    }
		    System.out.println("Finished the setup process...");
		  }

	private static InetSocketTransportAddress getTransportAddress()
	  {
	    String host = null;
	    String port = null;
	    if (host == null)
	    {
	      host = "172.16.101.132";
	     // System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default 'localhost'");
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
	   char[] characters = s.toCharArray();
	   int rand = (int)(Math.random() * s.length());
	 
	   s = s.replace(s.charAt(rand),'_');
	 
	   return s;
	}
	 
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

	//	Client client = new TransportClient()
			//	.addTransportAddress(new InetSocketTransportAddress(
		  Settings settings = ImmutableSettings.settingsBuilder()
	              .put("cluster.name", "Cluster")
	              .put("client.transport.sniff", true).build();
	 
		  
	    Client client = new TransportClient(settings)
	      .addTransportAddress(new InetSocketTransportAddress(
	      "172.16.104.17", 9300));
		
		String keywords = null;
		String description = null;
		String[] searchkeyword = null;
		String[] finalsearchkeyword = null;
		SearchHit[] matchingsegmentrecords = null;
		String category = null;
		String subcategory = null;
		DeviceObject deviceProperties = null;
		LocationObject locationProperties = null;
		Map<String, Object> result = null;
		// SearchHit[] results =
		// IndexCategoriesData.searchEntireUserData(client,
		// "dmpuserdatabase","core2");

		// System.setOut(new PrintStream(new BufferedOutputStream(new
		// FileOutputStream("log.txt"))));

		// Scans quarterly data of the day, enhances its data points and store
		// it in new index
		String refurl;
		String refcurrent;
		String clickedurl;
	
		//String reforiginal = (String) result.get("reforiginal");
	   // String refcuroriginal = (String) result.get("refcurrentoriginal");
	   // String clickedurloriginal = (String) result.get("clickurloriginal");
		String localstorageId;
		String page_title;
		String documentId;
		String ip;
		String browser;
		String userAgent;
		String channel_name;
		String subcategorydata;
		String localstorageid;
		String fingerprint_id;
		String cookie_id;	
		String session_id;
		String master_id;
		String plugin_data;
		String audience_segment;
		String request_time;
		String system_os;
		String date;
	
		
		
		
		String brand_name = null;
		String browserversion = null;
		String model_name = null;
		String device_os = null;
		String browser1 = null;
		String picture_gif = null;				
		String picture_jpg = null;
		String picture_png = null;
		String gif_animated = null;
		String streaming_video = null;
	    String streaming_3gpp = null;
		String streaming_mp4 = null;
		String streaming_mov = null;
		String colors = null;
		String dual_orientation = null;
		String ux_full_desktop = null;
		String city = null;
		String state = null;
		String org = null;
		String isp = null;
		String gender[] = { "male", "female" };
		String agegroup[] = { "13_17", "18_24", "25_34", "35_44",
				"45_54", "55_64", "65_more" };
		String agegroup1[] = {"25_34","35_44"};
		String gender1 = "male";
		String incomelevel[] = {"high","medium","medium","medium","low"};
		String subcategory_entertainment[] = {
				"_entertainment_bollywood", "_entertainment_music",
				"_entertainment_tv_shows", "_entertainment_games" };
		String subcategory_lifestyle[] = { "_lifestyle_food",
				"_lifestyle_health_fitness", "_lifestyle_fashion" };
		String subcategory_news[] = { "_news_india_news",
				"_news_world_news" };
		String subcategory_education[] = { "_education_mba",
				"_education_mca", "_education_btech",
				"_education_online_courses" };
		String subcategory_technology[] = { "_technology_mobile",
				"_technology_computing", "_technology_apps",
				"_technology_gadgets" };
		String subcategory_business[] = { "_business_india_business",
				"_business_international_business",
				"_business_markets", "_business_mf_simplified",
				"_business_startups" };
		String QuarterValue = null;
		
		
		
		
		    
		SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta5")
				.setTypes("core2").setSearchType(SearchType.QUERY_THEN_FETCH)
				.setScroll(new TimeValue(600000))
				.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchQuery("channel_name","shopclues2"),FilterBuilders.rangeFilter("request_time").from(args[0]).to(args[1]))).setSize(1000).execute()
				.actionGet();

		// Scroll until no hits are returned

		 ExecutorService executor = Executors.newFixedThreadPool(5);
		
		
		try{
			 
			while (true) {
				// Scroll until no hits are returned

				for (SearchHit hit : response1.getHits().getHits()) {

				/*
					Runnable worker = new WorkerThread(hit,client,i);

					executor.execute(worker);
				*/	
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
				 
		        }
		
		
		
		
		
		
		


  }	

}
