package com.spidio.DataLogix.web;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.xml.sax.InputSource;

import com.kohlschutter.boilerpipe.document.TextDocument;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;
import com.kohlschutter.boilerpipe.sax.BoilerpipeSAXInput;
import com.kohlschutter.boilerpipe.sax.HTMLFetcher;
import com.spidio.UserSegmenter.IndexCategoriesData;
import com.spidio.UserSegmenter.IndexCategoriesData1;
import com.spidio.UserSegmenter.IndexCategoriesData2;
import com.spidio.UserSegmenter.ProcessRefurl;
import com.spidio.UserSegmenter.TitleExtractor;
import com.spidio.UserSegmenter.TitleExtractorRegex;
import com.sree.textbytes.jtopia.Configuration;
import com.sree.textbytes.jtopia.TermDocument;
import com.sree.textbytes.jtopia.TermsExtractor;

public class ComputeEngagementTime {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/**
		 * This method returns ESclient instance.
		 *
		 * @return the es client ESClient instance
		 */

		Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true).build();
   
  	  
       Client client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress(
        "172.16.105.231", 9300));


		// Client client = new TransportClient()
		// .addTransportAddress(new InetSocketTransportAddress(
		// "localhost", 9300));

		String keywords = null;
		String description = null;
		String[] searchkeyword = null;
		String[] searchkeyword1 = null;

		// String [] finalsearchkeyword = null;
		String finalsearchkeyword;
		SearchHit[] matchingsegmentrecords = null;
		String category = null;
		String subcategory = null;
		String finalCategory = null;
		String refurl = null;
		String title = null;
		String audienceSegment = null;
		String[] parts = null;
		String[] parts1 = null;
		String keywordcheck = null;
		String title1 = null;
		String title2 = null;
		String id = null;
		TreeMap<String, Integer> categoryCount = new TreeMap<String, Integer>();
		TreeMap<String, Integer> keywordcategoryCount = new TreeMap<String, Integer>();
		TreeMap<String, Integer> aggregatecategoryCount = new TreeMap<String, Integer>();
		TreeMap<String, Integer> subcategoryMap = new TreeMap<String, Integer>();

		Integer count = 0;
		Integer flag = 0;
		Integer count1 = 0;
		// String id;
		Integer count2 = 0;
		Integer count3 = 0;
		Integer categorycount = 0;
		String agegroup = null;
		String gender = null;
		String incomelevel = null;
		String agegroupcalc = null;
		String gendercalc = null;
		String incomelevelcalc = null;
		String channelName = null;
		Map<String, Integer> gendermap = new HashMap<String, Integer>();
		Map<String, Integer> agemap = new HashMap<String, Integer>();
		Map<String, Integer> incomemap = new HashMap<String, Integer>();
		// Data is copied in elasticsearch every 5 minutes, this module scans
		// elasticsearch records for past 5 minutes and classifies them in to
		// categories and corresponding subcategories
		String genderValue = null;

		String incomeValue = null;

		String ageValue = null;

		Integer flag1 = 0;

		Integer flag2 = 0;

		Integer flag3 = 0;

		String fingerprintId = null;
		
		String request_time = null;

		Map<String, String> categorysubcategoryMap = new HashMap<String, String>();

		String[] agegroup1 = null;
	
		
		String rt1 = null;
		
		String rt2 = null;
		
		Timestamp timestamp = null;
		Timestamp timestamp1 = null;
		
		
		/*		
		try {
			System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	*/
		

		// SearchHit[] results = IndexCategoriesData2.searchDocumentContent(
		// client, "enhanceduserdatabeta1",
		// "core2","cookie_id","575962882.967132425.3646610088.120");

		
		  List<String> Id = new ArrayList<String>();
				  
		  List<String> requesttime = new ArrayList<String>();		  
		
		  String startDate = "now-1h";
  		
		  String endDate = "now";
		
		  Map<String, Object> result  = null;
		
		  SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta1")
					.setTypes("core2")
					.setSearchType(SearchType.QUERY_AND_FETCH)
					.setScroll(new TimeValue(600000))
					.setPostFilter(
							FilterBuilders.rangeFilter("request_time")
									.from(startDate).to(endDate)).setSize(100).execute()
									.actionGet();
		  Set<String> cookieset = new HashSet<String>();	  

		// Scroll until no hits are returned

		while (true) {
			// Scroll until no hits are returned

			for (SearchHit hit : response1.getHits().getHits()) {

				try {

					// System.out.println("------------------------------");

					result = hit.getSource();
					fingerprintId = (String) result.get("cookie_id");

				//	fingerprintId = "728088282.2167482368.1716827031";
					
                        if(cookieset.contains(fingerprintId)==false){
						
						cookieset.add(fingerprintId);
					
					
					//String [] fingerprintSegments = fingerprintId.split("\\.");
					//	System.out.println(fingerprintSegments[0]+":"+fingerprintSegments[1]+":"+fingerprintSegments[2]+":"+fingerprintSegments[3]);
					  //  if(fingerprintSegments.length > 1)
						//fingerprintId = fingerprintSegments[0].trim()+"."+fingerprintSegments[1].trim()+"."+fingerprintSegments[2].trim();
					//	fingerprintId = fingerprintId.trim();

					// Store/Site specific Categorisation
                   
					SearchHit[] results = IndexCategoriesData2
							.searchDocumentContent(client, "enhanceduserdatabeta1",
									"core2", "cookie_id", fingerprintId);

					
					
					for (SearchHit hit0 : results) {

						try {
							
							Map<String, Object> result0 = hit0.getSource();
							refurl = (String) result0.get("referrer");

							title = (String) result0.get("page_title");
							id = (String) hit0.getId();
                 
                            request_time = (String) result0.get("request_time"); 							
							
						//	String gender1 = (String)result0.get("gender");
						//	System.out.println("Test index"+gender);
							channelName = (String) result0.get("channel_name");
					
						    Id.add(id);    
						    
						    requesttime.add(request_time);
						
						
						
						} catch (Exception e) {

							e.printStackTrace();
							continue;

						}

						

					}

					for (int i =0; i<requesttime.size(); i++) {

						try {

							Long diffminutes = new Long(0);
							
							if(i != (requesttime.size()-1))
							{
							 rt1 = requesttime.get(i);
							 rt2 = requesttime.get(i+1);
						
							
							try {
							    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
							    Date parsedDate = dateFormat.parse(rt1);
							    timestamp = new java.sql.Timestamp(parsedDate.getTime());
							} catch(Exception e) { //this generic but you can control another types of exception
							    // look the origin of excption 
							}
							
							
							try {
							    SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
							    Date parsedDate1 = dateFormat1.parse(rt2);
							    timestamp1 = new java.sql.Timestamp(parsedDate1.getTime());
							} catch(Exception e) { //this generic but you can control another types of exception
							    // look the origin of excption 
							}
						
							
							diffminutes = compareTwoTimeStamps(timestamp1,timestamp);
							
							
							
							
							}
							else{
								Random rn = new Random();
							    int time = rn.nextInt(2) + 1;
							    diffminutes = (long)time;
								
							}
							
							
							if(diffminutes < 0){
								  
								  diffminutes = (long)1;
								  
							  }
							
							requesttime.set(i, diffminutes.toString());
						    
						
						
						
						} catch (Exception e) {

							e.printStackTrace();
							continue;

						}

						
					}

				
					for (int i =0; i<Id.size(); i++) {
					
					
					if(Id.get(i) != null && Id.get(i).isEmpty()==false){
						
					if(requesttime.get(i) != null && requesttime.get(i).isEmpty()==false  )	
					{
					
						
						if( Long.parseLong(requesttime.get(i)) < 0){
							  
							requesttime.set(i, new Long(1).toString());
						    
							  
						  }
						
						
						
						IndexCategoriesData.updateDocument1(client,
							"enhanceduserdatabeta1", "core2", Id.get(i), "engagementTime",
					         Long.parseLong(requesttime.get(i)));
					
					   System.out.println("Measurement:"+Id.get(i)+":"+requesttime.get(i));
					   
					}
					}	
				}	
					Id.clear();
					requesttime.clear();
                 }
				} catch (Exception e) {

					e.printStackTrace();
					continue;

				}
	
					
					
					
					
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
	
				public static long compareTwoTimeStamps(java.sql.Timestamp currentTime, java.sql.Timestamp oldTime)
				{
				    long milliseconds1 = oldTime.getTime();
				  long milliseconds2 = currentTime.getTime();

				  long diff = milliseconds2 - milliseconds1;
				  long diffSeconds = diff / 1000;
				  long diffMinutes = diff / (60 * 1000);
				  long diffHours = diff / (60 * 60 * 1000);
				  long diffDays = diff / (24 * 60 * 60 * 1000);

				  if(diffMinutes > 30){
						Random rn = new Random();
					    int time = rn.nextInt(5) + 1;
					    diffMinutes = (long)time;
					}
					
				  if(diffMinutes ==0){
						Random rn = new Random();
					    int time = 1;
					    diffMinutes = (long)time;
					}
				  
				  
				  if(diffMinutes < 0){
					  
					  diffMinutes = 1;
					  
				  }
				    return diffMinutes;
				}


	
	
public static String getKeys(Map<String,Integer> map, Integer value)	{
	
	      String key = "";
	      Integer count = 0;
	      
	for (Map.Entry<String, Integer> entry : map.entrySet()) {
		  if (entry.getValue().equals(value)) {
		      if (count ==0)
			  key = entry.getKey();
		      else{
		    	  if(key.contains(entry.getKey())==false)
		    	  key = key+entry.getKey();
		      }
		     count++;
		  }
		}
	
	
	return key;
			
	
}






}