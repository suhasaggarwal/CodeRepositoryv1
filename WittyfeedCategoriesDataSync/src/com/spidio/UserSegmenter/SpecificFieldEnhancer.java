package com.spidio.UserSegmenter;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.spidio.dataModel.DeviceObject;

public class SpecificFieldEnhancer
{
  public static void main(String[] args)
    throws Exception
  {
    Client client = new TransportClient()
      .addTransportAddress(new InetSocketTransportAddress(
      "172.16.101.132", 9300));
    
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
    
   
  
    String startDate = "now-720d";
		
    String endDate = "now";
	

	  
	
	 SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta1")
			.setTypes("core2").setSearchType(SearchType.QUERY_THEN_FETCH)
			.setScroll(new TimeValue(600000))
			.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchQuery("channel_name","cuberoot"),FilterBuilders.rangeFilter("request_time").from(startDate).to(endDate))).setSize(1000).execute()
			.actionGet();
	// Scroll until no hits are returned

    List<String> AuthorNames =  new ArrayList<String>();
    
	while (true) {

	 for (SearchHit hit : response1.getHits().getHits()) {
      		 
      System.out.println("------------------------------");
      Map<String, Object> result = hit.getSource();
      
      BufferedReader br = null;
    	
      String authorName = null;


      String documentId = hit.getId();
      String segment= (String)result.get("audience_segment");
      String subcategory = "";
      if(segment.equals("entertainment"))
         subcategory = "bollywood";
      
      
      
      if(segment.equals("technology"))
          subcategory = "_technology_gadgets";
      
      
      if(segment.equals("news"))
          subcategory = "_news_indianews";
      
      
      if(segment.equals("entertainment"))
          subcategory = "_entertainment_bollywood";
      
      if(segment.equals("sports"))
          subcategory = "_sports_cricket";
      
      
      try {  
        	
        	IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "subcategory",subcategory);
		    System.out.println(segment);
        	
        	
  	 }
	 catch(Exception e)
	 {
		 continue;
	 }
	
	
	 }
		response1 = client.prepareSearchScroll(response1.getScrollId())
				.setScroll(new TimeValue(600000)).execute().actionGet();
		// Break condition: No hits are returned

		
		// System.out.println("Number of scrolls:"+count3)
		
		if (response1.getHits().getHits().length == 0) {
			break;
	 }
	
	 }
	
	} 
	
 
  
}