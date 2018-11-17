package com.spidio.UserSegmenter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.spidio.dataModel.DeviceObject;

public class SpecificFieldEnhancer1
{
  public static void main(String[] args)
    throws Exception
  {
    Client client = new TransportClient()
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
    
    
    
    
    
    SearchResponse response1 = client.prepareSearch("shopcluesdata")
			.setTypes("core2").setSearchType(SearchType.QUERY_THEN_FETCH)
			.setScroll(new TimeValue(600000))
			.setQuery(QueryBuilders.matchQuery("channel_name","shopclues")).setSize(100).execute()
			.actionGet();

	// Scroll until no hits are returned

    BufferedReader br = null;
	
    String authorName = null;
    FileReader fr = null;

    List<String> productUrls  = new ArrayList<String>();
    
    fr = new FileReader("Shopcluesproductpages.txt");
	br = new BufferedReader(fr);

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
				productUrls.add(sCurrentLine);
			}
    
	
			
  while (true) {

	 for (SearchHit hit : response1.getHits().getHits()) {
      		 
      System.out.println("------------------------------");
      Map<String, Object> result = hit.getSource();
      

      String documentId = hit.getId();
      String url= (String)result.get("sourceUrl");
      Random random = new Random();
      
	  Integer idx = random.nextInt(productUrls.size());
		
	         if(url!=null && !url.isEmpty() && url.contains("socialpost"))
        	 IndexCategoriesData.updateDocument(client, "shopcluesdata", "core2", documentId, "sourceUrl","shopclues.com");
 		    
            
      
      
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
 
  
