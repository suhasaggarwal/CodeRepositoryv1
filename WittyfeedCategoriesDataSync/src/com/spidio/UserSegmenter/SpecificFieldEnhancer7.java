package com.spidio.UserSegmenter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;


import com.spidio.dataModel.DeviceObject;

//Calls Machine Learning Server API to derive Categories for Article Url
public class SpecificFieldEnhancer7
{
	
	public static List<String> audienceSegmentv1 = new ArrayList<String>();	
	
  public static void main(String[] args)
    throws Exception
  {
	 Settings settings = ImmutableSettings.settingsBuilder()
              .put("cluster.name", "elasticsearch")
              .put("client.transport.sniff", true).build();
 
	  
    Client client = new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress(
      "localhost", 9300));

	    try {
	        System.setOut(new PrintStream(new File("CategoriesDerivewittyf.txt")));
	    } catch (Exception e) {
	         e.printStackTrace();
	    }
	  PrintWriter writer = new PrintWriter("CategoriesDerivationwittyfv1.txt", "UTF-8");
	  
   
	    Map<String,String> categoryMap = new HashMap<String,String>();
		BufferedReader br = null;
		FileReader fr = null;
      String key = null;
      String value = null;
		
		try {

			//br = new BufferedReader(new FileReader(FILENAME));
			fr = new FileReader("categories.txt");
			br = new BufferedReader(fr);

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
		
				key = sCurrentLine.replace("_"," ").trim();
				value = "_"+sCurrentLine;
				categoryMap.put(key,value);
			}

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}

		}

	
		
		
		BufferedReader br1 = null;
		FileReader fr1 = null;
    String key1 = null;
    String value1 = null;
		
		
			//br = new BufferedReader(new FileReader(FILENAME));
			try {
				fr1 = new FileReader("rootCategories.txt");
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			br1 = new BufferedReader(fr1);

			String sCurrentLine;

			try {
				while ((sCurrentLine = br1.readLine()) != null) {

					key = sCurrentLine.trim();
					audienceSegmentv1.add(key);

             }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			br1.close();  	
//Reads Url List from file and supply Urls to categoriser API
//Split Urls about ?			
			List<String> ArticleUrls = new ArrayList<String>();    
			Scanner s = new Scanner(new File("wittyfeedurl.txt"));
			
			while (s.hasNext()){
			   ArticleUrls.add(s.next().trim().split("\\?")[0]);
			}
			s.close(); 
     	
     String url = null;
     for( int i=0;i<ArticleUrls.size();i++)  { 	
    
     try {    	 
    	 url = ArticleUrls.get(i);
       
      String title3 = url;
		//title3 = "journey to mysterious island";
		
	    //title3 = "http://www.google.com";
	    HttpURLConnection connection = null;
     
		title3 = URLEncoder.encode(title3, "UTF-8");
		
	    //title3 = title3.replaceAll(" ", "%20");
		String url2 = "http://101.53.130.215/getTextCategory?url="+title3;

      
      
      URL url1 = new URL(url2);
      

      //make connection
      URLConnection urlc = url1.openConnection();

      //use post mode
      urlc.setDoOutput(true);
      urlc.setAllowUserInteraction(false);

      //send query
      
      //get result
      BufferedReader br2 = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
      String l = null;
      String l1 = null;
      while ((l=br2.readLine())!=null) {
         // System.out.println(l);
          l1=l;
      }
      br2.close();
      
      if(l1 != null){
      
      String categoryv1 = l1.replace("\"", "").trim(); 
      
      categoryv1 = categoryMap.get(categoryv1);
      
      String audienceSegmentv2  = getAudienceSegment(categoryv1);
     
      System.out.println(url+";"+audienceSegmentv2+";"+categoryv1);
      
	  writer.println(url+";"+audienceSegmentv2+";"+categoryv1);
	
	  if(categoryv1 == null){
          
      	
          categoryv1= "";
           
          
         }
     
 //Process Category Data points    
     if(categoryv1 != null){
     	
     	categoryv1 = categoryv1.replace(",",".").replace("   "," ").replace("  ", " ");
        
     	categoryv1 = categoryv1.replace(" ",".").trim();
         categoryv1 = categoryv1.replace("..",".");
     }
     
     System.out.println(categoryv1);
     
    
     if(audienceSegmentv2 == null){
         
     	
     	audienceSegmentv2= "";
           
          
         }
     
     
     if(audienceSegmentv2 != null){
     	
     	audienceSegmentv2= audienceSegmentv2.replace(",",".").replace("   "," ").replace("  ", " ");
     	audienceSegmentv2 = audienceSegmentv2.replace(" ",".").trim();
     	audienceSegmentv2 = audienceSegmentv2.replace("..",".");
     }
     
     
     
     
     System.out.println(audienceSegmentv2);
   
     SearchHit[] results = IndexCategoriesData2
				.searchDocumentContent(client, "entity",
						"core2", "Entity1", url);
//Index categories in ES
     for (SearchHit hit0 : results) {
     
     IndexCategoriesData.updateDocument(client, "entity", "core2",hit0.getId() , "Entity8", categoryv1);
     System.out.println(hit0.getId() + ":" + categoryv1);
     IndexCategoriesData.updateDocument(client, "entity", "core2", hit0.getId(), "Entity10", audienceSegmentv2);
     System.out.println(hit0.getId() + ":" + audienceSegmentv1);

     }
	  
	  
      }

     }
     catch(Exception e){
    	 
    	 continue;
     }
            
      
      
     
     }

     writer.close();
	} 
	
  public static String getAudienceSegment(String segment){
		
	    for(int k=0; k<audienceSegmentv1.size();k++){
	    	
	    	if(segment.contains(audienceSegmentv1.get(k))){
	    		return audienceSegmentv1.get(k);
	    	}
	    	
	    }
	  
	    return "";
			
	  
}
  
}