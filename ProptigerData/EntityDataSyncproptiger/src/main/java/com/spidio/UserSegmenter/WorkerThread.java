package com.spidio.UserSegmenter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import util.GetMiddlewareData;

import com.publisherdata.Daos.AggregationModule;
import com.spidio.UserSegmenter.IndexCategoriesData;
import com.spidio.UserSegmenter.IndexCategoriesData2;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

	public class WorkerThread implements Runnable {
		
		SearchHit hit;
		Client client;
		BulkProcessor bulkProcessor;
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

		

		Map<String, String> categorysubcategoryMap = new HashMap<String, String>();

		String[] agegroup1 = null;
	
		String ecommerceCategory =  "";
		
		String tag= "";
		
		Integer price =0;
		
		Long handsetPrice = new Long(0);
		
		String price_range = "";
		
		String refurlv1 = "";
		
		Map<String, Object> result0;
		Map<String, Object> result;
		Map<String, Object> result1;
		Map<String, Object> result2;
		DateFormat df;
		Date startDatev2;
		Calendar calendar;
		String categoryv1 = null;
		String tags = null;
		String author=null;
		String audienceSegmentv1 = null;
		String startdate = null;
		String enddate = null;
		String newTime;
		AggregationModule mod = null;
		String mappedurl = "";
		
	    public WorkerThread(BulkProcessor bulkprocessor, SearchHit hit, Client client, String channel_name,  AggregationModule mod, String startdate, String enddate ){
	        this.hit = hit;
	        this.client = client;
	        this.bulkProcessor = bulkprocessor;
			this.channelName = channel_name; 
	        this.startdate = startdate;
	        this.enddate = enddate;
	        this.mod = mod;
	    }

	    
	    @Override
	    public void run() {
	    	//Handle the hit...
	    	
	    	String srcIP = "";
	    	String ES_ID = this.hit.getId();
	    	String ES_INDEX = "enhanceduserdatabeta1";
	    	try {

				// System.out.println("------------------------------");

				result = hit.getSource();
				refurl = (String) result.get("refcurrentoriginal");
                refurl = refurl;
				
                if(refurl != null && !refurl.equals("") && SpecificFieldEnhancer.refurlset.contains(refurl)==false){
					
                	SpecificFieldEnhancer.refurlset.add(refurl);
				
				
				//	fingerprintId = "728088282.2167482368.1716827031";
				
				System.out.println(refurl);
				//String [] fingerprintSegments = fingerprintId.split("\\.");
				//	System.out.println(fingerprintSegments[0]+":"+fingerprintSegments[1]+":"+fingerprintSegments[2]+":"+fingerprintSegments[3]);
				  //  if(fingerprintSegments.length > 1)
					//fingerprintId = fingerprintSegments[0].trim()+"."+fingerprintSegments[1].trim()+"."+fingerprintSegments[2].trim();
				//	fingerprintId = fingerprintId.trim();

				// Store/Site specific Categorisation
               
				SearchHit[] results = IndexCategoriesData2
						.searchDocumentContent(client, "enhanceduserdatabeta1",
								"core2", "refcurrentoriginal", refurl,startdate,enddate);

				Entity product = new Entity();
				if(refurl.contains("ia.wittyfeed")){
				mappedurl = refurl.replace("ia.wittyfeed","www.wittyfeed");
			     	product = GetMiddlewareData.getEntityData(channelName, mod, mappedurl);
				}
				else {
		     		product = GetMiddlewareData.getEntityData(channelName, mod, refurl);
				}
				
				
				for (SearchHit hit0 : results) {

					 Map<String, Object> result = hit0.getSource();
				      

				      String documentId = hit0.getId();
				      String url= (String)result.get("refcurrentoriginal");
				      Random random = new Random();
				      
					 // url = "http://www.shopclues.com/opus-black-cotton-formal-geometric-print-western-wear-womens-shirt.html";
				      
				     
				      
				   
				      try {  
				      
				      
				      
				      
				        	
				        	
				        	if(product.getEntity4()!=null && !product.getEntity4().isEmpty()){
				            	author = product.getEntity4().split(";")[0];
				        		IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "authorName",author);
				    		    System.out.println("author" + ":" + author);
				            	}
				        	else{
				        		IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "authorName","");
				    		    System.out.println("author" + ":" + author);
				        		
				        	}
				        	
				        	if(product.getEntity6()!=null && !product.getEntity6().isEmpty()){
				            	tag =product.getEntity6().replace(";",",");
				        		IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "tag",tag);
				    		    System.out.println("tag" + ":" + tag);
				            	}
				      
				        	else{
				        		IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "tag","");
				    		    System.out.println("tag" + ":" + tag);
				        		
				        	}
				      
				        	
				        	if(product.getEntity5()!=null && !product.getEntity5().isEmpty()){
				                
				        		
				        		  df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							     startDatev2 = df.parse(product.getEntity5());
								 calendar = Calendar.getInstance();
								 calendar.setTime(startDatev2);   
							 
							        // Substract 2 hour from the current time
							        calendar.add(Calendar.HOUR, 5);
							 
							        // Add 30 minutes to the calendar time
							        calendar.add(Calendar.MINUTE, 30);
							        newTime = df.format(calendar.getTime());
							        

				               // String[] publishdate1 = product.getEntity5().split("\\s+");
				        		IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId,"publishDatev1",newTime.substring(0, 10));
				             	System.out.println("publishDate" + ":" + product.getEntity5());
				             	
				         	} 
				        	else{
				        	//	IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "publishDatev1","2018-07-21 00:00:01");
				    		//    System.out.println("publishDate" + ":" + "");
				        		
				        	}
				        	
				        	
				        	
				        	
				        	if(product.getEntity12()!=null && !product.getEntity12().isEmpty()){
				                
				    		       
				            	 IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId,"section",product.getEntity12());
				            	 System.out.println("section" + ":" + product.getEntity12());
				        	
				        	} 
				        	else{
				        		IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "section","");
				    		    System.out.println("section" + ":" + "");
				        		
				        	}
				        	
				          	
				        	        	 
				        	if(product.getEntity8()!=null && !product.getEntity8().isEmpty()){
				            	
				        	/*	
				    		    String [] categories = product.getEntity5().split(">");
				            	    		    
				        	    String section = categories[2];
				    		    
				        	    
				        	    String leafCategory = "";
				        	    
				        	    if(categories.length == 3)
				    		    leafCategory = section;
				    		    
				    		    
				    		    
				    		    for(int i=3; i<categories.length;i++){
				    		    	
				    		    	leafCategory = leafCategory+"_"+categories[i];
				    		    }
				    		    
				    		    
				        	  */  
				    		    
				    		  
				    		   categoryv1 = product.getEntity8().replaceAll("\\s+","\\.").replaceAll(",", "\\.").replaceAll(";", "\\.");
				    		   audienceSegmentv1 = product.getEntity10().replaceAll("\\s+","\\.").replaceAll(",", "\\.").replaceAll(";", "\\.");
				    		    
				    		    IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "audience_segment", audienceSegmentv1);
				    		//    leafCategory = leafCategory.substring(1,leafCategory.length());
				    		    System.out.println("audience segment" + ":" +audienceSegmentv1);
				            //	leafCategory = "_"+section+"_"+leafCategory;
				            	System.out.println("subcategory" + ":" + categoryv1);
				    		    IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "subcategory",  categoryv1);
				        	}
				        	else
				        	
				        	
				        	{
				        		IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "audience_segment", "");
					    		//    leafCategory = leafCategory.substring(1,leafCategory.length());
					    		    System.out.println("audience segment" + ":" +audienceSegmentv1);
					            //	leafCategory = "_"+section+"_"+leafCategory;
					            	System.out.println("subcategory" + ":" + categoryv1);
					    		    IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "subcategory", "");
				        		
				        	}
				        	
				      
				      
				      }
					 catch(Exception e)
					 {
						 continue;
					 }
				   	  
				      finally {
	    		  //bulkProcessor.close();
	          }
				
				
				
				}
        
                }
	    	}
	     catch(Exception e){
	    	
	    	 
	     }
	    }
}
