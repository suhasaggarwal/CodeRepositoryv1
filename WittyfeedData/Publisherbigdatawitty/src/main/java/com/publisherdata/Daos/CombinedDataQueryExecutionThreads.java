package com.publisherdata.Daos;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.nlpcn.es4sql.SearchDao;

import util.GetMiddlewareData;

import com.publisherdata.model.Article;
import com.publisherdata.model.PublisherReport;

import javolution.util.FastMap;

public class CombinedDataQueryExecutionThreads implements Callable<List<PublisherReport>>{

	 public String query=null;
	 public List<PublisherReport> pubreport = new ArrayList();
     
	 public  CombinedDataQueryExecutionThreads(String query,TransportClient client,SearchDao searchDao) {
       this.query = query;
     }

	 public List<PublisherReport> call() {

		  PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream("combineddatathread.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
			System.setOut(out);
		 
		 
		
 	   
 		System.out.println(query);
 		
 		final long startTime1 = System.currentTimeMillis();
 		CSVResult csvResult = null;
		try {
			csvResult = AggregationModule.getCsvResult(false, query);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	    
 		final long endTime1 = System.currentTimeMillis();
 		
 		System.out.println("Total combined count execution time: " + (endTime1 - startTime1) );
 		
 		
 		List<String> headers = csvResult.getHeaders();
 	    List<String> lines = csvResult.getLines();
 	    List<PublisherReport> pubreport = new ArrayList();
 	    
 	 //   //System.out.println(headers);
 	//    System.out.println(lines);
 	    
 	  
 	    
 	  
 	    for (int i = 0; i < lines.size(); i++)
 	    {
 	    	String [] data = new String[2];
 	    	
 	    	 if(query.contains("tag"))
	 	    	{
 	    		 int k = lines.get(i).lastIndexOf(",");
 	    		 if(k!=0 && k!=-1){
 	    		 data[0] = lines.get(i).substring(0, k);
	 	    	 data[1] = lines.get(i).substring(k+1);
 	    		 }
 	    		 
	 	    	}
 	    	 else
 	    		data = ((String)lines.get(i)).split(",");
 	       
 	    	 if(query.contains("author")){
 	    		 
 	    		 if(data.length > 2 )
 	    			 data[1]=data[data.length-1];
 	    	 }
 	    	 
 	    	 
 	    	 PublisherReport obj = new PublisherReport();	
 	       
 	       if(data.length > 1 ){
 	        
 	    	if(query.contains("refcurrentoriginal")){
 	    	  obj.setPublisher_pages(data[0]);}
 	    		//String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=AggregationModule.capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);String tags = article.getTag();List<String> tags1 = Arrays.asList(tags.split("\\s*,\\s*")); obj.setArticleTag(tags1); if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
 	        
 	    	 if(query.contains("authorName"))
 	    	 {
 	    		obj.setArticleAuthor(data[0]);
        		String authorId = AggregationModule.AuthorMap1.get(data[0]);
        		obj.setAuthorId(authorId);
 	    	 }
 	    	
 	    	 
 	    	 if(query.contains("tag"))
  	 	    	{
 	    		obj.setArticleTags(data[0]);
  	 	    	}
 	    	 
 	    	 if(query.contains("modelName")){
 	    		 obj.setMobile_device_model_name(data[0]);
 	    		 
 	    	 }
 	    	 
 	    	 
 	    	 if(query.contains("resolution")){
 	    		 obj.setScreen_properties(data[0]);
 	    		 
 	    	 }
 	    	 
 	    	 
 	    	 
 	    	if(query.contains("postal")){
	    		 obj.setPostalcode(data[0]);
	    		 
	    	 }
 	    	 
 	    	 
 	    	 
 	    	 if(query.contains("gender"))
	 	    	{
	    		obj.setGender(data[0]);
	 	    	}
 	    	 
 	    	
 	    	 if(query.contains("section"))
 	    	   {
    		    obj.setSection(data[0]);
 	    	   }
 	    	 
 	    	 
 	    	 if(query.contains("agegroup"))
	 	    	{
	    		obj.setAge(data[0]);
	 	    	}
 	    	 
 	    	 if(query.contains("incomelevel"))
	 	    	{
	    		obj.setIncomelevel(data[0]);
	 	    	}
 	    	 
 	    	 if(query.contains("device"))
	 	    	{
	    		obj.setDevice_type(data[0]);
	 	    	}
 	    	 	    	 
 	    	
 	    	 if(query.contains("city")){
        		try{
        		
		        obj.setCity(data[0]);
		        
        		}
        		catch(Exception e){
        			continue;
        		}
        		
        		
        		
        		} 
 	    	
 	    	 
 	    	 if(query.contains("state"))
         	{
         	
         
         	obj.setState(data[0]);
         	}
          
 	    	 
 	    	 if(query.contains("country"))
          	{
          	
          
          	obj.setCountry(data[0]);
          	}
 	    	 
 	    	 
 	    	if(query.contains("brandName")){
       		 data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
       		obj.setBrandname(data[0]);
       	}
 	    	 
 	   	
 	    	
 	    	if(query.contains("system_os")){
    		
	                obj.setOs(data[0]);
	      
    	      }
 	    	
 	    	if(query.contains("referrerType")){
 	    		
                obj.setReferrerSource(data[0]);
      
	      }
 	    	
           if(query.contains("sourceUrl")){
 	    		
                obj.setReferrerMasterDomain(data[0]);
      
	      }
 	
 	    	    	
 	    	
 	    	if(query.contains("ISP")){
 	    		
                obj.setISP(data[0]);
      
	      }
 	    	
 	    	if(query.contains("organisation")){
 	    		
                obj.setOrganisation(data[0]);
      
	      }
 	    	 
 	    	 if(query.toLowerCase().contains("count(*)"))
 	    	obj.setCount(data[1]);	
 	      
 	    	if(query.toLowerCase().contains("engagementtime"))
 	 	    	obj.setEngagementTime(data[1]);	
 	 	      
 	    	if(query.toLowerCase().contains("distinct(cookiehash)"))
 	 	    	obj.setVisitorCount(data[1]);	
 	 	      
 	    	if(query.toLowerCase().contains("distinct(refcurrentoriginal)"))
 	 	    	obj.setPosts(data[1]);	
 	    	
 	       
 	       }
 	        if(data[0]!=null)
 	    	pubreport.add(obj);
 	    	
 	       }     
 
	 
	  return pubreport;
	
 	    }
	 }
	 
	
 	    
