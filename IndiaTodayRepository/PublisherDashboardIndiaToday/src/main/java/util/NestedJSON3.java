package util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.publisherdata.Daos.AggregationModule;
import com.publisherdata.model.PublisherReport;

public class NestedJSON3 {

	public static String getNestedJSON(ResultSet rs, String queryfield, List<String> groupby) {
		// TODO Auto-generated method stub
		Map<String, Set<HashMap<String,String>>> jobTypes = new HashMap<String, Set<HashMap<String,String>>>();
		String out = "";
		String jobType = "";
	    try {
			while (rs.next()) {
			    jobType = rs.getString(1);
			    Set<HashMap<String,String>> questions = jobTypes.containsKey(jobType) ? jobTypes.get(jobType) : new HashSet<HashMap<String,String>>();
			       HashMap<String,String> individualQuestion = new HashMap<String,String>();
			         individualQuestion.put(groupby.get(0),  rs.getString(2));
			     //    individualQuestion.put(groupby.get(1), rs.getString(groupby.get(1)));
			       questions.add(individualQuestion);
			    jobTypes.put(jobType, questions); 
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    try {
			out = new ObjectMapper().writeValueAsString(jobTypes);
		    System.out.println(out);
	    } catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	      return out;
	
	
	}


	public static List<PublisherReport> getNestedJSONObject(ResultSet rs, String queryfield, List<String> groupby,String filter) {
		// TODO Auto-generated method stub
		Map<PublisherReport, Set<PublisherReport>> jobTypes = new HashMap<PublisherReport, Set<PublisherReport>>();
		Map<String, Set<PublisherReport>> jobTypes1 = new HashMap<String, Set<PublisherReport>>();
		String out = "";
		String jobType = "";
	    String queryfield1;
		String jobType1 = "";
		PublisherReport obj = new PublisherReport();
		int i = 0;
		try {
			while (rs.next()) {
				jobType = rs.getString(1);
				if(i==0)
				obj = new PublisherReport();
				if(i!=0 && jobType.equals(jobType1))
				obj = new PublisherReport();
			    i++;
				jobType1= jobType;
				if(queryfield.equals("gender"))
		        	obj.setGender(rs.getString(1));
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(rs.getString(1));
		        	
		            if(queryfield.equals("state"))
	            	{
	            	
	            	queryfield=queryfield.replace("_", " ");
	          //  	queryfield = capitalizeString(rs.getString(1));
	            	obj.setState(rs.getString(1));
	            	}
	            
	            
	            if(queryfield.equals("country"))
	        	  {
	        	
	            	queryfield=queryfield.replace("_", " ");
	            //	queryfield = capitalizeString(rs.getString(1));
	            	obj.setCountry(rs.getString(1));
	             	}
	        
		            
		            
		            
		            if(queryfield.equals("city")){
		        		try{
		        	//	String locationproperties = citycodeMap.get(rs.getString(1));
				        queryfield=queryfield.replace("_"," ").replace("-"," ");
				     //   queryfield = capitalizeString(rs.getString(1));
				        obj.setCity(rs.getString(1));
				        System.out.println(rs.getString(1));
				      //  obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("audience_segment"))
		             {
		        		//String audienceSegment = audienceSegmentMap.get(rs.getString(1));
		        		//String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(1));
		        	//	if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		//obj.setAudience_segment(audienceSegment);
		        	//	obj.setAudienceSegmentCode(audienceSegmentCode);
		        	//	}
		        	//	else
		        	    obj.setAudience_segment(rs.getString(1));
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			           obj.setReferrerSource(rs.getString(1));
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 queryfield=queryfield.replace("_","-");
		        		 queryfield=queryfield+ " Years";
		        		// if(queryfield.contains("medium")==false)
		        		// obj.setAge(rs.getString(1));
		        	}
		            	
		            	
		        			        		        	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(rs.getString(1));
		     
		        	
		        	if(queryfield.equals("system_os")){
		       /* 		String osproperties = oscodeMap.get(rs.getString(1));
				        queryfield=queryfield.replace("_"," ").replace("-", " ");
				        queryfield= AggregationModule.capitalizeFirstLetter(rs.getString(1));
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);*/
		        	}
		         	
		        	if(queryfield.equals("modelName")){
			     /*     obj.setMobile_device_model_name(rs.getString(1));
			          String[] mobiledeviceproperties = devicecodeMap.get(rs.getString(1)).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]); */
		        	}
		        	
		        	
		        	if(queryfield.equals("brandName")){
		        		// queryfield= AggregationModule.capitalizeFirstLetter(rs.getString(1));
		        		obj.setBrandname(rs.getString(1));
		        	}
		        	

		        //	if(queryfield.equals("refcurrentoriginal"))
		  	     //     {String articleparts[] = queryfield.split("/"); S
			    
		        	// TODO Auto-generated method stub
		    	
		    		
		    	  
			    
			    Set<PublisherReport> questions = jobTypes1.containsKey(jobType) ? jobTypes1.get(jobType) : new LinkedHashSet<PublisherReport>();
                PublisherReport obj1 = new PublisherReport();
			    
			    if(groupby.get(0).equals(queryfield)==false)
      	{
          
      	if(groupby.get(0).equals("device"))
      	obj1.setDevice_type(rs.getString(2));
      	
      	 if(groupby.get(0).equals("state"))
       	{
     /*  	
       	data[0+1]=data[0+1].replace("_", " ");
       	data[0+1] = capitalizeString(rs.getString(2));
       	obj.setState(rs.getString(2)); */
       	}
       
       
       if(groupby.get(0).equals("country"))
   	  {
   	/*
       	data[0+1]=data[0+1].replace("_", " ");
       	data[0+1] = capitalizeString(rs.getString(2));
       	obj.setCountry(rs.getString(2)); */
        	}
      	
      	
      	
      	if(groupby.get(0).equals("city")){
      	/*	try{
      		String locationproperties = citycodeMap.get(rs.getString(2));
		        data[0+1]=data[0+1].replace("_"," ").replace("-"," ");
		        data[0+1]=capitalizeString(rs.getString(2));
		        obj.setCity(rs.getString(2));
		        System.out.println(rs.getString(2));
		        obj.setLocationcode(locationproperties); 
      		}
      		catch(Exception e)
      		{
      			continue;
      		}*/
      		obj1.setCity(rs.getString(2));
      	
      	}
      	if(groupby.get(0).equals("audience_segment"))
           {
     /* 		String audienceSegment = audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setAudience_segment(audienceSegment);
      		obj.setAudienceSegmentCode(audienceSegmentCode);
      		}
      		else
      	    obj.setAudience_segment(rs.getString(2));
      		*/
           
      		obj1.setAudience_segment(rs.getString(2));
           
           }
      	
      	
      	if(groupby.get(0).equals("gender"))
	             obj1.setGender(rs.getString(2));
      	
      	if(groupby.get(0).equals("hour"))
	             obj1.setDate(rs.getString(2));
      	
      	if(groupby.get(0).equals("minute"))
	             obj1.setDate(rs.getString(2));
      	
      	
      	//if(groupby.get(0).equals("gender"))
	           //  obj.setGender(rs.getString(2));
      	
      	
      	if(groupby.get(0).equals("refcurrentoriginal"))
	             obj1.setGender(rs.getString(2));
          	
      	if(groupby.get(0).equals("date"))
	             obj1.setDate(rs.getString(2));
          		            	
      	if(groupby.get(0).equals("subcategory"))
	             {
      	/*	String audienceSegment = audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setSubcategory(audienceSegment);
      		obj.setSubcategorycode(audienceSegmentCode);
      		}
      		else*/
      	    obj1.setSubcategory(rs.getString(2));
	             }
      	
      	if(groupby.get(0).equals("agegroup"))
      	{
      		
      	
      		// data[0+1]=data[0+1].replace("_","-");
      //		 data[0+1]=data[0+1]+ " Years";
     // 		 if(data[0+1].contains("medium")==false)
      		 obj1.setAge(rs.getString(2));
      	
      	  
      	
      	}
          	
          	
      	if(groupby.get(0).equals("incomelevel"))
	          obj1.setIncomelevel(rs.getString(2));
			       
      	   
      	
      	    if(queryfield.equals("audience_segment") && groupby.get(0).equals("subcategory")== false){
      	    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
          	obj1.setCount(rs.getString(3));
           
	            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
              obj1.setVisitorCount(rs.getString(3));
       

	            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
              obj1.setEngagementTime(rs.getString(3));	
      	    }
      	    else{
      	    	Random random = new Random();	
      	        Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
      	        Integer max = (int)Double.parseDouble(rs.getString(3));
      	        Integer randomNumber1 = random.nextInt(max) + 1;
      	        Integer randomNumber2 = random.nextInt(1000 + 1 - 500) + 500;
      	    	
      	    	obj1.setCount(rs.getString(3)); 	
      	        obj1.setExternalWorldCount(randomNumber.toString());	
      	        obj1.setVisitorCount(randomNumber1.toString());
      	        obj1.setThirdpartycount(randomNumber2.toString());
      	        obj1.setAverageTime("1.0");	
      	    	
      	    	
      	    }
      	    
      	    

		questions.add(obj1);
	    
		  
		
		
		jobTypes1.put(jobType,questions);
		//jobTypes.put(obj, questions); 
        
      	}
      }
			
		
		} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	    
		Integer count = 0;
		Integer externalWorldCount = 0;
		Integer visitorCount = 0;
		Integer thirdpartycount = 0;
		
		List<PublisherReport> pubreport1 = new ArrayList<PublisherReport>();
		
		Map<PublisherReport, Set<PublisherReport>> data = new HashMap<PublisherReport,Set<PublisherReport>>();
			
			try {
		
				for (Map.Entry<String, Set<PublisherReport>> entry : jobTypes1.entrySet())
				{
				     PublisherReport obj2 = new PublisherReport();
					if(queryfield.equals("audience_segment"))
		             {
		        		String audienceSegment = AggregationModule.audienceSegmentMap.get(entry.getKey());
		        		String audienceSegmentCode = AggregationModule.audienceSegmentMap2.get(entry.getKey());
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj2.setAudience_segment(audienceSegment);
		        		obj2.setAudienceSegmentCode(audienceSegmentCode);
		        		count =0;
				        externalWorldCount =0;
				        visitorCount = 0; 
		        		thirdpartycount = 0;
		        		}
		        	
		        		Set<PublisherReport> pubreport = jobTypes1.get(entry.getKey());
		        		for (PublisherReport s : pubreport) {
		        			PublisherReport obj3 = new PublisherReport();
		        			String audienceSegment1 = AggregationModule.audienceSegmentMap.get(s.getSubcategory());
			        		String audienceSegmentCode1 = AggregationModule.audienceSegmentMap2.get(s.getSubcategory());
			        		if(audienceSegment1!=null && !audienceSegment1.isEmpty()){
			        		 obj3.setAudience_segment(audienceSegment1);
			        		 obj3.setAudienceSegmentCode(audienceSegmentCode1);
			        		 obj3.setExternalWorldCount(s.getExternalWorldCount());
			        		 obj3.setCount(s.getCount());
			        		 obj3.setVisitorCount(s.getVisitorCount());
			        		 obj3.setThirdpartycount(s.getThirdpartycount());
			        		 obj3.setAverageTime("2.0");
			        		count = count+(int) Double.parseDouble(s.getCount());
			        		externalWorldCount = externalWorldCount+(int) Double.parseDouble(s.getExternalWorldCount());
			        		visitorCount = visitorCount + (int) Double.parseDouble(s.getVisitorCount());
			        		thirdpartycount = thirdpartycount + (int) Double.parseDouble(s.getThirdpartycount());
			        		obj2.getChildren().add(obj3);
			        		
			        		}
			        	
		        		
		        		
		        		}
					     obj2.setExternalWorldCount(externalWorldCount.toString());
		        		 obj2.setCount(count.toString());
		        		 obj2.setVisitorCount(visitorCount.toString());
		        		 obj2.setThirdpartycount(thirdpartycount.toString());
		        		 obj2.setAverageTime("1.0");
				//	System.out.println(entry.getKey() + "/" + entry.getValue());
				
				         pubreport1.add(obj2);
				         
				        
				
				}

				}	
				

			         PublisherReport obj2 = new PublisherReport();
			         obj2.setAudience_segment("Technology & computing");
				     obj2.setExternalWorldCount(externalWorldCount.toString());
	        		 obj2.setCount(count.toString());
	        		 obj2.setVisitorCount(visitorCount.toString());
	        		 obj2.setThirdpartycount(thirdpartycount.toString());
	        		 obj2.setAverageTime("1.0");
				
	        		 
	        		 PublisherReport obj3 = new PublisherReport();
			         obj3.setAudience_segment("Hardware");
				     obj3.setExternalWorldCount(externalWorldCount.toString());
	        		 obj3.setCount(count.toString());
	        		 obj3.setVisitorCount(visitorCount.toString());
	        		 obj3.setThirdpartycount(thirdpartycount.toString());
	        		 obj3.setAverageTime("1.0");
	        		 obj2.getChildren().add(obj3);
	        		 
	        		 PublisherReport obj4 = new PublisherReport();
			         obj4.setAudience_segment("computer");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	        		 obj4.setVisitorCount(visitorCount.toString());
	        		 obj4.setThirdpartycount(thirdpartycount.toString());
	        		 obj4.setAverageTime("2.0");
				
	        		 obj3.getChildren().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setAudience_segment("portable computer");
				     obj5.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5.setCount(count.toString());
	        		 obj5.setVisitorCount(visitorCount.toString());
	        		 obj5.setThirdpartycount(thirdpartycount.toString());
	        		 obj5.setAverageTime("1.0");
	        		
	        		 obj4.getChildren().add(obj5);
	        		 
	        		 
	        		 PublisherReport obj6 = new PublisherReport();
			         obj6.setAudience_segment("laptop");
				     obj6.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6.setCount(count.toString());
	        		 obj6.setVisitorCount(visitorCount.toString());
	        		 obj6.setThirdpartycount(thirdpartycount.toString());
	        		 obj6.setAverageTime("3.0");
	        		 obj5.getChildren().add(obj6);
	        		 
	        		 pubreport1.add(obj2);
	        		 
	        		 
				out = new ObjectMapper().writeValueAsString(pubreport1);
		       System.out.println(out);
	    } catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	      return pubreport1;
	
	
	}


	public static List<PublisherReport> getNestedJSONObjectGender(ResultSet rs, String queryfield, List<String> groupby,String filter) {
		// TODO Auto-generated method stub
		Map<PublisherReport, Set<PublisherReport>> jobTypes = new HashMap<PublisherReport, Set<PublisherReport>>();
		Map<String, Set<PublisherReport>> jobTypes1 = new HashMap<String, Set<PublisherReport>>();
		String out = "";
		String jobType = "";
	    String queryfield1;
		String jobType1 = "";
		PublisherReport obj = new PublisherReport();
		int i = 0;
		try {
			while (rs.next()) {
				jobType = rs.getString(1);
				if(i==0)
				obj = new PublisherReport();
				if(i!=0 && jobType.equals(jobType1))
				obj = new PublisherReport();
			    i++;
				jobType1= jobType;
				if(queryfield.equals("gender"))
		        	obj.setGender(rs.getString(1));
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(rs.getString(1));
		        	
		            if(queryfield.equals("state"))
	            	{
	            	
	            	queryfield=queryfield.replace("_", " ");
	          //  	queryfield = capitalizeString(rs.getString(1));
	            	obj.setState(rs.getString(1));
	            	}
	            
	            
	            if(queryfield.equals("country"))
	        	  {
	        	
	            	queryfield=queryfield.replace("_", " ");
	            //	queryfield = capitalizeString(rs.getString(1));
	            	obj.setCountry(rs.getString(1));
	             	}
	        
		            
		            
		            
		            if(queryfield.equals("city")){
		        		try{
		        	//	String locationproperties = citycodeMap.get(rs.getString(1));
				        queryfield=queryfield.replace("_"," ").replace("-"," ");
				     //   queryfield = capitalizeString(rs.getString(1));
				        obj.setCity(rs.getString(1));
				        System.out.println(rs.getString(1));
				      //  obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("audience_segment"))
		             {
		        		//String audienceSegment = audienceSegmentMap.get(rs.getString(1));
		        		//String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(1));
		        	//	if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		//obj.setAudience_segment(audienceSegment);
		        	//	obj.setAudienceSegmentCode(audienceSegmentCode);
		        	//	}
		        	//	else
		        	    obj.setAudience_segment(rs.getString(1));
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			           obj.setReferrerSource(rs.getString(1));
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 queryfield=queryfield.replace("_","-");
		        		 queryfield=queryfield+ " Years";
		        		// if(queryfield.contains("medium")==false)
		        		// obj.setAge(rs.getString(1));
		        	}
		            	
		            	
		        			        		        	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(rs.getString(1));
		     
		        	
		        	if(queryfield.equals("system_os")){
		       /* 		String osproperties = oscodeMap.get(rs.getString(1));
				        queryfield=queryfield.replace("_"," ").replace("-", " ");
				        queryfield= AggregationModule.capitalizeFirstLetter(rs.getString(1));
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);*/
		        	}
		         	
		        	if(queryfield.equals("modelName")){
			     /*     obj.setMobile_device_model_name(rs.getString(1));
			          String[] mobiledeviceproperties = devicecodeMap.get(rs.getString(1)).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]); */
		        	}
		        	
		        	
		        	if(queryfield.equals("brandName")){
		        		// queryfield= AggregationModule.capitalizeFirstLetter(rs.getString(1));
		        		obj.setBrandname(rs.getString(1));
		        	}
		        	

		        //	if(queryfield.equals("refcurrentoriginal"))
		  	     //     {String articleparts[] = queryfield.split("/"); S
			    
		        	// TODO Auto-generated method stub
		    	
		    		
		    	  
			    
			    Set<PublisherReport> questions = jobTypes1.containsKey(jobType) ? jobTypes1.get(jobType) : new LinkedHashSet<PublisherReport>();
                PublisherReport obj1 = new PublisherReport();
			    
			    if(groupby.get(0).equals(queryfield)==false)
      	{
          
      	if(groupby.get(0).equals("device"))
      	obj1.setDevice_type(rs.getString(2));
      	
      	 if(groupby.get(0).equals("state"))
       	{
     /*  	
       	data[0+1]=data[0+1].replace("_", " ");
       	data[0+1] = capitalizeString(rs.getString(2));
       	obj.setState(rs.getString(2)); */
       	}
       
       
       if(groupby.get(0).equals("country"))
   	  {
   	/*
       	data[0+1]=data[0+1].replace("_", " ");
       	data[0+1] = capitalizeString(rs.getString(2));
       	obj.setCountry(rs.getString(2)); */
        	}
      	
      	
      	
      	if(groupby.get(0).equals("city")){
      	/*	try{
      		String locationproperties = citycodeMap.get(rs.getString(2));
		        data[0+1]=data[0+1].replace("_"," ").replace("-"," ");
		        data[0+1]=capitalizeString(rs.getString(2));
		        obj.setCity(rs.getString(2));
		        System.out.println(rs.getString(2));
		        obj.setLocationcode(locationproperties); 
      		}
      		catch(Exception e)
      		{
      			continue;
      		}*/
      		obj1.setCity(rs.getString(2));
      	
      	}
      	if(groupby.get(0).equals("audience_segment"))
           {
     /* 		String audienceSegment = audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setAudience_segment(audienceSegment);
      		obj.setAudienceSegmentCode(audienceSegmentCode);
      		}
      		else
      	    obj.setAudience_segment(rs.getString(2));
      		*/
           
      		obj1.setAudience_segment(rs.getString(2));
           
           }
      	
      	
      	if(groupby.get(0).equals("gender"))
	             obj1.setGender(rs.getString(2));
      	
      	if(groupby.get(0).equals("hour"))
	             obj1.setDate(rs.getString(2));
      	
      	if(groupby.get(0).equals("minute"))
	             obj1.setDate(rs.getString(2));
      	
      	
      	//if(groupby.get(0).equals("gender"))
	           //  obj.setGender(rs.getString(2));
      	
      	
      	if(groupby.get(0).equals("refcurrentoriginal"))
	             obj1.setGender(rs.getString(2));
          	
      	if(groupby.get(0).equals("date"))
	             obj1.setDate(rs.getString(2));
          		            	
      	if(groupby.get(0).equals("subcategory"))
	             {
      	/*	String audienceSegment = audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setSubcategory(audienceSegment);
      		obj.setSubcategorycode(audienceSegmentCode);
      		}
      		else*/
      	    obj1.setSubcategory(rs.getString(2));
	             }
      	
      	if(groupby.get(0).equals("agegroup"))
      	{
      		
      	
      		// data[0+1]=data[0+1].replace("_","-");
      //		 data[0+1]=data[0+1]+ " Years";
     // 		 if(data[0+1].contains("medium")==false)
      		 obj1.setAge(rs.getString(2));
      	
      	  
      	
      	}
          	
          	
      	if(groupby.get(0).equals("incomelevel"))
	          obj1.setIncomelevel(rs.getString(2));
			       
      	   
      	
      	    if(queryfield.equals("audience_segment") && groupby.get(0).equals("gender")== false){
      	    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
          	obj1.setCount(rs.getString(3));
           
	            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
              obj1.setVisitorCount(rs.getString(3));
       

	            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
              obj1.setEngagementTime(rs.getString(3));	
      	    }
      	    else{
      	    	Random random = new Random();	
      	        Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
      	        Integer max = (int)Double.parseDouble(rs.getString(3));
      	        Integer randomNumber1 = random.nextInt(max) + 1;
      	        Integer randomNumber2 = random.nextInt(1000 + 1 - 500) + 500;
      	    	
      	    	obj1.setCount(rs.getString(3)); 	
      	        obj1.setExternalWorldCount(randomNumber.toString());	
      	        obj1.setVisitorCount(randomNumber1.toString());
      	        obj1.setThirdpartycount(randomNumber2.toString());
      	        obj1.setAverageTime("1.0");	
      	    	
      	    	
      	    }
      	    
      	    

		questions.add(obj1);
	    
		  
		
		
		jobTypes1.put(jobType,questions);
		//jobTypes.put(obj, questions); 
        
      	}
      }
			
		
		} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	    
		Integer count = 0;
		Integer externalWorldCount = 0;
		Integer visitorCount = 0;
		Integer thirdpartycount = 0;
		
		List<PublisherReport> pubreport1 = new ArrayList<PublisherReport>();
		
		Map<PublisherReport, Set<PublisherReport>> data = new HashMap<PublisherReport,Set<PublisherReport>>();
			
			try {
		
				for (Map.Entry<String, Set<PublisherReport>> entry : jobTypes1.entrySet())
				{
				     PublisherReport obj2 = new PublisherReport();
					if(queryfield.equals("audience_segment"))
		             {
		        		String audienceSegment = AggregationModule.audienceSegmentMap.get(entry.getKey());
		        		String audienceSegmentCode = AggregationModule.audienceSegmentMap2.get(entry.getKey());
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj2.setAudience_segment(audienceSegment);
		        		obj2.setAudienceSegmentCode(audienceSegmentCode);
		        		count =0;
				        externalWorldCount =0;
				        visitorCount = 0; 
		        		thirdpartycount = 0;
		        		}
		        	
		        		Set<PublisherReport> pubreport = jobTypes1.get(entry.getKey());
		        		for (PublisherReport s : pubreport) {
		        			PublisherReport obj3 = new PublisherReport();
		        		//	String audienceSegment1 = AggregationModule.audienceSegmentMap.get(s.getSubcategory());
			        	//	String audienceSegmentCode1 = AggregationModule.audienceSegmentMap2.get(s.getSubcategory());
			        		
		        			String gender = s.getGender();
		        			if(gender!=null && !gender.isEmpty()){
			        		 obj3.setGender(gender);
			        		 obj3.setExternalWorldCount(s.getExternalWorldCount());
			        		 obj3.setCount(s.getCount());
			        		 obj3.setVisitorCount(s.getVisitorCount());
			        		 obj3.setThirdpartycount(s.getThirdpartycount());
			        		 obj3.setAverageTime("2.0");
			        		count = count+(int) Double.parseDouble(s.getCount());
			        		externalWorldCount = externalWorldCount+(int) Double.parseDouble(s.getExternalWorldCount());
			        		visitorCount = visitorCount + (int) Double.parseDouble(s.getVisitorCount());
			        		thirdpartycount = thirdpartycount + (int) Double.parseDouble(s.getThirdpartycount());
			        		obj2.getChildren().add(obj3);
			        		
			        		}
			        	
		        		
		        		
		        		}
					     obj2.setExternalWorldCount(externalWorldCount.toString());
		        		 obj2.setCount(count.toString());
		        		 obj2.setVisitorCount(visitorCount.toString());
		        		 obj2.setThirdpartycount(thirdpartycount.toString());
		        		 obj2.setAverageTime("2.0");
				//	System.out.println(entry.getKey() + "/" + entry.getValue());
				
				         pubreport1.add(obj2);
				         
				        
				
				}

				}	
				

			         PublisherReport obj2 = new PublisherReport();
			         obj2.setAudience_segment("Technology & computing");
				     obj2.setExternalWorldCount(externalWorldCount.toString());
	        		 obj2.setCount(count.toString());
	        		 obj2.setVisitorCount(visitorCount.toString());
	        		 obj2.setThirdpartycount(thirdpartycount.toString());
	        		 obj2.setAverageTime("1.0");
				
	        		 
	        		 
	        		 PublisherReport obj4a = new PublisherReport();
			         obj4a.setGender("male");
				     obj4a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4a.setCount(count.toString());
	        		 obj4a.setVisitorCount(visitorCount.toString());
	        		 obj4a.setThirdpartycount(thirdpartycount.toString());
	        		 obj4a.setAverageTime("2.0");
				
	        		 obj2.getChildren().add(obj4a);
	        		 
	        		 
	        		 PublisherReport obj5a = new PublisherReport();
			         obj5a.setGender("female");
				     obj5a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5a.setCount(count.toString());
	        		 obj5a.setVisitorCount(visitorCount.toString());
	        		 obj5a.setThirdpartycount(thirdpartycount.toString());
	        		 obj5a.setAverageTime("1.0");
	        		 
	        		 obj2.getChildren().add(obj5a);
	        		 
	        		 
	        		 
	        		 PublisherReport obj3 = new PublisherReport();
			         obj3.setAudience_segment("Hardware");
				     obj3.setExternalWorldCount(externalWorldCount.toString());
	        		 obj3.setCount(count.toString());
	        		 obj3.setVisitorCount(visitorCount.toString());
	        		 obj3.setThirdpartycount(thirdpartycount.toString());
	        		 obj3.setAverageTime("1.0");
	        		 obj2.getChildren().add(obj3);
	        		 
	        		 PublisherReport obj4 = new PublisherReport();
			         obj4.setGender("male");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	        		 obj4.setVisitorCount(visitorCount.toString());
	        		 obj4.setThirdpartycount(thirdpartycount.toString());
	        		 obj4.setAverageTime("2.0");
				
	        		 obj3.getChildren().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setGender("female");
				     obj5.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5.setCount(count.toString());
	        		 obj5.setVisitorCount(visitorCount.toString());
	        		 obj5.setThirdpartycount(thirdpartycount.toString());
	        		 obj5.setAverageTime("1.0");
	        		
	        		 obj3.getChildren().add(obj5);
	        		 
	        		 pubreport1.add(obj2);
	        		
	        		 
	        		 
				out = new ObjectMapper().writeValueAsString(pubreport1);
		       System.out.println(out);
	    } catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	      return pubreport1;
	
	
	}



	public static List<PublisherReport> getNestedJSONObjectAgeGroup(ResultSet rs, String queryfield, List<String> groupby,String filter) {
		// TODO Auto-generated method stub
		Map<PublisherReport, Set<PublisherReport>> jobTypes = new HashMap<PublisherReport, Set<PublisherReport>>();
		Map<String, Set<PublisherReport>> jobTypes1 = new HashMap<String, Set<PublisherReport>>();
		String out = "";
		String jobType = "";
	    String queryfield1;
		String jobType1 = "";
		PublisherReport obj = new PublisherReport();
		int i = 0;
		try {
			while (rs.next()) {
				jobType = rs.getString(1);
				if(i==0)
				obj = new PublisherReport();
				if(i!=0 && jobType.equals(jobType1))
				obj = new PublisherReport();
			    i++;
				jobType1= jobType;
				if(queryfield.equals("gender"))
		        	obj.setGender(rs.getString(1));
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(rs.getString(1));
		        	
		            if(queryfield.equals("state"))
	            	{
	            	
	            	queryfield=queryfield.replace("_", " ");
	          //  	queryfield = capitalizeString(rs.getString(1));
	            	obj.setState(rs.getString(1));
	            	}
	            
	            
	            if(queryfield.equals("country"))
	        	  {
	        	
	            	queryfield=queryfield.replace("_", " ");
	            //	queryfield = capitalizeString(rs.getString(1));
	            	obj.setCountry(rs.getString(1));
	             	}
	        
		            
		            
		            
		            if(queryfield.equals("city")){
		        		try{
		        	//	String locationproperties = citycodeMap.get(rs.getString(1));
				        queryfield=queryfield.replace("_"," ").replace("-"," ");
				     //   queryfield = capitalizeString(rs.getString(1));
				        obj.setCity(rs.getString(1));
				        System.out.println(rs.getString(1));
				      //  obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("audience_segment"))
		             {
		        		//String audienceSegment = audienceSegmentMap.get(rs.getString(1));
		        		//String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(1));
		        	//	if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		//obj.setAudience_segment(audienceSegment);
		        	//	obj.setAudienceSegmentCode(audienceSegmentCode);
		        	//	}
		        	//	else
		        	    obj.setAudience_segment(rs.getString(1));
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			           obj.setReferrerSource(rs.getString(1));
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 queryfield=queryfield.replace("_","-");
		        		 queryfield=queryfield+ " Years";
		        		// if(queryfield.contains("medium")==false)
		        		// obj.setAge(rs.getString(1));
		        	}
		            	
		            	
		        			        		        	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(rs.getString(1));
		     
		        	
		        	if(queryfield.equals("system_os")){
		       /* 		String osproperties = oscodeMap.get(rs.getString(1));
				        queryfield=queryfield.replace("_"," ").replace("-", " ");
				        queryfield= AggregationModule.capitalizeFirstLetter(rs.getString(1));
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);*/
		        	}
		         	
		        	if(queryfield.equals("modelName")){
			     /*     obj.setMobile_device_model_name(rs.getString(1));
			          String[] mobiledeviceproperties = devicecodeMap.get(rs.getString(1)).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]); */
		        	}
		        	
		        	
		        	if(queryfield.equals("brandName")){
		        		// queryfield= AggregationModule.capitalizeFirstLetter(rs.getString(1));
		        		obj.setBrandname(rs.getString(1));
		        	}
		        	

		        //	if(queryfield.equals("refcurrentoriginal"))
		  	     //     {String articleparts[] = queryfield.split("/"); S
			    
		        	// TODO Auto-generated method stub
		    	
		    		
		    	  
			    
			    Set<PublisherReport> questions = jobTypes1.containsKey(jobType) ? jobTypes1.get(jobType) : new LinkedHashSet<PublisherReport>();
                PublisherReport obj1 = new PublisherReport();
			    
			    if(groupby.get(0).equals(queryfield)==false)
      	{
          
      	if(groupby.get(0).equals("device"))
      	obj1.setDevice_type(rs.getString(2));
      	
      	 if(groupby.get(0).equals("state"))
       	{
     /*  	
       	data[0+1]=data[0+1].replace("_", " ");
       	data[0+1] = capitalizeString(rs.getString(2));
       	obj.setState(rs.getString(2)); */
       	}
       
       
       if(groupby.get(0).equals("country"))
   	  {
   	/*
       	data[0+1]=data[0+1].replace("_", " ");
       	data[0+1] = capitalizeString(rs.getString(2));
       	obj.setCountry(rs.getString(2)); */
        	}
      	
      	
      	
      	if(groupby.get(0).equals("city")){
      	/*	try{
      		String locationproperties = citycodeMap.get(rs.getString(2));
		        data[0+1]=data[0+1].replace("_"," ").replace("-"," ");
		        data[0+1]=capitalizeString(rs.getString(2));
		        obj.setCity(rs.getString(2));
		        System.out.println(rs.getString(2));
		        obj.setLocationcode(locationproperties); 
      		}
      		catch(Exception e)
      		{
      			continue;
      		}*/
      		obj1.setCity(rs.getString(2));
      	
      	}
      	if(groupby.get(0).equals("audience_segment"))
           {
     /* 		String audienceSegment = audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setAudience_segment(audienceSegment);
      		obj.setAudienceSegmentCode(audienceSegmentCode);
      		}
      		else
      	    obj.setAudience_segment(rs.getString(2));
      		*/
           
      		obj1.setAudience_segment(rs.getString(2));
           
           }
      	
      	
      	if(groupby.get(0).equals("gender"))
	             obj1.setGender(rs.getString(2));
      	
      	if(groupby.get(0).equals("hour"))
	             obj1.setDate(rs.getString(2));
      	
      	if(groupby.get(0).equals("minute"))
	             obj1.setDate(rs.getString(2));
      	
      	
      	//if(groupby.get(0).equals("gender"))
	           //  obj.setGender(rs.getString(2));
      	
      	
      	if(groupby.get(0).equals("refcurrentoriginal"))
	             obj1.setGender(rs.getString(2));
          	
      	if(groupby.get(0).equals("date"))
	             obj1.setDate(rs.getString(2));
          		            	
      	if(groupby.get(0).equals("subcategory"))
	             {
      	/*	String audienceSegment = audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setSubcategory(audienceSegment);
      		obj.setSubcategorycode(audienceSegmentCode);
      		}
      		else*/
      	    obj1.setSubcategory(rs.getString(2));
	             }
      	
      	if(groupby.get(0).equals("agegroup"))
      	{
      		
      	
      		// data[0+1]=data[0+1].replace("_","-");
      //		 data[0+1]=data[0+1]+ " Years";
     // 		 if(data[0+1].contains("medium")==false)
      		 obj1.setAge(rs.getString(2));
      	
      	  
      	
      	}
          	
          	
      	if(groupby.get(0).equals("incomelevel"))
	          obj1.setIncomelevel(rs.getString(2));
			       
      	   
      	
      	    if(queryfield.equals("audience_segment") && groupby.get(0).equals("agegroup")== false){
      	    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
          	obj1.setCount(rs.getString(3));
           
	            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
              obj1.setVisitorCount(rs.getString(3));
       

	            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
              obj1.setEngagementTime(rs.getString(3));	
      	    }
      	    else{
      	    	Random random = new Random();	
      	        Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
      	        Integer max = (int)Double.parseDouble(rs.getString(3));
      	        Integer randomNumber1 = random.nextInt(max) + 1;
      	        Integer randomNumber2 = random.nextInt(1000 + 1 - 500) + 500;
      	    	
      	    	obj1.setCount(rs.getString(3)); 	
      	        obj1.setExternalWorldCount(randomNumber.toString());	
      	        obj1.setVisitorCount(randomNumber1.toString());
      	        obj1.setThirdpartycount(randomNumber2.toString());
      	        obj1.setAverageTime("1.0");	
      	    	
      	    	
      	    }
      	    
      	    

		questions.add(obj1);
	    
		  
		
		
		jobTypes1.put(jobType,questions);
		//jobTypes.put(obj, questions); 
        
      	}
      }
			
		
		} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	    
		Integer count = 0;
		Integer externalWorldCount = 0;
		Integer visitorCount = 0;
		Integer thirdpartycount = 0;
		
		List<PublisherReport> pubreport1 = new ArrayList<PublisherReport>();
		
		Map<PublisherReport, Set<PublisherReport>> data = new HashMap<PublisherReport,Set<PublisherReport>>();
			
			try {
		
				for (Map.Entry<String, Set<PublisherReport>> entry : jobTypes1.entrySet())
				{
				     PublisherReport obj2 = new PublisherReport();
					if(queryfield.equals("audience_segment"))
		             {
		        		String audienceSegment = AggregationModule.audienceSegmentMap.get(entry.getKey());
		        		String audienceSegmentCode = AggregationModule.audienceSegmentMap2.get(entry.getKey());
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj2.setAudience_segment(audienceSegment);
		        		obj2.setAudienceSegmentCode(audienceSegmentCode);
		        		count =0;
				        externalWorldCount =0;
				        visitorCount = 0; 
		        		thirdpartycount = 0;
		        		}
		        	
		        		Set<PublisherReport> pubreport = jobTypes1.get(entry.getKey());
		        		for (PublisherReport s : pubreport) {
		        			PublisherReport obj3 = new PublisherReport();
		        		//	String audienceSegment1 = AggregationModule.audienceSegmentMap.get(s.getSubcategory());
			        	//	String audienceSegmentCode1 = AggregationModule.audienceSegmentMap2.get(s.getSubcategory());
			        		
		        			String agegroup = s.getAge();
		        			if(agegroup!=null && !agegroup.isEmpty()){
			        		 obj3.setAge(agegroup);
			        		 obj3.setExternalWorldCount(s.getExternalWorldCount());
			        		 obj3.setCount(s.getCount());
			        		 obj3.setVisitorCount(s.getVisitorCount());
			        		 obj3.setThirdpartycount(s.getThirdpartycount());
			        		 obj3.setAverageTime("2.0");
			        		count = count+(int) Double.parseDouble(s.getCount());
			        		externalWorldCount = externalWorldCount+(int) Double.parseDouble(s.getExternalWorldCount());
			        		visitorCount = visitorCount + (int) Double.parseDouble(s.getVisitorCount());
			        		thirdpartycount = thirdpartycount + (int) Double.parseDouble(s.getThirdpartycount());
			        		obj2.getChildren().add(obj3);
			        		
			        		}
			        	
		        		
		        		
		        		}
					     obj2.setExternalWorldCount(externalWorldCount.toString());
		        		 obj2.setCount(count.toString());
		        		 obj2.setVisitorCount(visitorCount.toString());
		        		 obj2.setThirdpartycount(thirdpartycount.toString());
		        		 obj2.setAverageTime("1.0");
				//	System.out.println(entry.getKey() + "/" + entry.getValue());
				
				         pubreport1.add(obj2);
				         
				        
				
				}

				}	
				

			         PublisherReport obj2 = new PublisherReport();
			         obj2.setAudience_segment("Technology & computing");
				     obj2.setExternalWorldCount(externalWorldCount.toString());
	        		 obj2.setCount(count.toString());
	        		 obj2.setVisitorCount(visitorCount.toString());
	        		 obj2.setThirdpartycount(thirdpartycount.toString());
	        		 obj2.setAverageTime("2.0");
				     
	        		 
	        		 
	        		 
	        		 PublisherReport obj4a = new PublisherReport();
			         obj4a.setAge("25-34 Years");
				     obj4a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4a.setCount(count.toString());
	        		 obj4a.setVisitorCount(visitorCount.toString());
	        		 obj4a.setThirdpartycount(thirdpartycount.toString());
	        		 obj4a.setAverageTime("2.0");
				
	        		 obj2.getChildren().add(obj4a);
	        		 
	        		 
	        		 PublisherReport obj5a = new PublisherReport();
			         obj5a.setAge("35-44 Years");
				     obj5a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5a.setCount(count.toString());
	        		 obj5a.setVisitorCount(visitorCount.toString());
	        		 obj5a.setThirdpartycount(thirdpartycount.toString());
	        		 obj5a.setAverageTime("1.0");
	        		
	        		 obj2.getChildren().add(obj5a);
	        		 
	        		 

	        		 PublisherReport obj6a = new PublisherReport();
			         obj6a.setAge("45-54");
				     obj6a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6a.setCount(count.toString());
	        		 obj6a.setVisitorCount(visitorCount.toString());
	        		 obj6a.setThirdpartycount(thirdpartycount.toString());
	        		 obj6a.setAverageTime("1.0");
				
	        		 obj2.getChildren().add(obj6a);
	        		 
	        		 
	        		 	        		 
	        		 
	        		 PublisherReport obj3 = new PublisherReport();
			         obj3.setAudience_segment("Hardware");
				     obj3.setExternalWorldCount(externalWorldCount.toString());
	        		 obj3.setCount(count.toString());
	        		 obj3.setVisitorCount(visitorCount.toString());
	        		 obj3.setThirdpartycount(thirdpartycount.toString());
	        		 obj3.setAverageTime("1.0");
	        		 obj2.getChildren().add(obj3);
	        		 
	    	        		 
	        		 
	        		 PublisherReport obj4 = new PublisherReport();
			         obj4.setAge("25-34 Years");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	        		 obj4.setVisitorCount(visitorCount.toString());
	        		 obj4.setThirdpartycount(thirdpartycount.toString());
	        		 obj4.setAverageTime("2.0");
				
	        		 obj3.getChildren().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setAge("35-44 Years");
				     obj5.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5.setCount(count.toString());
	        		 obj5.setVisitorCount(visitorCount.toString());
	        		 obj5.setThirdpartycount(thirdpartycount.toString());
	        		 obj5.setAverageTime("1.0");
	        		
	        		 obj3.getChildren().add(obj5);
	        		 
	        		 

	        		 PublisherReport obj6 = new PublisherReport();
			         obj6.setAge("45-54");
				     obj6.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6.setCount(count.toString());
	        		 obj6.setVisitorCount(visitorCount.toString());
	        		 obj6.setThirdpartycount(thirdpartycount.toString());
	        		 obj6.setAverageTime("1.0");
				
	        		 obj3.getChildren().add(obj6);
	        		 
	        		 pubreport1.add(obj2); 
	        		
	        		 
	        		 
				out = new ObjectMapper().writeValueAsString(pubreport1);
		       System.out.println(out);
	    } catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	      return pubreport1;
	
	
	}


	
	public static List<PublisherReport> getNestedJSONObjectIncomeLevel(ResultSet rs, String queryfield, List<String> groupby,String filter) {
		// TODO Auto-generated method stub
		Map<PublisherReport, Set<PublisherReport>> jobTypes = new HashMap<PublisherReport, Set<PublisherReport>>();
		Map<String, Set<PublisherReport>> jobTypes1 = new HashMap<String, Set<PublisherReport>>();
		String out = "";
		String jobType = "";
	    String queryfield1;
		String jobType1 = "";
		PublisherReport obj = new PublisherReport();
		int i = 0;
		try {
			while (rs.next()) {
				jobType = rs.getString(1);
				if(i==0)
				obj = new PublisherReport();
				if(i!=0 && jobType.equals(jobType1))
				obj = new PublisherReport();
			    i++;
				jobType1= jobType;
				if(queryfield.equals("gender"))
		        	obj.setGender(rs.getString(1));
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(rs.getString(1));
		        	
		            if(queryfield.equals("state"))
	            	{
	            	
	            	queryfield=queryfield.replace("_", " ");
	          //  	queryfield = capitalizeString(rs.getString(1));
	            	obj.setState(rs.getString(1));
	            	}
	            
	            
	            if(queryfield.equals("country"))
	        	  {
	        	
	            	queryfield=queryfield.replace("_", " ");
	            //	queryfield = capitalizeString(rs.getString(1));
	            	obj.setCountry(rs.getString(1));
	             	}
	        
		            
		            
		            
		            if(queryfield.equals("city")){
		        		try{
		        	//	String locationproperties = citycodeMap.get(rs.getString(1));
				        queryfield=queryfield.replace("_"," ").replace("-"," ");
				     //   queryfield = capitalizeString(rs.getString(1));
				        obj.setCity(rs.getString(1));
				        System.out.println(rs.getString(1));
				      //  obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("incomelevel"))
		             {
		        		//String audienceSegment = audienceSegmentMap.get(rs.getString(1));
		        		//String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(1));
		        	//	if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		//obj.setAudience_segment(audienceSegment);
		        	//	obj.setAudienceSegmentCode(audienceSegmentCode);
		        	//	}
		        	//	else
		        	    obj.setAudience_segment(rs.getString(1));
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			           obj.setReferrerSource(rs.getString(1));
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 queryfield=queryfield.replace("_","-");
		        		 queryfield=queryfield+ " Years";
		        		// if(queryfield.contains("medium")==false)
		        		// obj.setAge(rs.getString(1));
		        	}
		            	
		            	
		        			        		        	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(rs.getString(1));
		     
		        	
		        	if(queryfield.equals("system_os")){
		       /* 		String osproperties = oscodeMap.get(rs.getString(1));
				        queryfield=queryfield.replace("_"," ").replace("-", " ");
				        queryfield= AggregationModule.capitalizeFirstLetter(rs.getString(1));
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);*/
		        	}
		         	
		        	if(queryfield.equals("modelName")){
			     /*     obj.setMobile_device_model_name(rs.getString(1));
			          String[] mobiledeviceproperties = devicecodeMap.get(rs.getString(1)).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]); */
		        	}
		        	
		        	
		        	if(queryfield.equals("brandName")){
		        		// queryfield= AggregationModule.capitalizeFirstLetter(rs.getString(1));
		        		obj.setBrandname(rs.getString(1));
		        	}
		        	

		        //	if(queryfield.equals("refcurrentoriginal"))
		  	     //     {String articleparts[] = queryfield.split("/"); S
			    
		        	// TODO Auto-generated method stub
		    	
		    		
		    	  
			    
			    Set<PublisherReport> questions = jobTypes1.containsKey(jobType) ? jobTypes1.get(jobType) : new LinkedHashSet<PublisherReport>();
                PublisherReport obj1 = new PublisherReport();
			    
			    if(groupby.get(0).equals(queryfield)==false)
      	{
          
      	if(groupby.get(0).equals("device"))
      	obj1.setDevice_type(rs.getString(2));
      	
      	 if(groupby.get(0).equals("state"))
       	{
     /*  	
       	data[0+1]=data[0+1].replace("_", " ");
       	data[0+1] = capitalizeString(rs.getString(2));
       	obj.setState(rs.getString(2)); */
       	}
       
       
       if(groupby.get(0).equals("country"))
   	  {
   	/*
       	data[0+1]=data[0+1].replace("_", " ");
       	data[0+1] = capitalizeString(rs.getString(2));
       	obj.setCountry(rs.getString(2)); */
        	}
      	
      	
      	
      	if(groupby.get(0).equals("city")){
      	/*	try{
      		String locationproperties = citycodeMap.get(rs.getString(2));
		        data[0+1]=data[0+1].replace("_"," ").replace("-"," ");
		        data[0+1]=capitalizeString(rs.getString(2));
		        obj.setCity(rs.getString(2));
		        System.out.println(rs.getString(2));
		        obj.setLocationcode(locationproperties); 
      		}
      		catch(Exception e)
      		{
      			continue;
      		}*/
      		obj1.setCity(rs.getString(2));
      	
      	}
      	if(groupby.get(0).equals("audience_segment"))
           {
     /* 		String audienceSegment = audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setAudience_segment(audienceSegment);
      		obj.setAudienceSegmentCode(audienceSegmentCode);
      		}
      		else
      	    obj.setAudience_segment(rs.getString(2));
      		*/
           
      		obj1.setAudience_segment(rs.getString(2));
           
           }
      	
      	
      	if(groupby.get(0).equals("gender"))
	             obj1.setGender(rs.getString(2));
      	
      	if(groupby.get(0).equals("hour"))
	             obj1.setDate(rs.getString(2));
      	
      	if(groupby.get(0).equals("minute"))
	             obj1.setDate(rs.getString(2));
      	
      	
      	//if(groupby.get(0).equals("gender"))
	           //  obj.setGender(rs.getString(2));
      	
      	
      	if(groupby.get(0).equals("refcurrentoriginal"))
	             obj1.setGender(rs.getString(2));
          	
      	if(groupby.get(0).equals("date"))
	             obj1.setDate(rs.getString(2));
          		            	
      	if(groupby.get(0).equals("subcategory"))
	             {
      	/*	String audienceSegment = audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setSubcategory(audienceSegment);
      		obj.setSubcategorycode(audienceSegmentCode);
      		}
      		else*/
      	    obj1.setSubcategory(rs.getString(2));
	             }
      	
      	if(groupby.get(0).equals("agegroup"))
      	{
      		
      	
      		// data[0+1]=data[0+1].replace("_","-");
      //		 data[0+1]=data[0+1]+ " Years";
     // 		 if(data[0+1].contains("medium")==false)
      		 obj1.setAge(rs.getString(2));
      	
      	  
      	
      	}
          	
          	
      	if(groupby.get(0).equals("incomelevel"))
	          obj1.setIncomelevel(rs.getString(2));
			       
      	   
      	
      	    if(queryfield.equals("audience_segment") && groupby.get(0).equals("incomelevel")== false){
      	    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
          	obj1.setCount(rs.getString(3));
           
	            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
              obj1.setVisitorCount(rs.getString(3));
       

	            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
              obj1.setEngagementTime(rs.getString(3));	
      	    }
      	    else{
      	    	Random random = new Random();	
      	        Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
      	        Integer max = (int)Double.parseDouble(rs.getString(3));
      	        Integer randomNumber1 = random.nextInt(max) + 1;
      	        Integer randomNumber2 = random.nextInt(1000 + 1 - 500) + 500;
      	    	
      	    	obj1.setCount(rs.getString(3)); 	
      	        obj1.setExternalWorldCount(randomNumber.toString());	
      	        obj1.setVisitorCount(randomNumber1.toString());
      	        obj1.setThirdpartycount(randomNumber2.toString());
      	        obj1.setAverageTime("1.0");	
      	    	
      	    	
      	    }
      	    
      	    

		questions.add(obj1);
	    
		  
		
		
		jobTypes1.put(jobType,questions);
		//jobTypes.put(obj, questions); 
        
      	}
      }
			
		
		} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	    
		Integer count = 0;
		Integer externalWorldCount = 0;
		Integer visitorCount = 0;
		Integer thirdpartycount = 0;
		
		List<PublisherReport> pubreport1 = new ArrayList<PublisherReport>();
		
		Map<PublisherReport, Set<PublisherReport>> data = new HashMap<PublisherReport,Set<PublisherReport>>();
			
			try {
		
				for (Map.Entry<String, Set<PublisherReport>> entry : jobTypes1.entrySet())
				{
				     PublisherReport obj2 = new PublisherReport();
					if(queryfield.equals("audience_segment"))
		             {
		        		String audienceSegment = AggregationModule.audienceSegmentMap.get(entry.getKey());
		        		String audienceSegmentCode = AggregationModule.audienceSegmentMap2.get(entry.getKey());
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj2.setAudience_segment(audienceSegment);
		        		obj2.setAudienceSegmentCode(audienceSegmentCode);
		        		count =0;
				        externalWorldCount =0;
				        visitorCount = 0; 
		        		thirdpartycount = 0;
		        		}
		        	
		        		Set<PublisherReport> pubreport = jobTypes1.get(entry.getKey());
		        		for (PublisherReport s : pubreport) {
		        			PublisherReport obj3 = new PublisherReport();
		        		//	String audienceSegment1 = AggregationModule.audienceSegmentMap.get(s.getSubcategory());
			        	//	String audienceSegmentCode1 = AggregationModule.audienceSegmentMap2.get(s.getSubcategory());
			        		
		        			String incomelevel = s.getIncomelevel();
		        			if(incomelevel !=null && !incomelevel.isEmpty()){
			        		 obj3.setIncomelevel(incomelevel);
			        		 obj3.setExternalWorldCount(s.getExternalWorldCount());
			        		 obj3.setCount(s.getCount());
			        		 obj3.setVisitorCount(s.getVisitorCount());
			        		 obj3.setThirdpartycount(s.getThirdpartycount());
			        		 obj3.setAverageTime("1.0");
			        		count = count+(int) Double.parseDouble(s.getCount());
			        		externalWorldCount = externalWorldCount+(int) Double.parseDouble(s.getExternalWorldCount());
			        		visitorCount = visitorCount + (int) Double.parseDouble(s.getVisitorCount());
			        		thirdpartycount = thirdpartycount + (int) Double.parseDouble(s.getThirdpartycount());
			        		obj2.getChildren().add(obj3);
			        		
			        		}
			        	
		        		
		        		
		        		}
					     obj2.setExternalWorldCount(externalWorldCount.toString());
		        		 obj2.setCount(count.toString());
		        		 obj2.setVisitorCount(visitorCount.toString());
		        		 obj2.setThirdpartycount(thirdpartycount.toString());
		        		 obj2.setAverageTime("2.0");
				//	System.out.println(entry.getKey() + "/" + entry.getValue());
				
				         pubreport1.add(obj2);
				         
				        
				
				}

				}	
				

			         PublisherReport obj2 = new PublisherReport();
			         obj2.setAudience_segment("Technology & computing");
				     obj2.setExternalWorldCount(externalWorldCount.toString());
	        		 obj2.setCount(count.toString());
	        		 obj2.setVisitorCount(visitorCount.toString());
	        		 obj2.setThirdpartycount(thirdpartycount.toString());
	        		 obj2.setAverageTime("1.0");
				
	        		 
	        		 PublisherReport obj4a = new PublisherReport();
			         obj4a.setIncomelevel("medium");
				     obj4a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4a.setCount(count.toString());
	        		 obj4a.setVisitorCount(visitorCount.toString());
	        		 obj4a.setThirdpartycount(thirdpartycount.toString());
	        		 obj4a.setAverageTime("1.0");
				
	        		 obj2.getChildren().add(obj4a);
	        		 
	        		 
	        		 PublisherReport obj5a = new PublisherReport();
			         obj5a.setIncomelevel("low");
				     obj5a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5a.setCount(count.toString());
	        		 obj5a.setVisitorCount(visitorCount.toString());
	        		 obj5a.setThirdpartycount(thirdpartycount.toString());
	        		 obj5a.setAverageTime("2.0");
	        		
	        		 obj2.getChildren().add(obj5a);
	        		 
	        		 PublisherReport obj6a = new PublisherReport();
			         obj6a.setIncomelevel("high");
				     obj6a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6a.setCount(count.toString());
	        		 obj6a.setVisitorCount(visitorCount.toString());
	        		 obj6a.setThirdpartycount(thirdpartycount.toString());
	        		 obj6a.setAverageTime("1.0");
	        		
	        		 obj2.getChildren().add(obj6a);
	        		 
	        		 
	        		 
	        		 PublisherReport obj3 = new PublisherReport();
			         obj3.setAudience_segment("Hardware");
				     obj3.setExternalWorldCount(externalWorldCount.toString());
	        		 obj3.setCount(count.toString());
	        		 obj3.setVisitorCount(visitorCount.toString());
	        		 obj3.setThirdpartycount(thirdpartycount.toString());
	        		 obj3.setAverageTime("1.0");
	        		 obj2.getChildren().add(obj3);
	        		 
	        		 PublisherReport obj4 = new PublisherReport();
			         obj4.setIncomelevel("medium");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	        		 obj4.setVisitorCount(visitorCount.toString());
	        		 obj4.setThirdpartycount(thirdpartycount.toString());
	        		 obj4.setAverageTime("1.0");
				
	        		 obj3.getChildren().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setIncomelevel("low");
				     obj5.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5.setCount(count.toString());
	        		 obj5.setVisitorCount(visitorCount.toString());
	        		 obj5.setThirdpartycount(thirdpartycount.toString());
	        		 obj5.setAverageTime("1.0");
	        		
	        		 obj3.getChildren().add(obj5);
	        		 
	        		 PublisherReport obj6 = new PublisherReport();
			         obj6.setIncomelevel("high");
				     obj6.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6.setCount(count.toString());
	        		 obj6.setVisitorCount(visitorCount.toString());
	        		 obj6.setThirdpartycount(thirdpartycount.toString());
	        		 obj6.setAverageTime("1.0");
	        		
	        		 obj3.getChildren().add(obj6);
	        		
	        		 pubreport1.add(obj2);
	        		 
				out = new ObjectMapper().writeValueAsString(pubreport1);
		       System.out.println(out);
	    } catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	      return pubreport1;
	
	
	}















}
