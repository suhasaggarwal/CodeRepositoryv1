package util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

public class NestedJSON8 {

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
				     obj2.setExternalWorldCount("167");
	        		 obj2.setCount("56");
	        		 obj2.setVisitorCount("105");
	        		 obj2.setThirdpartycount("102");
	        		 obj2.setAverageTime("1.0");
				
	        		 
	        		 PublisherReport obj3 = new PublisherReport();
			         obj3.setAudience_segment("Hardware");
				     obj3.setExternalWorldCount("167");
	        		 obj3.setCount("56");
	        		 obj3.setVisitorCount("105");
	        		 obj3.setThirdpartycount("102");
	        		 obj3.setAverageTime("1.0");
	        		 obj2.getChildren().add(obj3);
	        		 
	        		 PublisherReport obj4 = new PublisherReport();
			         obj4.setAudience_segment("Computer");
				     obj4.setExternalWorldCount("167");
	        		 obj4.setCount("56");
	        		 obj4.setVisitorCount("105");
	        		 obj4.setThirdpartycount("102");
	        		 obj4.setAverageTime("2.0");
				
	        		 obj3.getChildren().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setAudience_segment("Portable computer");
				     obj5.setExternalWorldCount("167");
	        		 obj5.setCount("56");
	        		 obj5.setVisitorCount("105");
	        		 obj5.setThirdpartycount("102");
	        		 obj5.setAverageTime("1.0");
	        		
	        		 obj4.getChildren().add(obj5);
	        		 
	        		 
	        		 PublisherReport obj6 = new PublisherReport();
			         obj6.setAudience_segment("Laptop");
				     obj6.setExternalWorldCount("167");
	        		 obj6.setCount("56");
	        		 obj6.setVisitorCount("105");
	        		 obj6.setThirdpartycount("102");
	        		 obj6.setAverageTime("3.0");
	        		 obj5.getChildren().add(obj6);
	        		 
	        		 pubreport1.add(obj2);

	     			Collections.sort(pubreport1, new Comparator<PublisherReport>() {
	     				
	     				@Override
	     		        public int compare(PublisherReport o1, PublisherReport o2) {
	     					return Integer.parseInt(o1.getCount()) > Integer.parseInt(o2.getCount()) ? -1 : (Integer.parseInt(o1.getCount()) < Integer.parseInt(o2.getCount())) ? 1 : 0;
	     		        }
	     		    });	
	        		 
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
	             obj1.setGender(AggregationModule.capitalizeString(rs.getString(2)));
      	
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
      	        Integer randomNumber1 = 90510 + max;
      	        Integer randomNumber2 = random.nextInt(1000 + 1 - 500) + 500;
      	    	
      	    	obj1.setCount(rs.getString(3)); 	
      	        obj1.setExternalWorldCount(randomNumber1.toString());	
      	      //  obj1.setVisitorCount(randomNumber1.toString());
      	     //   obj1.setThirdpartycount(randomNumber2.toString());
      	      //  obj1.setAverageTime("1.0");	
      	    	
      	    	
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
			        		 Integer eWC  = (int)Double.parseDouble(s.getExternalWorldCount());
			        		 obj3.setExternalWorldCount(eWC.toString());
			        		 Integer count1 = (int)Double.parseDouble(s.getCount());
			        		 obj3.setCount(count1.toString());
			        	//	 obj3.setVisitorCount(s.getVisitorCount());
			        	//	 obj3.setThirdpartycount(s.getThirdpartycount());
			        	//	 obj3.setAverageTime("2.0");
			        		count = count+(int) Double.parseDouble(s.getCount());
			        		externalWorldCount = externalWorldCount+(int) Double.parseDouble(s.getExternalWorldCount());
			        	//	visitorCount = visitorCount + (int) Double.parseDouble(s.getVisitorCount());
			        	//	thirdpartycount = thirdpartycount + (int) Double.parseDouble(s.getThirdpartycount());
			        		obj2.getGenderlist().add(obj3);
			        		
			        		}
			        	
		        		
		        		
		        		}
					     obj2.setExternalWorldCount(externalWorldCount.toString());
		        		 obj2.setCount(count.toString());
		        	//	 obj2.setVisitorCount(visitorCount.toString());
		        	//	 obj2.setThirdpartycount(thirdpartycount.toString());
		        	//	 obj2.setAverageTime("2.0");
				//	System.out.println(entry.getKey() + "/" + entry.getValue());
				
				         pubreport1.add(obj2);
				         
				        
				
				}

				}	
				

				   Integer total = 0;
		           Double share = 0.0;
		           Double scaledshare = 0.0;
		           Double scaledshare1 = 0.0;
		           Integer externalWorldtotal = 0;
		           Double externalWorldShare = 0.0;
		           Double scaledexternalWorldShare = 0.0;
		           Integer maxTotal = 0;
		           Integer maxexternalWorldTotal = 0;
		           Integer total1= 0; 
    	           Integer externalworldtotal1 = 0;   
    	           
    	           for(i=0;i<pubreport1.size();i++){
    	        	   
    	        	   if(pubreport1.get(i).getAudience_segment() == null || pubreport1.get(i).getAudience_segment().isEmpty())
        	    	    {
        	    	    	pubreport1.remove(i);
        	    	    }
    	        	   
    	        	   
    	           }
    	           
    	           
    	           
    	           
    	           
    	           
    	           
		           for(i=0;i<pubreport1.size();i++){
		         	  
		        	  
		         		  total=total+Integer.parseInt(pubreport1.get(i).getCount());
		         		 if(Integer.parseInt(pubreport1.get(i).getCount())> maxTotal)
	         	    	    {
	         	    	    	maxTotal = Integer.parseInt(pubreport1.get(i).getCount());
	         	    	    }
		        	    
		        	   
		                  
		         	         externalWorldtotal =  externalWorldtotal + Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
		           
		         	        if(Integer.parseInt(pubreport1.get(i).getExternalWorldCount())> maxexternalWorldTotal)
	         	    	    {
		         	        	maxexternalWorldTotal  = Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
	         	    	    }
		           
		           
		           
		           }
				
				
				
				
				 for(i=0;i<pubreport1.size();i++){
         	        
         	        	 share = ((double)(Integer.parseInt(pubreport1.get(i).getCount())/(double)total)*100);
         	        	scaledshare = ((double)(Integer.parseInt(pubreport1.get(i).getCount())/(double)maxTotal)*100);
         	     
         	         
         	          externalWorldShare =  (((double)Integer.parseInt(pubreport1.get(i).getExternalWorldCount())/(double)externalWorldtotal)*100);
         	          scaledexternalWorldShare =  (((double)Integer.parseInt(pubreport1.get(i).getExternalWorldCount())/(double)maxexternalWorldTotal  )*100);
         	          
         	          
         	          
         	         Double share3 = round(share, 2);
         	         Double scaledshare3 = round(scaledshare,2);
         	          pubreport1.get(i).setShare(share3.toString());
         	         pubreport1.get(i).setScaledShare(scaledshare3.toString());
         	          
         	          Double externalWorldShare3  = round(externalWorldShare,2);
         	          scaledexternalWorldShare =  round(scaledexternalWorldShare,2);
         	         
         	  
         	          pubreport1.get(i).setExternalWorldShare(externalWorldShare3.toString());                 
				      pubreport1.get(i).setScaledExternalWorldShare(scaledexternalWorldShare.toString());
				 }
		
         
				 
		 
	       for(i=0;i<pubreport1.size();i++){
    	         
    	          
    	         
    	           
    	          
    	        	total1=	Integer.parseInt(pubreport1.get(i).getCount());
    	        
    	        
    	           
    	           
    	            externalworldtotal1 = Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
    	       
    	        
                for(int j =0 ; j < pubreport1.get(i).getChildren().size(); j++){
                		
                	Integer total2 = 0;	
                	
                	
                	    total2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getCount());
                
                	
		                
                	    Integer externalWorldTotal2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getExternalWorldCount());	  
                		  
                		  Double share1 = ((double)total2/(double)total1)*100;
                		
                	    
                	    Double share2 = round(share1, 2);
                	       
                	    
                	    Double share3 = ((double)total2/(double)total)*100;
                	    
                	    Double share4 = round(share3,2);
                	    
                	    scaledshare1 = ((double)total2/(double)maxTotal)*100;
                	    
                	    Double scaledshare4 = round(scaledshare1,2); 
                	    
                	    Double externalWorldShare4 = ((double)externalWorldTotal2/(double)externalWorldtotal)*100;
                	    
                	    Double externalWorldShare5 = round(externalWorldShare4,2);
                	    
                	    Double scaledexternalWorldShare1 = ((double)externalWorldTotal2/(double)maxexternalWorldTotal)*100;
                	    scaledexternalWorldShare1 = round(scaledexternalWorldShare1,2 );
                	    
                		pubreport1.get(i).getChildren().get(j).setShare(share4.toString());
                        pubreport1.get(i).getChildren().get(j).setExternalWorldShare(externalWorldShare5.toString());
                        
                        pubreport1.get(i).getChildren().get(j).setScaledShare(scaledshare4.toString());
                        pubreport1.get(i).getChildren().get(j).setScaledExternalWorldShare(scaledexternalWorldShare1.toString());
                
                }
                	
                for(int j =0 ; j < pubreport1.get(i).getGenderlist().size(); j++){
            		
                	Integer total2 = 0;	
                	
              	
              	    total2 = Integer.parseInt(pubreport1.get(i).getGenderlist().get(j).getCount());
              
              	
		                
              	    Integer externalWorldTotal2 = Integer.parseInt(pubreport1.get(i).getGenderlist().get(j).getExternalWorldCount());	  
              		  
              		  Double share1 = ((double)total2/(double)total1)*100;
              		
              	    
              	    Double share2 = round(share1, 2);
              	       
              	    
              	    Double share3 = ((double)total2/(double)total)*100;
              	    
              	    Double share4 = round(share3,2);
              	    
              	    scaledshare1 = ((double)total2/(double)maxTotal)*100;
              	    
              	    Double scaledshare4 = round(scaledshare1,2); 
              	    
              	    Double externalWorldShare3 = ((double)externalWorldTotal2/(double)externalworldtotal1)*100;
              	    
              	     externalWorldShare3 = round(externalWorldShare3,2);
              	    
              	    Double externalWorldShare4 = ((double)externalWorldTotal2/(double)externalWorldtotal)*100;
              	    
              	    Double externalWorldShare5 = round(externalWorldShare4,2);
              	    
              	    Double scaledexternalWorldShare1 = ((double)externalWorldTotal2/(double)maxexternalWorldTotal)*100;
              	    scaledexternalWorldShare1 = round(scaledexternalWorldShare1,2 );
              	    
              		pubreport1.get(i).getGenderlist().get(j).setShare(share2.toString());
                      pubreport1.get(i).getGenderlist().get(j).setExternalWorldShare(externalWorldShare3.toString());
                      
                      pubreport1.get(i).getGenderlist().get(j).setScaledShare(scaledshare4.toString());
                      pubreport1.get(i).getGenderlist().get(j).setScaledExternalWorldShare(scaledexternalWorldShare1.toString());
                
                }
            
            
            }      
		
            Double share5 = 0.0;
			Double share6 = 0.0;
			Double scaledshare7 = 0.0;
	        Double scaledexternalWorldShare7 = 0.0;
			if(total!=0){
	       
	        share5 = ((double)count/(double)total1)*100;
			scaledshare7 = ((double)count/(double)maxTotal)*100; 
	      
	       
	        
			share6 = round(share5,2);
	        scaledshare7 = round(scaledshare7,2);
			}
				
	        Double externalWorldShare5 = 0.0;
	        externalWorldShare5 = ((double)externalWorldCount/(double)externalworldtotal1)*100;
	        scaledexternalWorldShare7 = ((double)externalWorldCount/(double)maxexternalWorldTotal)*100;
	        
	        
	        Double externalWorldShare6 = 0.0;
	        externalWorldShare6 = round(externalWorldShare5,2);
	        scaledexternalWorldShare7 = round(scaledexternalWorldShare7,2);
	        
			         PublisherReport obj2 = new PublisherReport();
			         obj2.setAudience_segment("Technology & computing");
				     obj2.setExternalWorldCount(externalWorldCount.toString());
	        		 obj2.setCount(count.toString());
	        	//	 obj2.setVisitorCount(visitorCount.toString());
	        	//	 obj2.setThirdpartycount(thirdpartycount.toString());
	        	//	 obj2.setAverageTime("1.0");
	        		 obj2.setShare(share6.toString());
	        		 obj2.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj2.setScaledShare(scaledshare7.toString());
	        		 obj2.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 
	        		 PublisherReport obj4a = new PublisherReport();
			         obj4a.setGender("Male");
				     obj4a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4a.setCount(count.toString());
	        //		 obj4a.setVisitorCount(visitorCount.toString());
	        //		 obj4a.setThirdpartycount(thirdpartycount.toString());
	        //		 obj4a.setAverageTime("2.0");
	        		 obj4a.setShare(share6.toString());
	        		 obj4a.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj4a.setScaledShare(scaledshare7.toString());
	        		 obj4a.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj2.getGenderlist().add(obj4a);
	        		 
	        		 
	        		 PublisherReport obj5a = new PublisherReport();
			         obj5a.setGender("Female");
				     obj5a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5a.setCount(count.toString());
	        	//	 obj5a.setVisitorCount(visitorCount.toString());
	            // 	 obj5a.setThirdpartycount(thirdpartycount.toString());
	        	//	 obj5a.setAverageTime("1.0");
	        		 obj5a.setShare(share6.toString());
	        		 obj5a.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj5a.setScaledShare(scaledshare7.toString());
	        		 obj5a.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj2.getGenderlist().add(obj5a);
	        		 
	        		 
	        		 
	        		 PublisherReport obj3 = new PublisherReport();
			         obj3.setAudience_segment("Hardware");
				     obj3.setExternalWorldCount(externalWorldCount.toString());
	        		 obj3.setCount(count.toString());
	        //		 obj3.setVisitorCount(visitorCount.toString());
	        //		 obj3.setThirdpartycount(thirdpartycount.toString());
	        //		 obj3.setAverageTime("1.0");
	        		 obj3.setShare(share6.toString());
	        		 obj3.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj3.setScaledShare(scaledshare7.toString());
	        		 obj3.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj2.getChildren().add(obj3);
	        		 
	        		 PublisherReport obj4 = new PublisherReport();
			         obj4.setGender("Male");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	        //		 obj4.setVisitorCount(visitorCount.toString());
	        //		 obj4.setThirdpartycount(thirdpartycount.toString());
	        //		 obj4.setAverageTime("2.0");
	        		 obj4.setShare(share6.toString()); 
	        		 obj4.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj4.setScaledShare(scaledshare7.toString());
	        		 obj4.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj3.getGenderlist().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setGender("Female");
				     obj5.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5.setCount(count.toString());
	        	//	 obj5.setVisitorCount(visitorCount.toString());
	        	//	 obj5.setThirdpartycount(thirdpartycount.toString());
	        	//	 obj5.setAverageTime("1.0");
	        		 obj5.setShare(share6.toString());
	        		 obj5.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj5.setScaledShare(scaledshare7.toString());
	        		 obj5.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj3.getGenderlist().add(obj5);
	        		 
	        	//	 pubreport1.add(obj2);
	        		
	        		 Collections.sort(pubreport1, new Comparator<PublisherReport>() {
		     				
		     				@Override
		     		        public int compare(PublisherReport o1, PublisherReport o2) {
		     					return Integer.parseInt(o1.getCount()) > Integer.parseInt(o2.getCount()) ? -1 : (Integer.parseInt(o1.getCount()) < Integer.parseInt(o2.getCount())) ? 1 : 0;
		     		        }
		     		    });	
	        		 
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
      		
      		 String agegroup = rs.getString(2);
      		 agegroup= agegroup.replace("_","-");
      		 agegroup=agegroup+ " Years";
      		 if(agegroup.contains("medium")==false && agegroup.contains("high")==false)
      		 obj1.setAge(agegroup);
      	
      	  
      	
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
      	        Integer randomNumber1 = 104570 + max;
      	        Integer randomNumber2 = random.nextInt(1000 + 1 - 500) + 500;
      	    	
      	    	obj1.setCount(rs.getString(3)); 	
      	        obj1.setExternalWorldCount(randomNumber1.toString());	
      	     //   obj1.setVisitorCount(randomNumber1.toString());
      	      //  obj1.setThirdpartycount(randomNumber2.toString());
      	      //  obj1.setAverageTime("1.0");	
      	    	
      	    	
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
			        		 Integer eWC  = (int)Double.parseDouble(s.getExternalWorldCount());
			        		 obj3.setExternalWorldCount(eWC.toString());
			        		 Integer count1 = (int)Double.parseDouble(s.getCount());
			        		 obj3.setCount(count1.toString());
			    //    		 obj3.setVisitorCount(s.getVisitorCount());
			     //   		 obj3.setThirdpartycount(s.getThirdpartycount());
			      //  		 obj3.setAverageTime("2.0");
			        		count = count+(int) Double.parseDouble(s.getCount());
			        		externalWorldCount = externalWorldCount+(int) Double.parseDouble(s.getExternalWorldCount());
			       // 		visitorCount = visitorCount + (int) Double.parseDouble(s.getVisitorCount());
			       // 		thirdpartycount = thirdpartycount + (int) Double.parseDouble(s.getThirdpartycount());
			        		obj2.getAgegrouplist().add(obj3);
			        		
			        		}
			        	
		        		
		        		
		        		}
					     obj2.setExternalWorldCount(externalWorldCount.toString());
		        		 obj2.setCount(count.toString());
		        	//	 obj2.setVisitorCount(visitorCount.toString());
		        	//	 obj2.setThirdpartycount(thirdpartycount.toString());
		        //		 obj2.setAverageTime("1.0");
				//	System.out.println(entry.getKey() + "/" + entry.getValue());
				
				         pubreport1.add(obj2);
				         
				        
				
				}

				}	
				 Integer total = 0;
		           Double share = 0.0;
		           Double scaledshare = 0.0;
		           Double scaledshare1 = 0.0;
		           Integer externalWorldtotal = 0;
		           Double externalWorldShare = 0.0;
		           Double scaledexternalWorldShare = 0.0;
		           Integer maxTotal = 0;
		           Integer maxexternalWorldTotal = 0;
		           Integer total1= 0;
	  	           Integer externalworldtotal1 =0;
		         
	  	         for(i=0;i<pubreport1.size();i++){
  	        	   
  	        	   if(pubreport1.get(i).getAudience_segment() == null || pubreport1.get(i).getAudience_segment().isEmpty())
      	    	    {
      	    	    	pubreport1.remove(i);
      	    	    }
  	        	   
  	        	   
  	           }
	  	           
	  	           
	  	           for(i=0;i<pubreport1.size();i++){
		         	  
		        	    
		         		  total=total+Integer.parseInt(pubreport1.get(i).getCount());
		         		 if(Integer.parseInt(pubreport1.get(i).getCount())> maxTotal)
	         	    	    {
	         	    	    	maxTotal = Integer.parseInt(pubreport1.get(i).getCount());
	         	    	    }
		        	    
		        	   
		                  
		         	         externalWorldtotal =  externalWorldtotal + Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
		           
		         	        if(Integer.parseInt(pubreport1.get(i).getExternalWorldCount())> maxexternalWorldTotal)
	         	    	    {
		         	        	maxexternalWorldTotal  = Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
	         	    	    }
		           
		           
		           
		           }
				
				
				
				
				 for(i=0;i<pubreport1.size();i++){
       	       
       	        	 share = ((double)(Integer.parseInt(pubreport1.get(i).getCount())/(double)total)*100);
       	        	scaledshare = ((double)(Integer.parseInt(pubreport1.get(i).getCount())/(double)maxTotal)*100);
       	        
       	          externalWorldShare =  (((double)Integer.parseInt(pubreport1.get(i).getExternalWorldCount())/(double)externalWorldtotal)*100);
       	          scaledexternalWorldShare =  (((double)Integer.parseInt(pubreport1.get(i).getExternalWorldCount())/(double)maxexternalWorldTotal  )*100);
       	          
       	          
       	          
       	         Double share3 = round(share, 2);
       	         Double scaledshare3 = round(scaledshare,2);
       	          pubreport1.get(i).setShare(share3.toString());
       	         pubreport1.get(i).setScaledShare(scaledshare3.toString());
       	          
       	          Double externalWorldShare3  = round(externalWorldShare,2);
       	          scaledexternalWorldShare =  round(scaledexternalWorldShare,2);
       	         
       	  
       	          pubreport1.get(i).setExternalWorldShare(externalWorldShare3.toString());                 
				      pubreport1.get(i).setScaledExternalWorldShare(scaledexternalWorldShare.toString());
				 }
		
       
				 
		 
	       for(i=0;i<pubreport1.size();i++){
  	         
  	          
  	        
  	        	total1=	Integer.parseInt(pubreport1.get(i).getCount());
  	        
  	        
  	          
  	        
  	           externalworldtotal1  = Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
  	        
              for(int j =0 ; j < pubreport1.get(i).getChildren().size(); j++){
              		
              	Integer total2 = 0;	
              	
              	
              	    total2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getCount());
              
              	 
		                
              	    Integer externalWorldTotal2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getExternalWorldCount());	  
              		  
              		  Double share1 = ((double)total2/(double)total1)*100;
              		
              	    
              	    Double share2 = round(share1, 2);
              	       
              	    
              	    Double share3 = ((double)total2/(double)total)*100;
              	    
              	    Double share4 = round(share3,2);
              	    
              	    scaledshare1 = ((double)total2/(double)maxTotal)*100;
              	    
              	    Double scaledshare4 = round(scaledshare1,2); 
              	    
              	    Double externalWorldShare4 = ((double)externalWorldTotal2/(double)externalWorldtotal)*100;
              	    
              	    Double externalWorldShare5 = round(externalWorldShare4,2);
              	    
              	    Double scaledexternalWorldShare1 = ((double)externalWorldTotal2/(double)maxexternalWorldTotal)*100;
              	    scaledexternalWorldShare1 = round(scaledexternalWorldShare1,2 );
              	    
              		pubreport1.get(i).getChildren().get(j).setShare(share2.toString());
                      pubreport1.get(i).getChildren().get(j).setExternalWorldShare(externalWorldShare5.toString());
                      
                      pubreport1.get(i).getChildren().get(j).setScaledShare(scaledshare4.toString());
                      pubreport1.get(i).getChildren().get(j).setScaledExternalWorldShare(scaledexternalWorldShare1.toString());
              
              }
              	
              for(int j =0 ; j < pubreport1.get(i).getAgegrouplist().size(); j++){
          		
              	Integer total2 = 0;	
              	
            	 
            	    total2 = Integer.parseInt(pubreport1.get(i).getAgegrouplist().get(j).getCount());
            
            	
		                
            	    Integer externalWorldTotal2 = Integer.parseInt(pubreport1.get(i).getAgegrouplist().get(j).getExternalWorldCount());	  
            		  
            		  Double share1 = ((double)total2/(double)total1)*100;
            		
            	    
            	    Double share2 = round(share1, 2);
            	       
            	    
            	    Double share3 = ((double)total2/(double)total)*100;
            	    
            	    Double share4 = round(share3,2);
            	    
            	    scaledshare1 = ((double)total2/(double)maxTotal)*100;
            	    
            	    Double scaledshare4 = round(scaledshare1,2); 
            	    
            	    Double externalWorldShare3 = ((double)externalWorldTotal2/(double)externalworldtotal1)*100;
            	    
            	    externalWorldShare3 = round(externalWorldShare3,2); 
            	    
            	    
            	    Double externalWorldShare4 = ((double)externalWorldTotal2/(double)externalWorldtotal)*100;
            	    
            	    Double externalWorldShare5 = round(externalWorldShare4,2);
            	    
            	    Double scaledexternalWorldShare1 = ((double)externalWorldTotal2/(double)maxexternalWorldTotal)*100;
            	    scaledexternalWorldShare1 = round(scaledexternalWorldShare1,2 );
            	    
            		pubreport1.get(i).getAgegrouplist().get(j).setShare(share2.toString());
                    pubreport1.get(i).getAgegrouplist().get(j).setExternalWorldShare(externalWorldShare3.toString());
                    
                    pubreport1.get(i).getAgegrouplist().get(j).setScaledShare(scaledshare4.toString());
                    pubreport1.get(i).getAgegrouplist().get(j).setScaledExternalWorldShare(scaledexternalWorldShare1.toString());
              
              }
          
          
          }      

				    
            Double share5 = 0.0;
			Double share6 = 0.0;
			Double scaledshare7 = 0.0;
	        Double scaledexternalWorldShare7 = 0.0;
	        
	        
	        if(total!=0){
	       
	      
	        share5 = ((double)count/(double)total1)*100;
	        scaledshare7 = ((double)count/(double)maxTotal)*100; 
	        
	           
	        
			share6 = round(share5,2);
	        scaledshare7 = round(scaledshare7,2);
	        }
           
	        Double externalWorldShare5 = 0.0;
	        externalWorldShare5 = ((double)externalWorldCount/(double)externalworldtotal1)*100;
	        
	        Double externalWorldShare6 = 0.0;
	        externalWorldShare6 = round(externalWorldShare5,2);
	        
	        scaledexternalWorldShare7 = ((double)externalWorldCount/(double)maxexternalWorldTotal)*100;
		    scaledexternalWorldShare7 = round(scaledexternalWorldShare7,2);

			         PublisherReport obj2 = new PublisherReport();
			         obj2.setAudience_segment("Technology & computing");
				     obj2.setExternalWorldCount(externalWorldCount.toString());
	        		 obj2.setCount(count.toString());
	        //		 obj2.setVisitorCount(visitorCount.toString());
	        //		 obj2.setThirdpartycount(thirdpartycount.toString());
	        //		 obj2.setAverageTime("2.0");
				     obj2.setShare(share6.toString());
				     obj2.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj2.setScaledShare(scaledshare7.toString());
				     obj2.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     
	        		 PublisherReport obj4a = new PublisherReport();
			         obj4a.setAge("25-34 Years");
				     obj4a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4a.setCount(count.toString());
	        //		 obj4a.setVisitorCount(visitorCount.toString());
	       // 		 obj4a.setThirdpartycount(thirdpartycount.toString());
	       // 		 obj4a.setAverageTime("2.0");
	        		 obj4a.setShare(share6.toString());
	        		 obj4a.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj4a.setScaledShare(scaledshare7.toString());
				     obj4a.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj2.getAgegrouplist().add(obj4a);
	        		
	        		 
	        		 PublisherReport obj5a = new PublisherReport();
			         obj5a.setAge("35-44 Years");
				     obj5a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5a.setCount(count.toString());
	      //  		 obj5a.setVisitorCount(visitorCount.toString());
	       // 		 obj5a.setThirdpartycount(thirdpartycount.toString());
	       // 		 obj5a.setAverageTime("1.0");
	        		 obj5a.setShare(share6.toString());
	        		 obj5a.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj5a.setScaledShare(scaledshare7.toString());
				     obj5a.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj2.getAgegrouplist().add(obj5a);
	        		 
	        		 

	        		 PublisherReport obj6a = new PublisherReport();
			         obj6a.setAge("45-54");
				     obj6a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6a.setCount(count.toString());
	        //		 obj6a.setVisitorCount(visitorCount.toString());
	        //		 obj6a.setThirdpartycount(thirdpartycount.toString());
	        //		 obj6a.setAverageTime("1.0");
	        		 obj6a.setShare(share6.toString());
	        		 obj6a.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj6a.setScaledShare(scaledshare7.toString());
				     obj6a.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj2.getAgegrouplist().add(obj6a);
	        		 
	        		 
	        		 	        		 
	        		 
	        		 PublisherReport obj3 = new PublisherReport();
			         obj3.setAudience_segment("Hardware");
				     obj3.setExternalWorldCount(externalWorldCount.toString());
	        		 obj3.setCount(count.toString());
	       // 		 obj3.setVisitorCount(visitorCount.toString());
	       // 		 obj3.setThirdpartycount(thirdpartycount.toString());
	       // 		 obj3.setAverageTime("1.0");
	        		
	        		 obj3.setShare(share6.toString());
	        		 obj3.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj3.setScaledShare(scaledshare7.toString());
				     obj3.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj2.getChildren().add(obj3);
	        		 
	    	        		 
	        		 
	        		 PublisherReport obj4 = new PublisherReport();
			         obj4.setAge("25-34 Years");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	      //  		 obj4.setVisitorCount(visitorCount.toString());
	       // 		 obj4.setThirdpartycount(thirdpartycount.toString());
	       // 		 obj4.setAverageTime("2.0");
	        		 obj4.setShare(share6.toString());
	        		 obj4.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj4.setScaledShare(scaledshare7.toString());
				     obj4.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj3.getAgegrouplist().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setAge("35-44 Years");
				     obj5.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5.setCount(count.toString());
	       // 		 obj5.setVisitorCount(visitorCount.toString());
	       // 		 obj5.setThirdpartycount(thirdpartycount.toString());
	       // 		 obj5.setAverageTime("1.0");
	        		 obj5.setShare(share6.toString());
	        		 obj5.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj5.setScaledShare(scaledshare7.toString());
				     obj5.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj3.getAgegrouplist().add(obj5);
	        		 
	        		 

	        		 PublisherReport obj6 = new PublisherReport();
			         obj6.setAge("45-54");
				     obj6.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6.setCount(count.toString());
	        //		 obj6.setVisitorCount(visitorCount.toString());
	        //		 obj6.setThirdpartycount(thirdpartycount.toString());
	       // 		 obj6.setAverageTime("1.0");
	        		 obj6.setShare(share6.toString());
	        		 obj6.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj6.setScaledShare(scaledshare7.toString());
				     obj6.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
	        		 obj3.getAgegrouplist().add(obj6);
	        		 
	        	//	 pubreport1.add(obj2); 
	        		
	        		 Collections.sort(pubreport1, new Comparator<PublisherReport>() {
		     				
		     				@Override
		     		        public int compare(PublisherReport o1, PublisherReport o2) {
		     					return Integer.parseInt(o1.getCount()) > Integer.parseInt(o2.getCount()) ? -1 : (Integer.parseInt(o1.getCount()) < Integer.parseInt(o2.getCount())) ? 1 : 0;
		     		        }
		     		    });	
	        		 
	        		 
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
			          obj.setIncomelevel(AggregationModule.capitalizeString(rs.getString(1)));
		     
		        	
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
	          obj1.setIncomelevel(AggregationModule.capitalizeString(rs.getString(2)));
			       
      	   
      	
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
      	        Integer randomNumber1 = max+115600;
      	        Integer randomNumber2 = random.nextInt(1000 + 1 - 500) + 500;
      	    	
      	    	obj1.setCount(rs.getString(3)); 	
      	        obj1.setExternalWorldCount(randomNumber1.toString());	
      	 //       obj1.setVisitorCount(randomNumber1.toString());
      	  //      obj1.setThirdpartycount(randomNumber2.toString());
      	  //      obj1.setAverageTime("1.0");	
      	    	
      	    	
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
			        		 Integer eWC  = (int)Double.parseDouble(s.getExternalWorldCount());
			        		 obj3.setExternalWorldCount(eWC.toString());
			        		 Integer count1 = (int)Double.parseDouble(s.getCount());
			        		 obj3.setCount(count1.toString());
			        //		 obj3.setVisitorCount(s.getVisitorCount());
			//        		 obj3.setThirdpartycount(s.getThirdpartycount());
			  //      		 obj3.setAverageTime("1.0");
			        		count = count+(int) Double.parseDouble(s.getCount());
			        		externalWorldCount = externalWorldCount+(int) Double.parseDouble(s.getExternalWorldCount());
			    //    		visitorCount = visitorCount + (int) Double.parseDouble(s.getVisitorCount());
			     //   		thirdpartycount = thirdpartycount + (int) Double.parseDouble(s.getThirdpartycount());
			        		obj2.getIncomelevellist().add(obj3);
			        		
			        		}
			        	
		        		
		        		
		        		}
					     obj2.setExternalWorldCount(externalWorldCount.toString());
		        		 obj2.setCount(count.toString());
		        	//	 obj2.setVisitorCount(visitorCount.toString());
		        	//	 obj2.setThirdpartycount(thirdpartycount.toString());
		        	//	 obj2.setAverageTime("2.0");
				//	System.out.println(entry.getKey() + "/" + entry.getValue());
				
				         pubreport1.add(obj2);
				         
				        
				
				}

				}	
				
				
				 Integer total = 0;
		           Double share = 0.0;
		           Double scaledshare = 0.0;
		           Double scaledshare1 = 0.0;
		           Integer externalWorldtotal = 0;
		           Double externalWorldShare = 0.0;
		           Double scaledexternalWorldShare = 0.0;
		           Integer maxTotal = 0;
		           Integer maxexternalWorldTotal = 0;
		           Integer total1= 0;
		           Integer externalworldtotal1 = 0;
		           
                    for(i=0;i<pubreport1.size();i++){
    	        	   
    	        	   if(pubreport1.get(i).getAudience_segment() == null || pubreport1.get(i).getAudience_segment().isEmpty())
        	    	    {
        	    	    	pubreport1.remove(i);
        	    	    }
    	        	   
    	        	   
    	           }
		           
		           
		           
		           
		           for(i=0;i<pubreport1.size();i++){
		         	  
		         		  total=total+Integer.parseInt(pubreport1.get(i).getCount());
		         		 if(Integer.parseInt(pubreport1.get(i).getCount())> maxTotal)
	         	    	    {
	         	    	    	maxTotal = Integer.parseInt(pubreport1.get(i).getCount());
	         	    	    }
		        	    
		        	   
		                  
		         	         externalWorldtotal =  externalWorldtotal + Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
		           
		         	        if(Integer.parseInt(pubreport1.get(i).getExternalWorldCount())> maxexternalWorldTotal)
	         	    	    {
		         	        	maxexternalWorldTotal  = Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
	         	    	    }
		           
		           
		           
		           }
				
				
				
				
				 for(i=0;i<pubreport1.size();i++){
     	        
     	        	 share = ((double)(Integer.parseInt(pubreport1.get(i).getCount())/(double)total)*100);
     	        	scaledshare = ((double)(Integer.parseInt(pubreport1.get(i).getCount())/(double)maxTotal)*100);
     	         
     	          
     	          externalWorldShare =  (((double)Integer.parseInt(pubreport1.get(i).getExternalWorldCount())/(double)externalWorldtotal)*100);
     	          scaledexternalWorldShare =  (((double)Integer.parseInt(pubreport1.get(i).getExternalWorldCount())/(double)maxexternalWorldTotal  )*100);
     	          
     	          
     	          
     	         Double share3 = round(share, 2);
     	         Double scaledshare3 = round(scaledshare,2);
     	          pubreport1.get(i).setShare(share3.toString());
     	         pubreport1.get(i).setScaledShare(scaledshare3.toString());
     	          
     	          Double externalWorldShare3  = round(externalWorldShare,2);
     	          scaledexternalWorldShare =  round(scaledexternalWorldShare,2);
     	         
     	  
     	          pubreport1.get(i).setExternalWorldShare(externalWorldShare3.toString());                 
				      pubreport1.get(i).setScaledExternalWorldShare(scaledexternalWorldShare.toString());
				 }
		
     
				 
		 
	       for(i=0;i<pubreport1.size();i++){
	         
	          
	           
	           
	        	total1=	Integer.parseInt(pubreport1.get(i).getCount());
	        
	        
	          
	        
	            externalworldtotal1 = Integer.parseInt(pubreport1.get(i).getExternalWorldCount());
	           
	           
	           
            for(int j =0 ; j < pubreport1.get(i).getChildren().size(); j++){
            		
            	Integer total2 = 0;	
            	
            	 
            	    total2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getCount());
            
            	
		                
            	    Integer externalWorldTotal2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getExternalWorldCount());	  
            		  
            		  Double share1 = ((double)total2/(double)total1)*100;
            		
            	    
            	    Double share2 = round(share1, 2);
            	       
            	    
            	    Double share3 = ((double)total2/(double)total)*100;
            	    
            	    Double share4 = round(share3,2);
            	    
            	    scaledshare1 = ((double)total2/(double)maxTotal)*100;
            	    
            	    Double scaledshare4 = round(scaledshare1,2); 
            	   
            	    Double externalWorldShare3 = ((double)externalWorldTotal2/(double)externalworldtotal1)*100;
            	    
            	    
            	    Double externalWorldShare4 = ((double)externalWorldTotal2/(double)externalWorldtotal)*100;
            	    
            	    Double externalWorldShare5 = round(externalWorldShare4,2);
            	    
            	    Double scaledexternalWorldShare1 = ((double)externalWorldTotal2/(double)maxexternalWorldTotal)*100;
            	    scaledexternalWorldShare1 = round(scaledexternalWorldShare1,2 );
            	    
            		pubreport1.get(i).getChildren().get(j).setShare(share2.toString());
                    pubreport1.get(i).getChildren().get(j).setExternalWorldShare(externalWorldShare3.toString());
                    
                    pubreport1.get(i).getChildren().get(j).setScaledShare(scaledshare4.toString());
                    pubreport1.get(i).getChildren().get(j).setScaledExternalWorldShare(scaledexternalWorldShare1.toString());
            
            }
            	
            for(int j =0 ; j < pubreport1.get(i).getIncomelevellist().size(); j++){
        		
            	Integer total2 = 0;	
            	
          	
          	    total2 = Integer.parseInt(pubreport1.get(i).getIncomelevellist().get(j).getCount());
          
          	
		                
          	    Integer externalWorldTotal2 = Integer.parseInt(pubreport1.get(i).getIncomelevellist().get(j).getExternalWorldCount());	  
          		  
          		  Double share1 = ((double)total2/(double)total1)*100;
          		
          	    
          	    Double share2 = round(share1, 2);
          	       
          	    
          	    Double share3 = ((double)total2/(double)total)*100;
          	    
          	    Double share4 = round(share3,2);
          	    
          	    scaledshare1 = ((double)total2/(double)maxTotal)*100;
          	    
          	    Double scaledshare4 = round(scaledshare1,2); 
          	    
          	    Double externalWorldShare3 = ((double)externalWorldTotal2/(double)externalworldtotal1)*100;
          	    
          	    externalWorldShare3 = round(externalWorldShare3,2); 
        	    
          	    
          	    Double externalWorldShare4 = ((double)externalWorldTotal2/(double)externalWorldtotal)*100;
          	    
          	    Double externalWorldShare5 = round(externalWorldShare4,2);
          	    
          	    Double scaledexternalWorldShare1 = ((double)externalWorldTotal2/(double)maxexternalWorldTotal)*100;
          	    scaledexternalWorldShare1 = round(scaledexternalWorldShare1,2 );
          	    
          		pubreport1.get(i).getIncomelevellist().get(j).setShare(share2.toString());
                  pubreport1.get(i).getIncomelevellist().get(j).setExternalWorldShare(externalWorldShare3.toString());
                  
                  pubreport1.get(i).getIncomelevellist().get(j).setScaledShare(scaledshare4.toString());
                  pubreport1.get(i).getIncomelevellist().get(j).setScaledExternalWorldShare(scaledexternalWorldShare1.toString());
            
            }
        
        
        }      

				
		    
            Double share5 = 0.0;
			Double share6 = 0.0;
			Double scaledshare7 = 0.0;
	        Double scaledexternalWorldShare7 = 0.0;
	        
	        
	        if(total!=0){
	       
	      
	        share5 = ((double)count/(double)total1)*100;
	        scaledshare7 = ((double)count/(double)maxTotal)*100; 
	        
	     
	      
	        
			share6 = round(share5,2);
	        scaledshare7 = round(scaledshare7,2);
	        }
          
	        Double externalWorldShare5 = 0.0;
	        externalWorldShare5 = ((double)externalWorldCount/(double)externalworldtotal1)*100;
	        
	        Double externalWorldShare6 = 0.0;
	        externalWorldShare6 = round(externalWorldShare5,2);
	        
	        scaledexternalWorldShare7 = ((double)externalWorldCount/(double)maxexternalWorldTotal)*100;
		    scaledexternalWorldShare7 = round(scaledexternalWorldShare7,2);

           
				

			         PublisherReport obj2 = new PublisherReport();
			         obj2.setAudience_segment("Technology & computing");
				     obj2.setExternalWorldCount(externalWorldCount.toString());
	        		 obj2.setCount(count.toString());
	        	//	 obj2.setVisitorCount(visitorCount.toString());
	        	//	 obj2.setThirdpartycount(thirdpartycount.toString());
	        	//	 obj2.setAverageTime("1.0");
	        		 obj2.setShare(share6.toString());
	        		 obj2.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj2.setScaledShare(scaledshare7.toString());
				     obj2.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     
	        		 
	        		 PublisherReport obj4a = new PublisherReport();
			         obj4a.setIncomelevel("Medium");
				     obj4a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4a.setCount(count.toString());
	      //  		 obj4a.setVisitorCount(visitorCount.toString());
	       // 		 obj4a.setThirdpartycount(thirdpartycount.toString());
	       // 		 obj4a.setAverageTime("1.0");
	        		 obj4a.setShare(share6.toString());
	        		 obj4a.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj4a.setScaledShare(scaledshare7.toString());
				     obj4a.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     
	        		 obj2.getIncomelevellist().add(obj4a);
	        		 
	        		 
	        		 PublisherReport obj5a = new PublisherReport();
			         obj5a.setIncomelevel("Low");
				     obj5a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5a.setCount(count.toString());
	        //		 obj5a.setVisitorCount(visitorCount.toString());
	        //		 obj5a.setThirdpartycount(thirdpartycount.toString());
	       // 		 obj5a.setAverageTime("2.0");
	        		 obj5a.setShare(share6.toString());
	        		 obj5a.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj5a.setScaledShare(scaledshare7.toString());
				     obj5a.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     
	        		 obj2.getIncomelevellist().add(obj5a);
	        		 
	        		 PublisherReport obj6a = new PublisherReport();
			         obj6a.setIncomelevel("High");
				     obj6a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6a.setCount(count.toString());
	        //		 obj6a.setVisitorCount(visitorCount.toString());
	        //		 obj6a.setThirdpartycount(thirdpartycount.toString());
	        //		 obj6a.setAverageTime("1.0");
	        		 obj6a.setShare(share6.toString());
	        		 obj6a.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj6a.setScaledShare(scaledshare7.toString());
				     obj6a.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     
	        		 obj2.getIncomelevellist().add(obj6a);
	        		 
	        		 
	        		 
	        		 PublisherReport obj3 = new PublisherReport();
			         obj3.setAudience_segment("Hardware");
				     obj3.setExternalWorldCount(externalWorldCount.toString());
	        		 obj3.setCount(count.toString());
	       // 		 obj3.setVisitorCount(visitorCount.toString());
	       // 		 obj3.setThirdpartycount(thirdpartycount.toString());
	       /// 		 obj3.setAverageTime("1.0");
	        		 obj3.setShare(share6.toString());
	        		 obj3.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj3.setScaledShare(scaledshare7.toString());
				     obj3.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     

	        		 obj2.getChildren().add(obj3);
	        		 
	        		 PublisherReport obj4 = new PublisherReport();
			         obj4.setIncomelevel("Medium");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	        	//	 obj4.setVisitorCount(visitorCount.toString());
	        	//	 obj4.setThirdpartycount(thirdpartycount.toString());
	        	//	 obj4.setAverageTime("1.0");
	        		 obj4.setShare(share6.toString());
	        		 obj4.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj4.setScaledShare(scaledshare7.toString());
				     obj4.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     
	        		 obj3.getIncomelevellist().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setIncomelevel("Low");
				     obj5.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5.setCount(count.toString());
	        //		 obj5.setVisitorCount(visitorCount.toString());
	        //		 obj5.setThirdpartycount(thirdpartycount.toString());
	        //		 obj5.setAverageTime("1.0");
	        		 obj5.setShare(share6.toString());
	        		 obj5.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj5.setScaledShare(scaledshare7.toString());
				     obj5.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     
	        		 obj3.getIncomelevellist().add(obj5);
	        		 
	        		 PublisherReport obj6 = new PublisherReport();
			         obj6.setIncomelevel("High");
				     obj6.setExternalWorldCount(externalWorldCount.toString());
	        		 obj6.setCount(count.toString());
	        	//	 obj6.setVisitorCount(visitorCount.toString());
	        	//	 obj6.setThirdpartycount(thirdpartycount.toString());
	        	//	 obj6.setAverageTime("1.0");
	        		 obj6.setShare(share6.toString());
	        		 obj6.setExternalWorldShare(externalWorldShare6.toString());
	        		 obj6.setScaledShare(scaledshare7.toString());
				     obj6.setScaledExternalWorldShare(scaledexternalWorldShare7.toString());
				     
	        		 obj3.getIncomelevellist().add(obj6);
	        		
	        	//	 pubreport1.add(obj2);
	        		 
	        		 
	        		 Collections.sort(pubreport1, new Comparator<PublisherReport>() {
		     				
		     				@Override
		     		        public int compare(PublisherReport o1, PublisherReport o2) {
		     					return Integer.parseInt(o1.getCount()) > Integer.parseInt(o2.getCount()) ? -1 : (Integer.parseInt(o1.getCount()) < Integer.parseInt(o2.getCount())) ? 1 : 0;
		     		        }
		     		    });	
	        		 
	        		 
	        		 
				out = new ObjectMapper().writeValueAsString(pubreport1);
		       System.out.println(out);
	    } catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	      return pubreport1;
	
	
	}




	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}











}
