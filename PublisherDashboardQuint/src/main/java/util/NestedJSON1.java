package util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.publisherdata.Daos.AggregationModule;
import com.publisherdata.model.PublisherReport;

public class NestedJSON1 {

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


	public static String getNestedJSONObject(ResultSet rs, String queryfield, List<String> groupby,String filter) {
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
		        		String audienceSegment = AggregationModule.audienceSegmentMap.get(rs.getString(1));
		        	 //   jobType = audienceSegment;
		        		String audienceSegmentCode = AggregationModule.audienceSegmentMap2.get(rs.getString(1));
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
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
			          obj.setMobile_device_model_name(rs.getString(1));
			          String[] mobiledeviceproperties = AggregationModule.devicecodeMap.get(rs.getString(1)).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]); 
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
      		String audienceSegment = AggregationModule.audienceSegmentMap.get(rs.getString(2));
      		String audienceSegmentCode = AggregationModule.audienceSegmentMap2.get(rs.getString(2));
      		if(audienceSegment!=null && !audienceSegment.isEmpty()){
      		obj.setSubcategory(audienceSegment);
      		obj.setSubcategorycode(audienceSegmentCode);
      		}
      		else
      	    obj1.setSubcategory(rs.getString(2));
	             }
      	
      	if(groupby.get(0).equals("agegroup"))
      	{
      		/*
      	
      		 data[0+1]=data[0+1].replace("_","-");
      		 data[0+1]=data[0+1]+ " Years";
      		 if(data[0+1].contains("medium")==false)
      		 obj.setAge(rs.getString(2));
      	
      	  */
      	
      	}
          	
          	
      	if(groupby.get(0).equals("incomelevel"))
	          obj1.setIncomelevel(rs.getString(2));
			       
      	   if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
          	obj1.setCount(rs.getString(3));
           
	            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
              obj1.setVisitorCount(rs.getString(3));
       

	            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
              obj1.setEngagementTime(rs.getString(3));	
		

		questions.add(obj1);
	    
		  
		
		
		jobTypes1.put(jobType,questions);
		//jobTypes.put(obj, questions); 
        
      	}
      }
			
		
		} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	    
			
			try {
			
				
				
				out = new ObjectMapper().writeValueAsString(jobTypes1);
		 //   System.out.println(out);
	    } catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	      return out;
	
	
	}






















}
