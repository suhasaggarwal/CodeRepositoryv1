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

public class NestedJSON5v1 {

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
				try{
				
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
      	        Double externalCount = 0.0;
      	    	obj1.setCount(rs.getString(3)); 	
      	        if(Integer.parseInt(rs.getString(6))!=0){
      	    	obj1.setExternalWorldCount(rs.getString(6));	
      	        }
      	    	else{
      	            externalCount = Double.parseDouble(rs.getString(3))+805.0;
      	        	obj1.setExternalWorldCount(externalCount.toString());
      	        }
      	        obj1.setVisitorCount(rs.getString(5));
      	        Double thirdpartyCount = externalCount - Double.parseDouble(rs.getString(3));
      	        
      	        obj1.setThirdpartycount(thirdpartyCount.toString());
      	        Integer AverageTime = 0;
      	        
      	        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
      	        AverageTime=(int) Math.round(Double.parseDouble(rs.getString(4))/Double.parseDouble(rs.getString(3)));
      	        

	            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
        	    AverageTime=(int) Math.round(Double.parseDouble(rs.getString(4))/Double.parseDouble(rs.getString(5)));
      	        
	           	        
      	        obj1.setAverageTime(AverageTime.toString());	
      	    	
      	    	obj1.setEngagementTime(rs.getString(4));
      	    }
      	    
      	    

		questions.add(obj1);
	    
		  
		
		
		jobTypes1.put(jobType,questions);
		//jobTypes.put(obj, questions); 
        
      	}
    
				}	
				catch(Exception e){
					
					continue;
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
		Integer engagementTime = 0;
		
		List<PublisherReport> pubreport1 = new ArrayList<PublisherReport>();
		
		Map<PublisherReport, Set<PublisherReport>> data = new HashMap<PublisherReport,Set<PublisherReport>>();
			
			try {
		
				for (Map.Entry<String, Set<PublisherReport>> entry : jobTypes1.entrySet())
				{
					
					try{
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
		        		engagementTime = 0;
		        		}
		        	
		        		Set<PublisherReport> pubreport = jobTypes1.get(entry.getKey());
		        		for (PublisherReport s : pubreport) {
		        			PublisherReport obj3 = new PublisherReport();
		        			String audienceSegment1 = AggregationModule.audienceSegmentMap.get(s.getSubcategory());
			        		String audienceSegmentCode1 = AggregationModule.audienceSegmentMap2.get(s.getSubcategory());
			        		if(audienceSegment1!=null && !audienceSegment1.isEmpty()){
			        		 obj3.setAudience_segment(audienceSegment1);
			        		 obj3.setAudienceSegmentCode(audienceSegmentCode1);
			        		 Integer eWC  = (int)Double.parseDouble(s.getExternalWorldCount());
			        		 obj3.setExternalWorldCount(eWC.toString());
			        		 Integer count1 = (int)Double.parseDouble(s.getCount());
			        		 obj3.setCount(count1.toString());
			        		 Integer vC = (int)Double.parseDouble(s.getVisitorCount());
			        		 obj3.setVisitorCount(vC.toString());
			        		 Integer tpC = (int)Double.parseDouble(s.getThirdpartycount());
			        		 obj3.setThirdpartycount(tpC.toString());
			        		 Integer eT = (int)Double.parseDouble(s.getEngagementTime());
			        		 obj3.setEngagementTime(eT.toString());
			        		 obj3.setAverageTime(s.getAverageTime());
			        		count = count+(int) Double.parseDouble(s.getCount());
			        		externalWorldCount = externalWorldCount+(int) Double.parseDouble(s.getExternalWorldCount());
			        		visitorCount = visitorCount + (int) Double.parseDouble(s.getVisitorCount());
			        		thirdpartycount = thirdpartycount + (int) Double.parseDouble(s.getThirdpartycount());
			        		engagementTime = engagementTime + (int ) Double.parseDouble(s.getEngagementTime());
			        		obj2.getChildren().add(obj3);
			        		
			        		}
			        	
		        		
		        		
		        		}
					     obj2.setExternalWorldCount(externalWorldCount.toString());
		        		 obj2.setCount(count.toString());
		        		 obj2.setVisitorCount(visitorCount.toString());
		        		 obj2.setThirdpartycount(thirdpartycount.toString());
		        		 obj2.setEngagementTime(engagementTime.toString());
		        		 Integer averageTime1 = 0;
		        		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		        		 averageTime1 = engagementTime / count;
		        		 
		        		 if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		        	     averageTime1 = engagementTime / visitorCount;
		        		 
		        		 obj2.setAverageTime(averageTime1.toString());
				//	System.out.println(entry.getKey() + "/" + entry.getValue());
				
				         pubreport1.add(obj2);
				         
				        
				
				}

				}	
				catch(Exception e){
					
					
					continue;
				}
				}
			         
				
				
				   Integer total = 0;
		           Double share = 0.0;
		                    
		           for(i=0;i<pubreport1.size();i++){
		         	  
		         	       if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		         		  total=total+Integer.parseInt(pubreport1.get(i).getCount());
		              
		                   if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
		                 	  total=total+Integer.parseInt(pubreport1.get(i).getEngagementTime());
		               
		                   if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		                 	  total=total+Integer.parseInt(pubreport1.get(i).getVisitorCount());
		         	  
		         	  
		           }
		                    
		            for(i=0;i<pubreport1.size();i++){
		         	         if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		         	        	 share = ((double)(Integer.parseInt(pubreport1.get(i).getCount())/(double)total)*100);
		         	        
		         	          if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
		         	        	  share =((double)(Integer.parseInt(pubreport1.get(i).getEngagementTime())/(double)total)*100);
		         	      
		         	          if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		         	        	  share =(((double)Integer.parseInt(pubreport1.get(i).getVisitorCount())/(double)total)*100);
		         	
		         	  
		         	          
		         	          Double share3 = round(share, 2);
		         	          
		         	          pubreport1.get(i).setShare(share3.toString());
		         	  
		           }
				
		            for(i=0;i<pubreport1.size();i++){
	        	         
	        	           Integer total1= 0;
	        	        
	        	           if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))	
	        	        	total1=	Integer.parseInt(pubreport1.get(i).getCount());
	        	        
	        	        
	        	           if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )	
	        	        	total1=	Integer.parseInt(pubreport1.get(i).getVisitorCount());
	        	        
	        	        

	        	           if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )	
	        	        	total1=	Integer.parseInt(pubreport1.get(i).getEngagementTime());
	        	           
	        	           
	        	           
	        	           
		                for(int j =0 ; j < pubreport1.get(i).getChildren().size(); j++){
		                		
		                	Integer total2 = 0;	
		                	
		                	  if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))	
		                	    total2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getCount());
		                
		                	  if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") ) 
		                		  total2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getVisitorCount());
				                
		                	  if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") ) 
		                		  total2 = Integer.parseInt(pubreport1.get(i).getChildren().get(j).getEngagementTime());
		                		  
		                		
		                	    Double share1 = ((double)total2/(double)total1)*100;
		                		
		                	    
		                	    Double share2 = round(share1, 2);
		                	       
		                	    
		                	    Double share3 = ((double)total2/(double)total)*100;
		                	    
		                	    Double share4 = round(share3,2);
		                	    
		                		pubreport1.get(i).getChildren().get(j).setShare(share4.toString());
		                
		                
		                }
		                	
		                	
		            
		            
		            }      
				
				
				
				
				
				
				
				
				
				
				
		        	
					Double share5 = 0.0;
					Double share6 = 0.0;
					
			        if(total!=0){
			       
			        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))	
			        share5 = ((double)56/(double)total)*100;
					
			        if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") ) 
			        share5 = ((double)105/(double)total)*100;
			        
			        if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )  
			        share5 = ((double)1045/(double)total)*100;
			        
					share6 = round(share5,2);
			        }
			        
				
			        /*
			        PublisherReport obj2 = new PublisherReport();
				         obj2.setAudience_segment("Technology & computing");
					     obj2.setExternalWorldCount("167");
		        		 obj2.setCount("56");
		        		 obj2.setVisitorCount("105");
		        		 obj2.setThirdpartycount("102");
		        		 obj2.setEngagementTime("1045");
		        		 obj2.setAverageTime("1");
					     obj2.setShare(share6.toString());
		        		 
		        		 PublisherReport obj3 = new PublisherReport();
				         obj3.setAudience_segment("Hardware");
					     obj3.setExternalWorldCount("167");
		        		 obj3.setCount("56");
		        		 obj3.setVisitorCount("105");
		        		 obj3.setThirdpartycount("102");
		        		 obj3.setAverageTime("1");
		        		 obj3.setShare(share6.toString());

		        		 obj3.setEngagementTime("1045");
		        		
		        		 obj2.getChildren().add(obj3);
		        		 
		        		 PublisherReport obj4 = new PublisherReport();
				         obj4.setAudience_segment("Computer");
					     obj4.setExternalWorldCount("167");
		        		 obj4.setCount("56");
		        		 obj4.setVisitorCount("105");
		        		 obj4.setThirdpartycount("102");
		        		 obj4.setShare(share6.toString());
		        		 obj4.setAverageTime("2");

		        		 obj4.setEngagementTime("1045");
		        		
		        		 obj3.getChildren().add(obj4);
		        		 
		        		 
		        		 PublisherReport obj5 = new PublisherReport();
				         obj5.setAudience_segment("Portable computer");
					     obj5.setExternalWorldCount("167");
		        		 obj5.setCount("56");
		        		 obj5.setVisitorCount("105");
		        		 obj5.setThirdpartycount("102");
		        		 obj5.setAverageTime("1");
		        		 obj5.setShare(share6.toString());

		        		 obj5.setEngagementTime("1045");
		        		
		        		 
		        		 obj4.getChildren().add(obj5);
		        		 
		        		 
		        		 PublisherReport obj6 = new PublisherReport();
				         obj6.setAudience_segment("Laptop");
					     obj6.setExternalWorldCount("167");
		        		 obj6.setCount("56");
		        		 obj6.setVisitorCount("105");
		        		 obj6.setThirdpartycount("102");
		        		 obj6.setAverageTime("3");
		        		 obj6.setShare(share6.toString());
		        		 obj6.setEngagementTime("1045");
		        		
		        		 
		        		 obj5.getChildren().add(obj6);
		        		
			        
			        
		        		 pubreport1.add(obj2);
                         */

		        		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews")){
		        			    
		        	            Collections.sort(pubreport1, new Comparator<PublisherReport>() {
		        	 				
		        	 				@Override
		        	 		        public int compare(PublisherReport o1, PublisherReport o2) {
		        	 					return Double.parseDouble(o1.getCount()) > Double.parseDouble(o2.getCount()) ? -1 : (Double.parseDouble(o1.getCount()) < Double.parseDouble(o2.getCount())) ? 1 : 0;
		        	 		        }
		        	 		    });	
		        			    
		        			    }
		        			    
		        			    if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") ){
		        			    	

		        		            Collections.sort(pubreport1, new Comparator<PublisherReport>() {
		        		 				
		        		 				@Override
		        		 		        public int compare(PublisherReport o1, PublisherReport o2) {
		        		 					return Double.parseDouble(o1.getEngagementTime()) > Double.parseDouble(o2.getEngagementTime()) ? -1 : (Double.parseDouble(o1.getEngagementTime()) < Double.parseDouble(o2.getEngagementTime())) ? 1 : 0;
		        		 		        }
		        		 		    });	
		        			    	
		        			    	
		        			    }
		        			    
		        			    if(filter != null && !filter.isEmpty() && filter.equals("visitorCount") ){
		        			    	
		        	                    Collections.sort(pubreport1, new Comparator<PublisherReport>() {
		        		 				
		        		 				@Override
		        		 		        public int compare(PublisherReport o1, PublisherReport o2) {
		        		 					return Double.parseDouble(o1.getVisitorCount()) > Double.parseDouble(o2.getVisitorCount()) ? -1 : (Double.parseDouble(o1.getVisitorCount()) < Double.parseDouble(o2.getVisitorCount())) ? 1 : 0;
		        		 		        }
		        		 		    });	
		        			    	
		        			    	
		        			    }
		        		 
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
			         obj4a.setGender("Male");
				     obj4a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4a.setCount(count.toString());
	        		 obj4a.setVisitorCount(visitorCount.toString());
	        		 obj4a.setThirdpartycount(thirdpartycount.toString());
	        		 obj4a.setAverageTime("2.0");
				
	        		 obj2.getChildren().add(obj4a);
	        		 
	        		 
	        		 PublisherReport obj5a = new PublisherReport();
			         obj5a.setGender("Female");
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
			         obj4.setGender("Male");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	        		 obj4.setVisitorCount(visitorCount.toString());
	        		 obj4.setThirdpartycount(thirdpartycount.toString());
	        		 obj4.setAverageTime("2.0");
				
	        		 obj3.getChildren().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setGender("Female");
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
			         obj4a.setIncomelevel("Medium");
				     obj4a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4a.setCount(count.toString());
	        		 obj4a.setVisitorCount(visitorCount.toString());
	        		 obj4a.setThirdpartycount(thirdpartycount.toString());
	        		 obj4a.setAverageTime("1.0");
				
	        		 obj2.getChildren().add(obj4a);
	        		 
	        		 
	        		 PublisherReport obj5a = new PublisherReport();
			         obj5a.setIncomelevel("Low");
				     obj5a.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5a.setCount(count.toString());
	        		 obj5a.setVisitorCount(visitorCount.toString());
	        		 obj5a.setThirdpartycount(thirdpartycount.toString());
	        		 obj5a.setAverageTime("2.0");
	        		
	        		 obj2.getChildren().add(obj5a);
	        		 
	        		 PublisherReport obj6a = new PublisherReport();
			         obj6a.setIncomelevel("High");
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
			         obj4.setIncomelevel("Medium");
				     obj4.setExternalWorldCount(externalWorldCount.toString());
	        		 obj4.setCount(count.toString());
	        		 obj4.setVisitorCount(visitorCount.toString());
	        		 obj4.setThirdpartycount(thirdpartycount.toString());
	        		 obj4.setAverageTime("1.0");
				
	        		 obj3.getChildren().add(obj4);
	        		 
	        		 
	        		 PublisherReport obj5 = new PublisherReport();
			         obj5.setIncomelevel("Low");
				     obj5.setExternalWorldCount(externalWorldCount.toString());
	        		 obj5.setCount(count.toString());
	        		 obj5.setVisitorCount(visitorCount.toString());
	        		 obj5.setThirdpartycount(thirdpartycount.toString());
	        		 obj5.setAverageTime("1.0");
	        		
	        		 obj3.getChildren().add(obj5);
	        		 
	        		 PublisherReport obj6 = new PublisherReport();
			         obj6.setIncomelevel("High");
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



	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}



	public static List<PublisherReport > getNestedReffererJSON (List<PublisherReport> pubreport, String filter){
	   
		String url1 = "facebook.com";
		
		String url2 = "google.com";

		List<PublisherReport> pubreportv1 = new ArrayList();
		
		PublisherReport pubreport1a = new PublisherReport();
		pubreport1a.setReferrerMasterDomain(url1);

		
		PublisherReport pubreport1b = new PublisherReport();
		pubreport1b.setReferrerMasterDomain(url2);

		Integer total =0;
		Integer shares = 0;
	    Integer likes = 0;	
	
		Integer total1=0;
		Integer shares1 =0;
		Integer likes1= 0;
		
		for(int i=0; i<pubreport.size(); i++)
		{
			PublisherReport pubreport1 = pubreport.get(i);
			if(pubreport1.getReferrerMasterDomain().toLowerCase().contains("facebook"))
			{
				
				pubreport1a.getChildren().add(pubreport1);
				
			/*	
				List<PublisherReport> pubreport2 = pubreport1a.getChildren();
				for(int j=0; j< pubreport.size();i++)
				{
					if(pubreport2.get(j).getReferrerMasterDomain()==null)
					{
						pubreport2.get(j).setReferrerMasterDomain(pubreport1.getReferrerMasterDomain());
				*/		
						//  if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						//  {
							 // pubreport2.get(j).setCount(pubreport1.getCount());
						      total = total + (int) Double.parseDouble(pubreport1.getCount());
						  
						//  }
					           
						       //     if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						         //   {       	//pubreport2.get(j).setVisitorCount(pubreport1.getVisitorCount());
						         //               total = total + (int) Double.parseDouble(pubreport1.getVisitorCount());
						         //   }

						        //    if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
						         //   {	
						            	//pubreport2.get(j).setEngagementTime(pubreport1.getEngagementTime());	
						          //  	 total = total + (int) Double.parseDouble(pubreport1.getEngagementTime());
						          //  }
					
						            //pubreport2.get(j).setShares(pubreport1.getShares());
					
						            shares = shares + Integer.parseInt(pubreport1.getShares());
						             
						          //  pubreport2.get(j).setLikes(pubreport1.getLikes());
					
					                likes = likes + Integer.parseInt(pubreport1.getLikes());					
					
					             //   pubreport1a.setChildren(pubreport2);
					
					             
					
					}
					
				
		
			else if(pubreport1.getReferrerMasterDomain().toLowerCase().contains("google"))
			{
				pubreport1b.getChildren().add(pubreport1);
				
				
			///	for(int j=0; j< pubreport.size();i++)
			//	{
				//	if(pubreport3.get(j).getReferrerMasterDomain()==null)
				//	{
					//	pubreport3.get(j).setReferrerMasterDomain(pubreport1.getReferrerMasterDomain());
						
						//  if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						//  {
						//	  pubreport3.get(j).setCount(pubreport1.getCount());
						      total1 = total1 + (int) Double.parseDouble(pubreport1.getCount());
						  
						//  }
					           
						     //       if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						      //      {       	
						         //   	pubreport3.get(j).setVisitorCount(pubreport1.getVisitorCount());
						               //         total1 = total1 + (int) Double.parseDouble(pubreport1.getVisitorCount());
						        //    }

						       //     if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
						         //   {	
						          //  	pubreport3.get(j).setEngagementTime(pubreport1.getEngagementTime());	
						          //  	 total1 = total1 + (int) Double.parseDouble(pubreport1.getEngagementTime());
						          //  }
					
						       //    pubreport3.get(j).setShares(pubreport1.getShares());
					//
						           // shares = shares + Integer.parseInt(pubreport1.getShares());
						             
						       //    pubreport3.get(j).setLikes(pubreport1.getLikes());
					
					            //    likes = likes + Integer.parseInt(pubreport1.getLikes());					
					          //    pubreport1b.setChildren(pubreport3);
					          //    break;
				
				
			}
			else{
				
				
				pubreportv1.add(pubreport1);
				
				
			}
			
			
			
		}
		
	//	 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		//  {
			  pubreport1a.setCount(total.toString());
			  pubreport1b.setCount(total1.toString());
		 // }
	           
		            
		 
		// if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		 //    {       	
		            
		//	 pubreport1a.setVisitorCount(total.toString());
		//	 pubreport1b.setCount(total.toString());
		  //   }

		 
	//	if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
		  //     {	
		            	
		//	pubreport1a.setEngagementTime(total.toString());  
		//	pubreport1b.setCount(total.toString());
		       
		 //      }
		
		  pubreport1a.setShares(shares.toString());
		  pubreport1a.setLikes(likes.toString());
	
		  pubreport1b.setShares(shares1.toString());
		  pubreport1b.setLikes(likes1.toString());
	
		  pubreportv1.add(pubreport1a);
		  pubreportv1.add(pubreport1b);
		  
		  
		  Double total2 = 0.0;
	      Double share = 0.0;
	                 
	        for(int i=0;i<pubreportv1.size();i++){
	      	  
	      	 // if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
	      		  total2=total2+Double.parseDouble(pubreportv1.get(i).getCount());
	           
	              //  if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
	              //	  total=total+Double.parseDouble(pubreport.get(i).getEngagementTime());
	            
	             //   if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
	             // 	  total=total+Double.parseDouble(pubreport.get(i).getVisitorCount());
	      	  
	      	  
	        }
	                 
	         for(int i=0;i<pubreportv1.size();i++){
	      	        // if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
	      	        	 share = NestedJSON5.round((Double.parseDouble(pubreportv1.get(i).getCount())/total2)*100,2);
	      	        
	      	       //   if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
	      	        //	  share =(int) Math.round(( Double.parseDouble(pubreport.get(i).getEngagementTime())/total)*100);
	      	      
	      	       //   if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
	      	        //	  share =(int) Math.round(( Double.parseDouble(pubreport.get(i).getVisitorCount())/total)*100);
	      	
	      	  pubreportv1.get(i).setShare(share.toString());
	      	  
	        }
		  
		  return pubreportv1;
	}









}
