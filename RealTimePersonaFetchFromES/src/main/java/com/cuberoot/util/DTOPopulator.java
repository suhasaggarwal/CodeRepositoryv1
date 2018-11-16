package com.cuberoot.util;


import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.websystique.springmvc.model.Reports;


/**
 * Utility for converting ResultSets into DTO
 */
public class DTOPopulator {
    /**
     * Populate a result set into DTO
     
     */
    public static List<Reports> populateDTO(ResultSet resultSet)
            throws Exception {
       
    	   List<Reports> report = new ArrayList<Reports>();
           Reports reportDTO = null;
    	   String name;
           while (resultSet.next()) {
        	
        	Double impressions = null;
           	Double clicks = null;
           	Double mediacost = null;
           	Double conversions = null;
           	Double reach = null;  
        	       	   
        	int total_rows = resultSet.getMetaData().getColumnCount();
            
        	
        	Reports obj = new Reports();
            for (int i = 0; i < total_rows; i++) {
               name = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
              
               if( name.equals("impression")){
                obj.setImpressions(resultSet.getObject(i + 1).toString());
                impressions = Double.parseDouble(resultSet.getObject(i + 1).toString());
               }
               
               if( name.equals("date"))
            	 obj.setDate(resultSet.getObject(i + 1).toString());
            
               if( name.equals("campaign_id"))
            	  obj.setCampaign_id(resultSet.getObject(i + 1).toString());
            
              if( name.equals("channel"))   
            	   obj.setChannel(resultSet.getObject(i + 1).toString());
            
            
              if( name.equals("clicks")){
                  obj.setClicks(resultSet.getObject(i + 1).toString());
                  clicks = Double.parseDouble(resultSet.getObject(i + 1).toString());
              }
                  
                  
              if( name.equals("conversions")){
                  obj.setConversions(resultSet.getObject(i + 1).toString());
                  conversions = Double.parseDouble(resultSet.getObject(i + 1).toString());
              }
              
              
              if( name.equals("reach")){
                  obj.setReach(resultSet.getObject(i + 1).toString());              
                  reach = Double.parseDouble(resultSet.getObject(i + 1).toString());
              }
              
              
              
              
              if( name.equals("cost")){
                  obj.setCost(resultSet.getObject(i + 1).toString());
                  mediacost = Double.parseDouble(resultSet.getObject(i + 1).toString());
              }   
            
              if( name.equals("audience_segment"))
                  obj.setAudience_segment(resultSet.getObject(i + 1).toString());
              
              if( name.equals("city"))
                  obj.setCity(resultSet.getObject(i + 1).toString());
              
              if( name.equals("device_type"))
                  obj.setDevice_type(resultSet.getObject(i + 1).toString());  
            
              if( name.equals("os"))
                  obj.setOs(resultSet.getObject(i + 1).toString());  
            
              if(name.equals("age"))
                  obj.setAge(resultSet.getObject(i + 1).toString());  
              
              if(name.equals("gender"))
                  obj.setGender(resultSet.getObject(i + 1).toString());  
            

              if(impressions !=null && impressions !=0 && mediacost !=null && mediacost !=0)
                  obj.setCpm((mediacost/impressions)*1000);
               else
             	 obj.setCpm(0.0);
               
                
                if(clicks !=null && clicks != 0  && mediacost !=null && mediacost !=0)
                 obj.setCpc((mediacost/clicks));
                else
                 obj.setCpc(0.0);
             
                if(conversions !=null && conversions != 0 && mediacost !=null && mediacost !=0)
                 obj.setCPConversion((mediacost/conversions)*1000);
                else
                 obj.setCPConversion(0.0);
                 
            
                if(reach !=null && reach != 0 && mediacost !=null && mediacost !=0)
                    obj.setCpp((mediacost/reach)*1000);
                   else
                    obj.setCpp(0.0);
                    
            
                if(impressions !=null && impressions !=0 && clicks !=null && clicks !=0)
                    obj.setCtr((clicks/impressions));
                 else
               	 obj.setCtr(0.0);
            
               
                if(conversions !=null && conversions != 0 && impressions !=null && impressions !=0)
                    obj.setConvRate((conversions/impressions));
                   else
                    obj.setConvRate(0.0);
                
                
            
            }
            report.add(obj);
        
        }
        return report;
    }
    
}