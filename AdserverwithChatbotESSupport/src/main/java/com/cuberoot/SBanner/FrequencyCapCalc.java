package com.cuberoot.SBanner;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sourceforge.wurfl.core.Device;



public class FrequencyCapCalc {

public String[][] calcList(String[][] allcampinfo, String siteID, String chnlID, String fingerprintId) throws ParseException {
		
	System.out.println(fingerprintId);	
	EhCacheKeyCodeRepository ehcache = EhCacheKeyCodeRepository.getInstance();
    String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    List<String> results  = ehcache.get(fingerprintId,false);
	Map<String,Integer> campaignFrequencyList = new HashMap<String,Integer>();
    
	    String starttimestamp = "";
	    String endtimestamp = "";
	
	    int allcamplength=allcampinfo.length;
        String [][] mycampinfo=new String[allcamplength][45];
        String [][] mobilecampinfo=new String[allcamplength][35];
        String [][] desktopcampinfo =new String[allcamplength][35];
        int mcicnt1=0;
        int mcicnt2=0;
        int mcicnt3=0;
        String [] parts;
        String eligiblecmpId = "";
        String cachedResult = "";
        int cap = 0;
        String dateRange = "";
        Integer timedelta = 0;
        Integer count =0;
       List<String> targetcmpId = new ArrayList<String>();
       
        for (int x1 = 0; x1 < allcamplength; x1++) {

			
			if(allcampinfo[x1][4] != null
					&& allcampinfo[x1][0] != null){
			
			if (allcampinfo[x1][4].equals(chnlID)
					&& allcampinfo[x1][0].equals(siteID)) {
        
				
				     cap =  Integer.parseInt(allcampinfo[x1][36]);
				     timedelta = Integer.parseInt(allcampinfo[x1][37]);		
					 Integer minutes= timedelta;
					 if(results!=null){
							DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							Calendar cal = Calendar.getInstance();
							endtimestamp = sdf.format(cal.getTime()).toString();

							cal.add(Calendar.MINUTE, -minutes);
							starttimestamp = sdf.format(cal.getTime()).toString();
							dateRange = starttimestamp + "_" + endtimestamp;
							dateRange = dateRange.replace("/", "-");

							
							for(int i=0; i<results.size(); i++){
								
								
							 cachedResult = results.get(i);	
							 
							 parts = cachedResult.split("@");	
							
							 SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

							 Date date1 = format.parse(starttimestamp);
							 Date date2 = format.parse(endtimestamp);
                             Date date3 = format.parse(parts[1]); 
							 
                             if(parts[2].equals(allcampinfo[x1][1])){
                             
							 if (date3.compareTo(date2) <= 0 && date3.compareTo(date1) >= 0) {
							     
							 
								
							    	if(campaignFrequencyList.get(parts[2])==null)
							    	campaignFrequencyList.put(parts[2],1); 
							    	else
							    	{
							    		count = campaignFrequencyList.get(parts[2]);
							    		campaignFrequencyList.put(parts[2],count+1);
							    	    
							    	}
							        count=0;  
							   }		  
                             }		   
						}
							
						}	
						
			      }		
			}				 
        }	
			
			
                                    for (int x1 = 0; x1 < allcamplength; x1++) {

			
			                if(allcampinfo[x1][4] != null && allcampinfo[x1][0] != null){
			
			if (allcampinfo[x1][4].equals(chnlID)
					&& allcampinfo[x1][0].equals(siteID)) {
        
				 cap =  Integer.parseInt(allcampinfo[x1][36]);
			
			             if(campaignFrequencyList.get(allcampinfo[x1][1])==null || campaignFrequencyList.get(allcampinfo[x1][1]) < cap){
									
								 targetcmpId.add(allcampinfo[x1][1]);
						
							}
				
			
					 }
					 
					
				 
				 
					 
			 }
			
			}		
         		 
				 	
        for (Map.Entry entry : campaignFrequencyList.entrySet()) {
            System.out.println(entry.getKey() + ", " + entry.getValue());
        }
        
        for (int x1 = 0; x1 < allcamplength; x1++) {

			
			    System.out.println("Frequency Not Filled Matching CampaignId:"+targetcmpId);
        
				
				 if(targetcmpId.contains(allcampinfo[x1][1])){
					
					    mycampinfo[mcicnt1][0] = allcampinfo[x1][0];
			            mycampinfo[mcicnt1][1] = allcampinfo[x1][1];                 
			            mycampinfo[mcicnt1][2] = allcampinfo[x1][2];                   
			            mycampinfo[mcicnt1][3] = allcampinfo[x1][3];                   
			            mycampinfo[mcicnt1][4] = allcampinfo[x1][4];
			            mycampinfo[mcicnt1][5] = allcampinfo[x1][5]; 
			            mycampinfo[mcicnt1][6] = allcampinfo[x1][6];
			            mycampinfo[mcicnt1][7] = allcampinfo[x1][7]; 
			            mycampinfo[mcicnt1][8] = allcampinfo[x1][8]; 
			            mycampinfo[mcicnt1][9] = allcampinfo[x1][9]; 
			            mycampinfo[mcicnt1][10] = allcampinfo[x1][10];
			            mycampinfo[mcicnt1][11] = allcampinfo[x1][11];
			            
			            mycampinfo[mcicnt1][12] = allcampinfo[x1][12];
			            mycampinfo[mcicnt1][13] = allcampinfo[x1][13];
			            mycampinfo[mcicnt1][14] = allcampinfo[x1][14];
			            mycampinfo[mcicnt1][15] = allcampinfo[x1][15];
			            mycampinfo[mcicnt1][16] = allcampinfo[x1][16];
			            mycampinfo[mcicnt1][18] = allcampinfo[x1][18];
			            mycampinfo[mcicnt1][19] = allcampinfo[x1][19];
			            mycampinfo[mcicnt1][20] = allcampinfo[x1][20];
				        mycampinfo[mcicnt1][21] = allcampinfo[x1][21];
				        mycampinfo[mcicnt1][22] = allcampinfo[x1][22];
				        mycampinfo[mcicnt1][23] = allcampinfo[x1][23];
				        mycampinfo[mcicnt1][24] = allcampinfo[x1][24];
				        mycampinfo[mcicnt1][25] = allcampinfo[x1][25];
				        mycampinfo[mcicnt1][26] = allcampinfo[x1][26];
				        mycampinfo[mcicnt1][27] = allcampinfo[x1][27];
				        mycampinfo[mcicnt1][28] = allcampinfo[x1][28];
				        mycampinfo[mcicnt1][29] = allcampinfo[x1][29];
				        mycampinfo[mcicnt1][30] = allcampinfo[x1][30];
				        mycampinfo[mcicnt1][31] = allcampinfo[x1][31];
				        mycampinfo[mcicnt1][32] = allcampinfo[x1][32];
				        mycampinfo[mcicnt1][33] = allcampinfo[x1][33];
				        mycampinfo[mcicnt1][34] = allcampinfo[x1][34];
				        mycampinfo[mcicnt1][35] = allcampinfo[x1][35];
					
					    mcicnt1++;
					
			
				}
			
					
        }		 
return mycampinfo;

}		
	
	
	
	
}
