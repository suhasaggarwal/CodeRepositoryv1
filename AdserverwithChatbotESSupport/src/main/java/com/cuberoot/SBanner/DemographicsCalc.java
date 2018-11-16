package com.cuberoot.SBanner;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;



public class DemographicsCalc {

	
	/** The s esclient. */
    private static Client client;
    
     /**
      * This method returns ESclient instance.
      *
      * @return the es client
      * ESClient instance
      */
	public final Client getEsClient() {

		if (client == null) {
			client = getES();
		}
		return client;
	}

	/**
	 * Sets the es client.
	 *
	 * @param esClient the new es client
	 */
	public final void setEsClient(final Client esClient) {

		DemographicsCalc.client = esClient;
	}

	/**
	 * Gets the es.
	 *
	 * @return the es
	 */
	private Client getES() {
		try {
			
			final Builder settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch");
			
			final TransportClient transportClient = new TransportClient(settings);
			transportClient.addTransportAddress(new InetSocketTransportAddress("172.16.101.132", 9300));
			
			return transportClient;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	
	
	

	public  String[][] calcList(String[][] allcampinfo,String siteID, String chnlID, String fingerprintId)
	{
		
		System.out.println(allcampinfo);
		allcamplength = allcampinfo.length;

		genderenabledCampaigns = new String[allcamplength][35];
		agegroupenabledCampaigns = new String[allcamplength][35];
		incomeLevelEnabledCampaigns = new String[allcamplength][35];
		matchCampaigns = new String[allcamplength][35];
		
		String [] fingerprintSegments = fingerprintId.split("\\.");
	//	System.out.println(fingerprintSegments[0]+":"+fingerprintSegments[1]+":"+fingerprintSegments[2]+":"+fingerprintSegments[3]);
	    if(fingerprintSegments.length > 1)
		fingerprintId = fingerprintSegments[0].trim()+"."+fingerprintSegments[1].trim()+"."+fingerprintSegments[2].trim();
		fingerprintId = fingerprintId.trim();
	    System.out.println(fingerprintId);
	    int randomNumber = 	(int) (Math.random() * (100));	
	    String genderValue = null;
	    String ageGroupValue = null;
	    String incomeValue = null;
	    int tracker = 0;
	    int tracker1 = 0;
	    int tracker2 = 0;
	    int tracker3 = 0;
	    
		if (demographicsEnabled == true) {
			
			
				
			getEsClient(); 
			String campaignId = null;
			
	/*		
			try {
				Class.forName("org.elasticsearch.search.internal.InternalSearchHit");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	*/		
			
		//	SearchHit[] fingerprintDetails = new SearchHit[100];
			SearchHit[]	fingerprintDetails = ElasticSearchAPIs1
					.searchDocumentFingerprintIds(client,
							"enhanceduserdatabeta1", "core2",
							"cookie_id", fingerprintId);
			// Get Search Hits results
			// Returns a Map, returns audience segment from the map
	

		if(fingerprintDetails!=null && fingerprintDetails.length!=0)	
		{	
			
	    System.out.println("Match Demographics Parameters"+"\n");	
		
		for (SearchHit hit : fingerprintDetails) {
		Map<String, Object> result = hit.getSource();
		
		if(genderValue == null || genderValue.isEmpty() == true)
		genderValue = (String) result.get("gender");
		
		if(ageGroupValue == null || ageGroupValue.isEmpty() == true)
		ageGroupValue = (String) result.get("agegroup");
		
		
		if(incomeValue == null || incomeValue.isEmpty() == true)
		incomeValue = (String) result.get("incomelevel");
		
		} 
        
		List genderCampaigns = new ArrayList<Integer>();
		List ageGroupCampaigns = new ArrayList<Integer>();
		List incomeValueCampaigns = new ArrayList<Integer>();	
	
		
		int campMatch1 = 0;
		int campMatch2 = 0;
		int campMatch3 = 0;
		int campMatch4 = 0;
				
		
		Integer pathmatch1 = 0;
		Integer pathmatch2 = 0;
		Integer pathmatch3 =0;
		
		String pathMatch = null;

     //Gender based, AgeGroup Based, Income Level Match
		
			for (int x1 = 0; x1 < allcamplength; x1++) {

				
				if(allcampinfo[x1][4] != null
						&& allcampinfo[x1][0] != null){
				
				if (allcampinfo[x1][4].equals(chnlID)
						&& allcampinfo[x1][0].equals(siteID)) {

				
					if (allcampinfo[x1][28] != null && allcampinfo[x1][28].isEmpty()==false){
					
					if (allcampinfo[x1][28].equals("1")) {

						if ((allcampinfo[x1][24]).toLowerCase().contains(genderValue.toLowerCase())) {

							campMatch1 = 1;
                            
						}
						else
							pathmatch1 = 1;
					}
       
					else
						campMatch1 = 1;
					
				}
				else
					campMatch1 = 1;	
					
					
				
		if (allcampinfo[x1][29] != null && allcampinfo[x1][29].isEmpty()==false){  
				 
				if (allcampinfo[x1][29].equals("1")) {
					  
						String[] ageGroupValues = allcampinfo[x1][25].split(",");
						
						
					for(int i= 0; i < ageGroupValues.length; i++){	
						if ((allcampinfo[x1][25]).toLowerCase().contains(ageGroupValues[i].toLowerCase())) {

							campMatch2 = 1;
                            
						}
						
						   
					}
					
					 if(campMatch2 == 0)
							pathmatch2 = 1;
				
				
				
				     }

					else
						campMatch2 = 1;

		          }
		    else
			campMatch2 = 1;	
			
				
		if (allcampinfo[x1][30] != null && allcampinfo[x1][30].isEmpty()==false){ 	
					
			       if (allcampinfo[x1][30].equals("1")) {

						if ((allcampinfo[x1][26]).toLowerCase().contains(incomeValue.toLowerCase())) {

							campMatch3 = 1;
                            
						}
						else
							pathmatch3 = 1;
					
			       } else
						campMatch3 = 1;

		        }

		    else
			campMatch3 = 1;
 
	
		
		 pathMatch  = pathmatch1.toString()+ pathmatch2.toString()+pathmatch3.toString();
		
		//Combine path match here, concatenate three path match variables
		 
				//All demographics
				
				if ( allcampinfo[x1][27] == null ||  allcampinfo[x1][27].isEmpty()==true || allcampinfo[x1][27].equals("0")) {

					campMatch4 = 1;

				}
		
				
				 
				  
				if(segmentEnabled == false)
				
				{
				
					if(pathMatch.equals("000")==true)
					{
						
						matchCampaigns[tracker][0] = allcampinfo[x1][0];
						matchCampaigns[tracker][1] = allcampinfo[x1][1];
						matchCampaigns[tracker][2] = allcampinfo[x1][2];
						matchCampaigns[tracker][3] = allcampinfo[x1][3];
						matchCampaigns[tracker][4] = allcampinfo[x1][4];
						matchCampaigns[tracker][5] = allcampinfo[x1][5];
						matchCampaigns[tracker][6] = allcampinfo[x1][6];
						matchCampaigns[tracker][7] = allcampinfo[x1][7];
						matchCampaigns[tracker][8] = allcampinfo[x1][8];
						matchCampaigns[tracker][9] = allcampinfo[x1][9];
						matchCampaigns[tracker][10] = allcampinfo[x1][10];
						matchCampaigns[tracker][11] = allcampinfo[x1][11];
						matchCampaigns[tracker][12] = allcampinfo[x1][12];
						matchCampaigns[tracker][13] = allcampinfo[x1][13];
						matchCampaigns[tracker][14] = allcampinfo[x1][14];    
						matchCampaigns[tracker][15] = allcampinfo[x1][15];
						matchCampaigns[tracker][16] = allcampinfo[x1][16];
						matchCampaigns[tracker][17] = allcampinfo[x1][17];
						matchCampaigns[tracker][18] = allcampinfo[x1][18];
						matchCampaigns[tracker][19] = allcampinfo[x1][19];
						matchCampaigns[tracker][20] = allcampinfo[x1][20];
						matchCampaigns[tracker][21] = allcampinfo[x1][21];
						matchCampaigns[tracker][22] = allcampinfo[x1][22];
						matchCampaigns[tracker][23] = allcampinfo[x1][23];
						matchCampaigns[tracker][24] = allcampinfo[x1][24];
						matchCampaigns[tracker][25] = allcampinfo[x1][25];
						matchCampaigns[tracker][26] = allcampinfo[x1][26];
						matchCampaigns[tracker][27] = allcampinfo[x1][27];
						matchCampaigns[tracker][28] = allcampinfo[x1][28];
						matchCampaigns[tracker][29] = allcampinfo[x1][29];
						matchCampaigns[tracker][30] = allcampinfo[x1][30];
						matchCampaigns[tracker][31] = allcampinfo[x1][31];
						matchCampaigns[tracker][32] = allcampinfo[x1][32];
						matchCampaigns[tracker][33] = allcampinfo[x1][33];
						matchCampaigns[tracker][34] = allcampinfo[x1][34];
						matchCampaigns[tracker][35] = allcampinfo[x1][35];
						tracker++;
						
						
					}
					
					
			/*		
							
					if( (campMatch1 == 1 &&  campMatch2 == 1 &&  campMatch3 == 1)){
						 
					matchCampaigns[tracker][0] = allcampinfo[x1][0];
					matchCampaigns[tracker][1] = allcampinfo[x1][1];
					matchCampaigns[tracker][2] = allcampinfo[x1][2];
					matchCampaigns[tracker][3] = allcampinfo[x1][3];
					matchCampaigns[tracker][4] = allcampinfo[x1][4];
					matchCampaigns[tracker][5] = allcampinfo[x1][5];
					matchCampaigns[tracker][6] = allcampinfo[x1][6];
					matchCampaigns[tracker][7] = allcampinfo[x1][7];
					matchCampaigns[tracker][8] = allcampinfo[x1][8];
					matchCampaigns[tracker][9] = allcampinfo[x1][9];
					matchCampaigns[tracker][10] = allcampinfo[x1][10];
					matchCampaigns[tracker][11] = allcampinfo[x1][11];
					matchCampaigns[tracker][12] = allcampinfo[x1][12];
					matchCampaigns[tracker][13] = allcampinfo[x1][13];
					matchCampaigns[tracker][14] = allcampinfo[x1][14];    
					matchCampaigns[tracker][15] = allcampinfo[x1][15];
					matchCampaigns[tracker][16] = allcampinfo[x1][16];
					matchCampaigns[tracker][17] = allcampinfo[x1][17];
					matchCampaigns[tracker][18] = allcampinfo[x1][18];
					matchCampaigns[tracker][19] = allcampinfo[x1][19];
					matchCampaigns[tracker][20] = allcampinfo[x1][20];
					matchCampaigns[tracker][21] = allcampinfo[x1][21];
					matchCampaigns[tracker][22] = allcampinfo[x1][22];
					matchCampaigns[tracker][23] = allcampinfo[x1][23];
					matchCampaigns[tracker][24] = allcampinfo[x1][24];
					matchCampaigns[tracker][25] = allcampinfo[x1][25];
					matchCampaigns[tracker][26] = allcampinfo[x1][26];
					matchCampaigns[tracker][27] = allcampinfo[x1][27];
					matchCampaigns[tracker][28] = allcampinfo[x1][28];
					matchCampaigns[tracker][29] = allcampinfo[x1][29];
					matchCampaigns[tracker][30] = allcampinfo[x1][30];
					matchCampaigns[tracker][31] = allcampinfo[x1][31];
					tracker++;
					
				  }
				
				//System.out.println(campaignId1);
				else{
					
			*/		
					if( (campMatch4 == 1)){
						 
						matchCampaigns[tracker2][0] = allcampinfo[x1][0];
						matchCampaigns[tracker2][1] = allcampinfo[x1][1];
						matchCampaigns[tracker2][2] = allcampinfo[x1][2];
						matchCampaigns[tracker2][3] = allcampinfo[x1][3];
						matchCampaigns[tracker2][4] = allcampinfo[x1][4];
						matchCampaigns[tracker2][5] = allcampinfo[x1][5];
						matchCampaigns[tracker2][6] = allcampinfo[x1][6];
						matchCampaigns[tracker2][7] = allcampinfo[x1][7];
						matchCampaigns[tracker2][8] = allcampinfo[x1][8];
						matchCampaigns[tracker2][9] = allcampinfo[x1][9];
						matchCampaigns[tracker2][10] = allcampinfo[x1][10];
						matchCampaigns[tracker2][11] = allcampinfo[x1][11];
						matchCampaigns[tracker2][12] = allcampinfo[x1][12];
						matchCampaigns[tracker2][13] = allcampinfo[x1][13];
						matchCampaigns[tracker2][14] = allcampinfo[x1][14];    
						matchCampaigns[tracker2][15] = allcampinfo[x1][15];
						matchCampaigns[tracker2][16] = allcampinfo[x1][16];
						matchCampaigns[tracker2][17] = allcampinfo[x1][17];
						matchCampaigns[tracker2][18] = allcampinfo[x1][18];
						matchCampaigns[tracker2][19] = allcampinfo[x1][19];
						matchCampaigns[tracker2][20] = allcampinfo[x1][20];
						matchCampaigns[tracker2][21] = allcampinfo[x1][21];
						matchCampaigns[tracker2][22] = allcampinfo[x1][22];
						matchCampaigns[tracker2][23] = allcampinfo[x1][23];
						matchCampaigns[tracker2][24] = allcampinfo[x1][24];
						matchCampaigns[tracker2][25] = allcampinfo[x1][25];
						matchCampaigns[tracker2][26] = allcampinfo[x1][26];
						matchCampaigns[tracker2][27] = allcampinfo[x1][27];
						matchCampaigns[tracker2][28] = allcampinfo[x1][28];
						matchCampaigns[tracker2][29] = allcampinfo[x1][29];
						matchCampaigns[tracker2][30] = allcampinfo[x1][30];
						matchCampaigns[tracker2][31] = allcampinfo[x1][31];
						matchCampaigns[tracker2][32] = allcampinfo[x1][32];
						matchCampaigns[tracker2][33] = allcampinfo[x1][33];
						matchCampaigns[tracker2][34] = allcampinfo[x1][34];
						matchCampaigns[tracker2][35] = allcampinfo[x1][35];
						tracker2++;
						
					  }
					
	            //  }
				}
				
				if(segmentEnabled == true)
					
				{
					if( (campMatch1 == 1 &&  campMatch2 == 1 &&  campMatch3 == 1) || campMatch4 ==1){
						 
						matchCampaigns[tracker1][0] = allcampinfo[x1][0];
						matchCampaigns[tracker1][1] = allcampinfo[x1][1];
						matchCampaigns[tracker1][2] = allcampinfo[x1][2];
						matchCampaigns[tracker1][3] = allcampinfo[x1][3];
						matchCampaigns[tracker1][4] = allcampinfo[x1][4];
						matchCampaigns[tracker1][5] = allcampinfo[x1][5];
						matchCampaigns[tracker1][6] = allcampinfo[x1][6];
						matchCampaigns[tracker1][7] = allcampinfo[x1][7];
						matchCampaigns[tracker1][8] = allcampinfo[x1][8];
						matchCampaigns[tracker1][9] = allcampinfo[x1][9];
						matchCampaigns[tracker1][10] = allcampinfo[x1][10];
						matchCampaigns[tracker1][11] = allcampinfo[x1][11];
						matchCampaigns[tracker1][12] = allcampinfo[x1][12];
						matchCampaigns[tracker1][13] = allcampinfo[x1][13];
						matchCampaigns[tracker1][14] = allcampinfo[x1][14];    
						matchCampaigns[tracker1][15] = allcampinfo[x1][15];
						matchCampaigns[tracker1][16] = allcampinfo[x1][16];
						matchCampaigns[tracker1][17] = allcampinfo[x1][17];
						matchCampaigns[tracker1][18] = allcampinfo[x1][18];
						matchCampaigns[tracker1][19] = allcampinfo[x1][19];
						matchCampaigns[tracker1][20] = allcampinfo[x1][20];
						matchCampaigns[tracker1][21] = allcampinfo[x1][21];
						matchCampaigns[tracker1][22] = allcampinfo[x1][22];
						matchCampaigns[tracker1][23] = allcampinfo[x1][23];
						matchCampaigns[tracker1][24] = allcampinfo[x1][24];
						matchCampaigns[tracker1][25] = allcampinfo[x1][25];
						matchCampaigns[tracker1][26] = allcampinfo[x1][26];
						matchCampaigns[tracker1][27] = allcampinfo[x1][27];
						matchCampaigns[tracker1][28] = allcampinfo[x1][28];
						matchCampaigns[tracker1][29] = allcampinfo[x1][29];
						matchCampaigns[tracker1][30] = allcampinfo[x1][30];
						matchCampaigns[tracker1][31] = allcampinfo[x1][31];
						matchCampaigns[tracker1][32] = allcampinfo[x1][32];
						matchCampaigns[tracker1][33] = allcampinfo[x1][33];
						matchCampaigns[tracker1][34] = allcampinfo[x1][34];
						matchCampaigns[tracker1][35] = allcampinfo[x1][35];
						tracker1++;
						
				}
			  }
		    }
		
		  }
	    }		
		     if (tracker > 0)
			 return matchCampaigns;
		     
		     if (tracker1 > 0)
		     return matchCampaigns;
		
		     if (tracker2 > 0)
		     return matchCampaigns;
		
		
		}	
		
		}
		
			
       return allcampinfo;
			
	
	
	}
	


	private String allcampinfo[][] = null;
	private int allcamplength = 0;
	private String mycampinfo[][] = null;
	private String selectedcampinfo[][] = null;
	private String genderenabledCampaigns[][]=null;
	private String agegroupenabledCampaigns[][] = null;
	private String incomeLevelEnabledCampaigns[][] = null;
	private String matchCampaigns[][] = null; 

	private boolean demographicsEnabled = GlobalConfiguration.getBoolean("demographicsenabled");
    private boolean segmentEnabled = GlobalConfiguration.getBoolean("segmentenabled");
}
