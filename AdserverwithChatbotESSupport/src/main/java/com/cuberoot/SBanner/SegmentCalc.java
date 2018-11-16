package com.cuberoot.SBanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

public class SegmentCalc implements Target {
	public String[][] calcList() {
		// TODO Auto-generated method stub
		return null;
	}
	
	/** The s esclient. */
    private static Client client;
    
     /**
      * This method returns ESclient instance.
      *
      * @return the es client
      * ESClient instance
      */
	public static Client getEsClient() {

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

		SegmentCalc.client = esClient;
	}

	/**
	 * Gets the es.
	 *
	 * @return the es
	 */
	public static Client getES() {
		try {
			
			final Builder settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch");
			
			final TransportClient transportClient = new TransportClient(settings);
			transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
			
			return transportClient;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String[][] calcList(String[][] allcampinfo, String siteID, String chnlID, String fingerprintId)
	{
		//allcampinfo = (String[][]) sc.getAttribute("campinfo");
		//System.out.println(allcampinfo);
		
	//	File file = new File("/root/Segment.log");
	//	FileOutputStream fos = null;
	//	try {
	//		fos = new FileOutputStream(file);
	//	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
	//		e.printStackTrace();
	//	}
		//PrintStream ps = new PrintStream(fos);
		//System.setOut(ps);
		
		
		
		int allcamplength = allcampinfo.length;

		
	    System.out.println(fingerprintId);
	    int randomNumber = 	(int) (Math.random() * (100));	
		if (audienceSegmentEnabled == true) 
		{
			
		  if( randomNumber < 20  )
			{
			getEsClient(); 
			String campaignId = null;
			
			
			SearchHit[] fingerprintDetails1 = null;
			
			SearchHit[] fingerprintDetails = null;
			
			if(fingerprintId !=null && fingerprintId.isEmpty()==false && fingerprintId.contains(".")==false){
			
				fingerprintId.trim();
				fingerprintDetails1 = ElasticSearchAPIs.searchDocumentFingerprintIds(client,
						"enhanceduserdatabeta1", "core2",
						"mastercookie_id", fingerprintId);
				
				  String cookie_id = null;
				  if(fingerprintDetails1 != null && fingerprintDetails1.length!=0){
						for (SearchHit hit : fingerprintDetails1) {
							Map<String, Object> result = hit.getSource();
							
							cookie_id=(String) result.get("cookie_id"); 
							if(cookie_id!=null)
							break;
							}
						
						  
						
				 if(cookie_id !=null && cookie_id.isEmpty()==false){
			         cookie_id = cookie_id.trim();
					 fingerprintDetails = ElasticSearchAPIs.searchDocumentFingerprintIds(client,
								"enhanceduserdatabeta1", "core2",
								"cookie_id", cookie_id);		
				   }	
				  }	
				
				
			}
				else{
					
					String [] fingerprintSegments = null;
					
					if(fingerprintId !=null && fingerprintId.isEmpty()==false){
					fingerprintSegments = fingerprintId.split("\\.");
				    //System.out.println(fingerprintSegments[0]+":"+fingerprintSegments[1]+":"+fingerprintSegments[2]+":"+fingerprintSegments[3]);

					if(fingerprintSegments.length > 1)
					fingerprintId = fingerprintSegments[0].trim()+"_"+fingerprintSegments[1].trim()+"_"+fingerprintSegments[2].trim();
					fingerprintId = fingerprintId.trim();
					
					
					fingerprintDetails = ElasticSearchAPIs.searchDocumentFingerprintIds(client,
							"enhanceduserdatabeta1", "core2",
							"fingerprint_id", fingerprintId);
					}
					
				}
			
			
			// Get Search Hits results
			// Returns a Map, returns audience segment from the map
	   
			
			
			
					
		System.out.println("Time Decay"+"\n");	
		String audienceSegmentValue = "";
			// String audienceSegment = "";
         String latestaudienceSegment = null;
		
         
         int counter = 0;
         int flag = 0;
         HashSet<String> audienceSegment = new HashSet<String>();

             if(fingerprintDetails != null && fingerprintDetails.length!=0){
			for (SearchHit hit : fingerprintDetails) {
				System.out.println("------------------------------");
				Map<String, Object> result = hit.getSource();
				audienceSegmentValue = (String) result.get("subcategory");
				if (audienceSegmentValue != null && audienceSegmentValue.isEmpty() == false)
				audienceSegmentValue = audienceSegmentValue.trim();
				if (flag == 0 && audienceSegmentValue != null && audienceSegmentValue.isEmpty()== false){
				    
					latestaudienceSegment = audienceSegmentValue;
				    flag = 1;
				}
				if (audienceSegmentValue != null && audienceSegmentValue.isEmpty() == false
						&& audienceSegment.contains(audienceSegmentValue) == false) {
					audienceSegment.add(audienceSegmentValue);
					if (audienceSegment.size() == 3)
						break;
				}
				 System.out.println(audienceSegment);
                 counter++;
			}
            
			
			// populate set
			SearchHit[] campaignDetailsTopPriority = null;
			SearchHit[] campaignDetails = null;
			SearchHit[] campaignDetails1 = null;
			SearchHit[] campaignDetails2 = null;

			int tracker = 0;
			String [][]selectedcampinfo = new String[allcamplength][21];
			int count1 = 0;
			HashMap<String, Integer> campaignIdCount = new HashMap<String, Integer>();
			HashSet<String> validCampaigns = new HashSet<String>();
			HashSet<String> campaign = new HashSet<String>();
			HashSet<String> campaign1 = new HashSet<String>();
			HashSet<String> campaign2 = new HashSet<String>();

			ArrayList topCampaign = new ArrayList();
			ArrayList validCampaigns1 = new ArrayList();
			ArrayList validCampaigns2 = new ArrayList();
			ArrayList validCampaigns3 = new ArrayList();

			
			if(latestaudienceSegment !=null && !latestaudienceSegment.isEmpty()){
			campaignDetailsTopPriority = ElasticSearchAPIs
					.searchDocumentCampaignIds(client,
							"campaignkeywordindex", "core2",
							"audience_segment",latestaudienceSegment);
			}
			
			if (campaignDetailsTopPriority  != null) {
				for (SearchHit hit : campaignDetailsTopPriority) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					topCampaign.add(campaignId);
				}
			}
			
			String campaignId1 = null;		
			
			if (audienceSegment != null && audienceSegment.isEmpty() == false) {
				for (String s : audienceSegment) {
					System.out.println(s);				
					
					if (count1 == 0)
						campaignDetails = ElasticSearchAPIs
								.searchDocumentCampaignIds(client,
										"campaignkeywordindex", "core2",
										"audience_segment", s);

					if (count1 == 1)
						campaignDetails1 = ElasticSearchAPIs
								.searchDocumentCampaignIds(client,
										"campaignkeywordindex", "core2",
										"audience_segment", s);

					if (count1 == 2) {
						campaignDetails2 = ElasticSearchAPIs
								.searchDocumentCampaignIds(client,
										"campaignkeywordindex", "core2",
										"audience_segment", s);
						break;
					}
					count1++;

				}

			}

				
			if (campaignDetails != null) {
				for (SearchHit hit : campaignDetails) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					campaign.add(campaignId);
				}
			}
			
			
			
			if (campaignDetails1 != null) {
				for (SearchHit hit : campaignDetails1) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					campaign1.add(campaignId);
				}
			}

			if (campaignDetails2 != null) {
				for (SearchHit hit : campaignDetails2) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					campaign2.add(campaignId);

				}
			}

			if (campaign != null && !campaign.isEmpty()) {
				for (String campaign_Id : campaign) {

					campaignIdCount.put(campaign_Id, 1);
				}
			}

			if (campaign1 != null && !campaign1.isEmpty()) {
				for (String campaign_Id : campaign1) {

					if (campaignIdCount.containsKey(campaign_Id))
						campaignIdCount.put(campaign_Id, 2);
					else
						campaignIdCount.put(campaign_Id, 1);
				}
			}

			if (campaign2 != null && !campaign2.isEmpty()) {

				for (String campaign_Id : campaign2) {

					if (campaignIdCount.containsKey(campaign_Id)) {
						int countValue = campaignIdCount.get(campaign_Id);
						campaignIdCount.put(campaign_Id, countValue + 1);
					}

					else {
						campaignIdCount.put(campaign_Id, 1);

					}
				}
			}

			
			if(campaignIdCount !=null && !campaignIdCount.isEmpty() ){
			for (Map.Entry<String, Integer> campaignIdCount1 : campaignIdCount
					.entrySet()) {
				System.out.println("Key = " + campaignIdCount1.getKey()
						+ ", Value = " + campaignIdCount1.getValue());
				if (campaignIdCount1.getValue() == 3)
					validCampaigns3.add(campaignIdCount1.getKey());
				if (campaignIdCount1.getValue() == 2)
					validCampaigns2.add(campaignIdCount1.getKey());
				if (campaignIdCount1.getValue() == 1)
					validCampaigns1.add(campaignIdCount1.getKey());

			 }
			}
			
			
			if (validCampaigns3 != null && !validCampaigns3.isEmpty()){
			for(int i=0; i < validCampaigns3.size(); i++){
			
				campaignId1 = (String) validCampaigns3.get(i);

			if (campaignId1 != null && !campaignId1.isEmpty()) {
				for (int x1 = 0; x1 < allcamplength; x1++) {

					if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
							(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
							&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][31].equals("1")) {
						selectedcampinfo[tracker][0] = allcampinfo[x1][0];
						selectedcampinfo[tracker][1] = allcampinfo[x1][1];
						selectedcampinfo[tracker][2] = allcampinfo[x1][2];
						selectedcampinfo[tracker][3] = allcampinfo[x1][3];
						selectedcampinfo[tracker][4] = allcampinfo[x1][4];
						selectedcampinfo[tracker][5] = allcampinfo[x1][5];
						selectedcampinfo[tracker][6] = allcampinfo[x1][6];
						selectedcampinfo[tracker][7] = allcampinfo[x1][7];
						selectedcampinfo[tracker][8] = allcampinfo[x1][8];
						selectedcampinfo[tracker][9] = allcampinfo[x1][9];
						selectedcampinfo[tracker][10] = allcampinfo[x1][10];
						selectedcampinfo[tracker][11] = allcampinfo[x1][11];
						selectedcampinfo[tracker][12] = allcampinfo[x1][12];
						selectedcampinfo[tracker][13] = allcampinfo[x1][13];
						selectedcampinfo[tracker][14] = allcampinfo[x1][14];
						tracker++;
						System.out.println("All three matching campaigns: "+campaignId1);
						
						break;
					}

					

				}
			 }
			}
		}
			if (tracker > 0)
				return selectedcampinfo;
			
			tracker = 0;

			if (topCampaign != null && !topCampaign.isEmpty()){
			for (int j =0; j < topCampaign.size(); j++){
			
				campaignId1 = (String) topCampaign.get(j);

			if (campaignId1 != null && !campaignId1.isEmpty()) {
				for (int x1 = 0; x1 < allcamplength; x1++) {

					if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
							(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
							&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][31].equals("1")) {
						selectedcampinfo[tracker][0] = allcampinfo[x1][0];
						selectedcampinfo[tracker][1] = allcampinfo[x1][1];
						selectedcampinfo[tracker][2] = allcampinfo[x1][2];
						selectedcampinfo[tracker][3] = allcampinfo[x1][3];
						selectedcampinfo[tracker][4] = allcampinfo[x1][4];
						selectedcampinfo[tracker][5] = allcampinfo[x1][5];
						selectedcampinfo[tracker][6] = allcampinfo[x1][6];
						selectedcampinfo[tracker][7] = allcampinfo[x1][7];
						selectedcampinfo[tracker][8] = allcampinfo[x1][8];
						selectedcampinfo[tracker][9] = allcampinfo[x1][9];
						selectedcampinfo[tracker][10] = allcampinfo[x1][10];
						selectedcampinfo[tracker][11] = allcampinfo[x1][11];
						selectedcampinfo[tracker][12] = allcampinfo[x1][12];
						selectedcampinfo[tracker][13] = allcampinfo[x1][13];
						selectedcampinfo[tracker][14] = allcampinfo[x1][14];
						tracker++;
						System.out.println("Top Campaign: "+campaignId1);
						break;
					}

					

				}
			}
		   }
		}		
			if (tracker > 0)
				return selectedcampinfo;
						
			tracker = 0;

			
			
			if (validCampaigns2 != null && !validCampaigns2.isEmpty()){
			for (int k =0; k < validCampaigns2.size(); k++){
			
				campaignId1 = (String) validCampaigns2.get(k);

			if (campaignId1 != null && !campaignId1.isEmpty()) {
				for (int x1 = 0; x1 < allcamplength; x1++) {

					if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
							(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
							&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][31].equals("1")) {
						selectedcampinfo[tracker][0] = allcampinfo[x1][0];
						selectedcampinfo[tracker][1] = allcampinfo[x1][1];
						selectedcampinfo[tracker][2] = allcampinfo[x1][2];
						selectedcampinfo[tracker][3] = allcampinfo[x1][3];
						selectedcampinfo[tracker][4] = allcampinfo[x1][4];
						selectedcampinfo[tracker][5] = allcampinfo[x1][5];
						selectedcampinfo[tracker][6] = allcampinfo[x1][6];
						selectedcampinfo[tracker][7] = allcampinfo[x1][7];
						selectedcampinfo[tracker][8] = allcampinfo[x1][8];
						selectedcampinfo[tracker][9] = allcampinfo[x1][9];
						selectedcampinfo[tracker][10] = allcampinfo[x1][10];
						selectedcampinfo[tracker][11] = allcampinfo[x1][11];
						selectedcampinfo[tracker][12] = allcampinfo[x1][12];
						selectedcampinfo[tracker][13] = allcampinfo[x1][13];
						selectedcampinfo[tracker][14] = allcampinfo[x1][14];
						tracker++;
						System.out.println("2 matching Campaign: "+campaignId1);
						
						break;
					}

					

				}

				// Convert allcampinfo to specific campaign data returned by
				// elastic search, it can give a mixture of campaignIds with
				// geo targeting enabled or disabled.
			}
				
			}
		 }
			if (tracker > 0)
				return selectedcampinfo;

			
			tracker = 0;
			
			
		if (validCampaigns1 != null && !validCampaigns1.isEmpty()){
			for (int l =0; l < validCampaigns1.size(); l++){
			
			
			campaignId1 = (String) validCampaigns1.get(l);

			

			if (campaignId1 != null && !campaignId1.isEmpty()) {
				for (int x1 = 0; x1 < allcamplength; x1++) {

					if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
							(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
							&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][31].equals("1")) {
						selectedcampinfo[tracker][0] = allcampinfo[x1][0];
						selectedcampinfo[tracker][1] = allcampinfo[x1][1];
						selectedcampinfo[tracker][2] = allcampinfo[x1][2];
						selectedcampinfo[tracker][3] = allcampinfo[x1][3];
						selectedcampinfo[tracker][4] = allcampinfo[x1][4];
						selectedcampinfo[tracker][5] = allcampinfo[x1][5];
						selectedcampinfo[tracker][6] = allcampinfo[x1][6];
						selectedcampinfo[tracker][7] = allcampinfo[x1][7];
						selectedcampinfo[tracker][8] = allcampinfo[x1][8];
						selectedcampinfo[tracker][9] = allcampinfo[x1][9];
						selectedcampinfo[tracker][10] = allcampinfo[x1][10];
						selectedcampinfo[tracker][11] = allcampinfo[x1][11];
						selectedcampinfo[tracker][12] = allcampinfo[x1][12];
						selectedcampinfo[tracker][13] = allcampinfo[x1][13];
						selectedcampinfo[tracker][14] = allcampinfo[x1][14];
						tracker++;
						System.out.println("1 matching Campaign: "+campaignId1);
						
						break;
					}

					

				}

				// Convert allcampinfo to specific campaign data returned by
				// elastic search, it can give a mixture of campaignIds with
				// geo targeting enabled or disabled.

			}
		  }
		}	
			if (tracker > 0)
				return selectedcampinfo;

             }	
			
             int tracker1 = 0;
 			 String [][]selectedcampinfo1 = new String[allcamplength][21];
			
			
			for (int x1 = 0; x1 < allcamplength; x1++) {

				if (allcampinfo[x1][4] != null 
						&& allcampinfo[x1][0] != null) {	
				
				
				if ((allcampinfo[x1][31] == null && allcampinfo[x1][4].equals(chnlID)
								&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].isEmpty() && allcampinfo[x1][4].equals(chnlID)
										&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].equals("0") && allcampinfo[x1][4].equals(chnlID)
												&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].equals("1") && allcampinfo[x1][32] == null && allcampinfo[x1][4].equals(chnlID)
														&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].equals("1") && allcampinfo[x1][32].isEmpty() && allcampinfo[x1][4].equals(chnlID)
																&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].equals("1") && allcampinfo[x1][32].equals("0") && allcampinfo[x1][4].equals(chnlID)
														&& allcampinfo[x1][0].equals(siteID)) ) {
					selectedcampinfo1[tracker1][0] = allcampinfo[x1][0];
					selectedcampinfo1[tracker1][1] = allcampinfo[x1][1];
					selectedcampinfo1[tracker1][2] = allcampinfo[x1][2];
					selectedcampinfo1[tracker1][3] = allcampinfo[x1][3];
					selectedcampinfo1[tracker1][4] = allcampinfo[x1][4];
					selectedcampinfo1[tracker1][5] = allcampinfo[x1][5];
					selectedcampinfo1[tracker1][6] = allcampinfo[x1][6];
					selectedcampinfo1[tracker1][7] = allcampinfo[x1][7];
					selectedcampinfo1[tracker1][8] = allcampinfo[x1][8];
					selectedcampinfo1[tracker1][9] = allcampinfo[x1][9];
					selectedcampinfo1[tracker1][10] = allcampinfo[x1][10];
					selectedcampinfo1[tracker1][11] = allcampinfo[x1][11];
					selectedcampinfo1[tracker1][12] = allcampinfo[x1][12];
					selectedcampinfo1[tracker1][13] = allcampinfo[x1][13];
					selectedcampinfo1[tracker1][14] = allcampinfo[x1][14];
					tracker1++;
					System.out.println("Rotation Mode Campaigns : "+allcampinfo[x1][1] + "Is Match Campaigns : "+allcampinfo[x1][32]);
					
				  }

				}	
			  }	
			
		      if (tracker1 > 0)
			 return selectedcampinfo1;
			
             tracker1 = 0;
             
             
		
			}

	 else {
				 
			getEsClient();
			String campaignId = null;
            SearchHit[] fingerprintDetails1 = null;
			
			SearchHit[] fingerprintDetails = null;
			
			if(fingerprintId !=null && fingerprintId.isEmpty()==false && fingerprintId.contains(".")==false){
				
				fingerprintId = fingerprintId.trim();
				
				fingerprintDetails1 = ElasticSearchAPIs.searchDocumentFingerprintIds(client,
						"enhanceduserdatabeta1", "core2",
						"mastercookie_id", fingerprintId);
				
				  String cookie_id = null;
				  if(fingerprintDetails1 != null && fingerprintDetails1.length!=0){
						for (SearchHit hit : fingerprintDetails1) {
							Map<String, Object> result = hit.getSource();
							
							cookie_id=(String) result.get("cookie_id"); 
							if(cookie_id!=null)
							break;
							}
						
						  
						
				    if(cookie_id !=null && cookie_id.isEmpty()==false){	
			             cookie_id = cookie_id.trim();
				    	fingerprintDetails = ElasticSearchAPIs.searchDocumentFingerprintIds(client,
								"enhanceduserdatabeta1", "core2",
								"cookie_id", cookie_id);		
				    }
				  }	
				
				
			}
				else{
					
					String [] fingerprintSegments = null;
					
					if(fingerprintId !=null && fingerprintId.isEmpty()==false){	
					fingerprintSegments = fingerprintId.split("\\.");
				    //System.out.println(fingerprintSegments[0]+":"+fingerprintSegments[1]+":"+fingerprintSegments[2]+":"+fingerprintSegments[3]);

					if(fingerprintSegments.length > 1)
					fingerprintId = fingerprintSegments[0].trim()+"_"+fingerprintSegments[1].trim()+"_"+fingerprintSegments[2].trim();
					fingerprintId = fingerprintId.trim();	
					
						
						
						fingerprintDetails = ElasticSearchAPIs.searchDocumentFingerprintIds(client,
							"enhanceduserdatabeta1", "core2",
							"fingerprint_id", fingerprintId);
					
				  }	
				}
			
			// Get Search Hits results
			// Returns a Map, returns audience segment from the map
			
			System.out.println("Frequency Module"+"\n");
			String audienceSegmentValue = "";
			// String audienceSegment = "";
            String latestaudienceSegment = null;
            String mostFrequentAudienceSegment = null;
			String timestamp = "";
			HashMap<String,Integer> audienceSegmentFrequent = new HashMap<String,Integer>();
			HashSet<String> latestfrequentaudienceSegments = CategoryAPIs.getLatestCategory(fingerprintDetails);
			Set<Frequency> set = new TreeSet<Frequency>();
            Date date = null;
		    Integer counter1 = 0; 
			Integer count = 1;
			Integer value = 0;
			if(fingerprintDetails != null && fingerprintDetails.length!=0){
			for (SearchHit hit : fingerprintDetails) {
				System.out.println("------------------------------");
				Map<String, Object> result = hit.getSource();
				audienceSegmentValue = (String) result.get("subcategory");
				if (audienceSegmentValue != null && audienceSegmentValue.isEmpty() == false)
				audienceSegmentValue = audienceSegmentValue.trim();
				if(audienceSegmentValue != null && !audienceSegmentValue.isEmpty()){
     			if(audienceSegmentFrequent.containsKey(audienceSegmentValue) == false){
					audienceSegmentFrequent.put(audienceSegmentValue,1);    
					System.out.println(result.get("request_time")+"\n");
					System.out.println("Adding Value First Time"+"\n");
     			}
     			 else{
						 
     				     count=audienceSegmentFrequent.get(audienceSegmentValue);	
     				     audienceSegmentFrequent.put(audienceSegmentValue,count+1);	
     				     value=count+1;
     				     System.out.println(result.get("request_time")+"\n");
					     System.out.println("Added Count to field count:"+value+":"+ audienceSegmentValue+"\n");

     			 
     			    }
				}
			 }
				
			// System.out.println(audienceSegment);
			
			
			
			if(audienceSegmentFrequent != null && !audienceSegmentFrequent.isEmpty()){
			Iterator it = audienceSegmentFrequent.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        System.out.println(pair.getKey() + " = " + pair.getValue());
		        set.add(new Frequency((String)pair.getKey(),(Integer)pair.getValue()));
		        it.remove(); // avoids a ConcurrentModificationException
		     }
			}
		    LinkedHashSet<String> audienceSegment = new LinkedHashSet<String>();
		    ArrayList<String> mostFrequentSegments= new ArrayList<String>();
		    
		    if(set !=null && !set.isEmpty()){
		    int counter = 0;
		    int maxFrequency=0;
		    for (Frequency f : set) {
		        if(counter == 0){
		         latestaudienceSegment = f.getName();
		    	 maxFrequency = f.getFreq();
		    	}
		       
             //Get All campaigns having maximum frequency
		        
		        if(f.getFreq() == maxFrequency)
		        	mostFrequentSegments.add(f.getName());
		        
		        if(counter == 3)
		        	break;
		        audienceSegment.add(f.getName());
		        
		        counter++;
		    	System.out.println(f);
		      }
		    }

			// populate set
			SearchHit[] campaignDetails = null;
			SearchHit[] campaignDetails1 = null;
			SearchHit[] campaignDetails2 = null;

			int tracker = 0;
			String [][]selectedcampinfo = new String[allcamplength][21];
			int count1 = 0;
			HashMap<String, Integer> campaignIdCount = new HashMap<String, Integer>();
			HashSet<String> validCampaigns = new HashSet<String>();
			HashSet<String> campaign = new HashSet<String>();
			HashSet<String> campaign1 = new HashSet<String>();
			HashSet<String> campaign2 = new HashSet<String>();
            ArrayList<String> latestFrequentCategories = new ArrayList<String>();
            ArrayList<String> mostFrequentCategories = new ArrayList<String>();
            
			ArrayList topCampaign = new ArrayList();
			ArrayList frequentCampaign = new ArrayList();
			ArrayList validCampaigns1 = new ArrayList();
			ArrayList validCampaigns2 = new ArrayList();
			ArrayList validCampaigns3 = new ArrayList();

			SearchHit[] campaignDetailsTopPriority = null;
			SearchHit[] campaignDetailsMostFrequent = null;

			for(int i=0; i < mostFrequentSegments.size(); i++)
			{
				if(latestfrequentaudienceSegments.contains(mostFrequentSegments.get(i)))
					latestFrequentCategories.add(mostFrequentSegments.get(i));
			        mostFrequentCategories.add(mostFrequentSegments.get(i));
			   
			}
			
			int index = (int) (Math.random() * (latestFrequentCategories.size()));
			if (latestFrequentCategories != null && !latestFrequentCategories.isEmpty())
				latestaudienceSegment = (String)latestFrequentCategories.get(index);
			
			
			
			
			
			
			if(latestaudienceSegment !=null && !latestaudienceSegment.isEmpty()){			
			campaignDetailsTopPriority = ElasticSearchAPIs
					.searchDocumentCampaignIds(client,
							"campaignkeywordindex", "core2",
							"audience_segment",latestaudienceSegment);
			}
	//Display campaign that is top priority i.e whose category is most frequent and latest.		
			
			if (campaignDetailsTopPriority  != null) {
				for (SearchHit hit : campaignDetailsTopPriority) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					topCampaign.add(campaignId);
				    System.out.println("topcampaign"+topCampaign);
				
				}
			}
			
		
			index = (int) (Math.random() * (mostFrequentCategories.size()));
			if (mostFrequentCategories != null && !mostFrequentCategories.isEmpty())
				mostFrequentAudienceSegment = (String)mostFrequentCategories.get(index);
			
			if(mostFrequentAudienceSegment !=null && !mostFrequentAudienceSegment.isEmpty()){		
			campaignDetailsMostFrequent = ElasticSearchAPIs
					.searchDocumentCampaignIds(client,
							"campaignkeywordindex", "core2",
							"audience_segment",mostFrequentAudienceSegment);
			}
			
			if (campaignDetailsMostFrequent  != null) {
				for (SearchHit hit : campaignDetailsMostFrequent) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					frequentCampaign.add(campaignId);
					System.out.println("frequentcampaign:"+ frequentCampaign);
				
				}
			}
			
			
			
			
			String campaignId1 = null;
			
			if (audienceSegment != null && audienceSegment.isEmpty()==false) {
				for (String s : audienceSegment) {
					System.out.println(s);
			
					if (count1 == 0)
						campaignDetails = ElasticSearchAPIs
								.searchDocumentCampaignIds(client,
										"campaignkeywordindex", "core2",
										"audience_segment", s);

					if (count1 == 1)
						campaignDetails1 = ElasticSearchAPIs
								.searchDocumentCampaignIds(client,
										"campaignkeywordindex", "core2",
										"audience_segment", s);

					if (count1 == 2) {
						campaignDetails2 = ElasticSearchAPIs
								.searchDocumentCampaignIds(client,
										"campaignkeywordindex", "core2",
										"audience_segment", s);
						break;
					}
					count1++;

				}

			}

			if (campaignDetails != null) {
				for (SearchHit hit : campaignDetails) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					campaign.add(campaignId);
				}
			}
			
			
			if (campaignDetails1 != null) {
				for (SearchHit hit : campaignDetails1) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					campaign1.add(campaignId);
				}
			}

			if (campaignDetails2 != null) {
				for (SearchHit hit : campaignDetails2) {
					System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();
					campaignId = (String) result.get("campaign_id");
					campaign2.add(campaignId);

				}
			}

			if (campaign != null && !campaign.isEmpty()) {
				for (String campaign_Id : campaign) {

					campaignIdCount.put(campaign_Id, 1);
				}
			}

			if (campaign1 != null && !campaign1.isEmpty()) {
				for (String campaign_Id : campaign1) {

					if (campaignIdCount.containsKey(campaign_Id))
						campaignIdCount.put(campaign_Id, 2);
					else
						campaignIdCount.put(campaign_Id, 1);
				}
			}

			if (campaign2 != null && !campaign2.isEmpty()) {

				for (String campaign_Id : campaign2) {

					if (campaignIdCount.containsKey(campaign_Id)) {
						int countValue = campaignIdCount.get(campaign_Id);
						campaignIdCount.put(campaign_Id, countValue + 1);
					}

					else {
						campaignIdCount.put(campaign_Id, 1);

					}
				}
			}

			if(campaignIdCount !=null && !campaignIdCount.isEmpty()){
			for (Map.Entry<String, Integer> campaignIdCount1 : campaignIdCount
					.entrySet()) {
				System.out.println("Key = " + campaignIdCount1.getKey()
						+ ", Value = " + campaignIdCount1.getValue());
				if (campaignIdCount1.getValue() == 3)
					validCampaigns3.add(campaignIdCount1.getKey());
				if (campaignIdCount1.getValue() == 2)
					validCampaigns2.add(campaignIdCount1.getKey());
				if (campaignIdCount1.getValue() == 1)
					validCampaigns1.add(campaignIdCount1.getKey());

		    	}
			}
			
			if(latestfrequentaudienceSegments.contains(latestaudienceSegment))
			{
				
				if (topCampaign != null && !topCampaign.isEmpty()){
				for(int i=0; i < topCampaign.size(); i++){
					
						campaignId1 = (String) topCampaign.get(i);

					if (campaignId1 != null && !campaignId1.isEmpty()) {
						for (int x1 = 0; x1 < allcamplength; x1++) {

							if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
									(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
									&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][31].equals("1")) {
								selectedcampinfo[tracker][0] = allcampinfo[x1][0];
								selectedcampinfo[tracker][1] = allcampinfo[x1][1];
								selectedcampinfo[tracker][2] = allcampinfo[x1][2];
								selectedcampinfo[tracker][3] = allcampinfo[x1][3];
								selectedcampinfo[tracker][4] = allcampinfo[x1][4];
								selectedcampinfo[tracker][5] = allcampinfo[x1][5];
								selectedcampinfo[tracker][6] = allcampinfo[x1][6];
								selectedcampinfo[tracker][7] = allcampinfo[x1][7];
								selectedcampinfo[tracker][8] = allcampinfo[x1][8];
								selectedcampinfo[tracker][9] = allcampinfo[x1][9];
								selectedcampinfo[tracker][10] = allcampinfo[x1][10];
								selectedcampinfo[tracker][11] = allcampinfo[x1][11];
								selectedcampinfo[tracker][12] = allcampinfo[x1][12];
								selectedcampinfo[tracker][13] = allcampinfo[x1][13];
								selectedcampinfo[tracker][14] = allcampinfo[x1][14];
								tracker++;
								
								System.out.println("Top Campaign: " +campaignId1);
								break;
							}

							

						}
					}
				  }
				}
			}
					if (tracker > 0)
						return selectedcampinfo;
					
					
			
				     tracker = 0;

			
			
		if (validCampaigns3 != null && !validCampaigns3.isEmpty()){
		    for(int j=0; j < validCampaigns3.size(); j++){
		
				campaignId1 = (String) validCampaigns3.get(j);

			if (campaignId1 != null && !campaignId1.isEmpty()) {
				for (int x1 = 0; x1 < allcamplength; x1++) {

					if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
							(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
							&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][31].equals("1")) {
						selectedcampinfo[tracker][0] = allcampinfo[x1][0];
						selectedcampinfo[tracker][1] = allcampinfo[x1][1];
						selectedcampinfo[tracker][2] = allcampinfo[x1][2];
						selectedcampinfo[tracker][3] = allcampinfo[x1][3];
						selectedcampinfo[tracker][4] = allcampinfo[x1][4];
						selectedcampinfo[tracker][5] = allcampinfo[x1][5];
						selectedcampinfo[tracker][6] = allcampinfo[x1][6];
						selectedcampinfo[tracker][7] = allcampinfo[x1][7];
						selectedcampinfo[tracker][8] = allcampinfo[x1][8];
						selectedcampinfo[tracker][9] = allcampinfo[x1][9];
						selectedcampinfo[tracker][10] = allcampinfo[x1][10];
						selectedcampinfo[tracker][11] = allcampinfo[x1][11];
						selectedcampinfo[tracker][12] = allcampinfo[x1][12];
						selectedcampinfo[tracker][13] = allcampinfo[x1][13];
						selectedcampinfo[tracker][14] = allcampinfo[x1][14];
						tracker++;
						System.out.println("All three Matching Categories: " +campaignId1);
						
						break;
					
					}
				}
			  }
		    }
		}	
			if (tracker > 0)
				return selectedcampinfo;
              
			   tracker = 0;
			
			   if (frequentCampaign != null && !frequentCampaign.isEmpty()){
			   
				   for(int k=0; k<frequentCampaign.size(); k++){
			
				campaignId1 = (String) frequentCampaign.get(k);

			if (campaignId1 != null && !campaignId1.isEmpty()) {
				for (int x1 = 0; x1 < allcamplength; x1++) {

					if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
							(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
							&& allcampinfo[x1][0].equals(siteID)  && allcampinfo[x1][31].equals("1")) {
						selectedcampinfo[tracker][0] = allcampinfo[x1][0];
						selectedcampinfo[tracker][1] = allcampinfo[x1][1];
						selectedcampinfo[tracker][2] = allcampinfo[x1][2];
						selectedcampinfo[tracker][3] = allcampinfo[x1][3];
						selectedcampinfo[tracker][4] = allcampinfo[x1][4];
						selectedcampinfo[tracker][5] = allcampinfo[x1][5];
						selectedcampinfo[tracker][6] = allcampinfo[x1][6];
						selectedcampinfo[tracker][7] = allcampinfo[x1][7];
						selectedcampinfo[tracker][8] = allcampinfo[x1][8];
						selectedcampinfo[tracker][9] = allcampinfo[x1][9];
						selectedcampinfo[tracker][10] = allcampinfo[x1][10];
						selectedcampinfo[tracker][11] = allcampinfo[x1][11];
						selectedcampinfo[tracker][12] = allcampinfo[x1][12];
						selectedcampinfo[tracker][13] = allcampinfo[x1][13];
						selectedcampinfo[tracker][14] = allcampinfo[x1][14];
						tracker++;
						System.out.println("Frequent Campaign: " + campaignId1);
						
						break;
					}

					

				}
			}
	     }
	  }
				if (tracker > 0)
				return selectedcampinfo;
			
			
			
		     tracker = 0;

		     if (validCampaigns2 != null && !validCampaigns2.isEmpty()){
		     
			for(int l=0; l < validCampaigns2.size(); l++){
			
				campaignId1 = (String) validCampaigns2.get(l);

			if (campaignId1 != null && !campaignId1.isEmpty()) {
				for (int x1 = 0; x1 < allcamplength; x1++) {

					if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
							(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
							&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][31].equals("1")) {
						selectedcampinfo[tracker][0] = allcampinfo[x1][0];
						selectedcampinfo[tracker][1] = allcampinfo[x1][1];
						selectedcampinfo[tracker][2] = allcampinfo[x1][2];
						selectedcampinfo[tracker][3] = allcampinfo[x1][3];
						selectedcampinfo[tracker][4] = allcampinfo[x1][4];
						selectedcampinfo[tracker][5] = allcampinfo[x1][5];
						selectedcampinfo[tracker][6] = allcampinfo[x1][6];
						selectedcampinfo[tracker][7] = allcampinfo[x1][7];
						selectedcampinfo[tracker][8] = allcampinfo[x1][8];
						selectedcampinfo[tracker][9] = allcampinfo[x1][9];
						selectedcampinfo[tracker][10] = allcampinfo[x1][10];
						selectedcampinfo[tracker][11] = allcampinfo[x1][11];
						selectedcampinfo[tracker][12] = allcampinfo[x1][12];
						selectedcampinfo[tracker][13] = allcampinfo[x1][13];
						selectedcampinfo[tracker][14] = allcampinfo[x1][14];
						tracker++;
						System.out.println("2 Matching campaigns: "+campaignId1);

						break;
					}
				}

				// Convert allcampinfo to specific campaign data returned by
				// elastic search, it can give a mixture of campaignIds with
				// geo targeting enabled or disabled.

			 }
			}
		
		  }	
			if (tracker > 0)
				return selectedcampinfo;

			tracker =0;
			
		if (validCampaigns1 != null && !validCampaigns1.isEmpty()){	
			for(int m=0; m < validCampaigns1.size(); m++){
			
				campaignId1 = (String) validCampaigns1.get(m);

			

			if (campaignId1 != null && !campaignId1.isEmpty()) {
				for (int x1 = 0; x1 < allcamplength; x1++) {

					if (allcampinfo[x1][1] != null && campaignId1.trim().equalsIgnoreCase(
							(allcampinfo[x1][1]).trim()) && allcampinfo[x1][4].equals(chnlID)
							&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][31].equals("1")) {
						selectedcampinfo[tracker][0] = allcampinfo[x1][0];
						selectedcampinfo[tracker][1] = allcampinfo[x1][1];
						selectedcampinfo[tracker][2] = allcampinfo[x1][2];
						selectedcampinfo[tracker][3] = allcampinfo[x1][3];
						selectedcampinfo[tracker][4] = allcampinfo[x1][4];
						selectedcampinfo[tracker][5] = allcampinfo[x1][5];
						selectedcampinfo[tracker][6] = allcampinfo[x1][6];
						selectedcampinfo[tracker][7] = allcampinfo[x1][7];
						selectedcampinfo[tracker][8] = allcampinfo[x1][8];
						selectedcampinfo[tracker][9] = allcampinfo[x1][9];
						selectedcampinfo[tracker][10] = allcampinfo[x1][10];
						selectedcampinfo[tracker][11] = allcampinfo[x1][11];
						selectedcampinfo[tracker][12] = allcampinfo[x1][12];
						selectedcampinfo[tracker][13] = allcampinfo[x1][13];
						selectedcampinfo[tracker][14] = allcampinfo[x1][14];
						tracker++;
						System.out.println("1 Matching Campaign:"+campaignId1);
						break;
						
					}

					

				}

				// Convert allcampinfo to specific campaign data returned by
				// elastic search, it can give a mixture of campaignIds with
			}	// geo targeting enabled or disabled.

			}
			
		}	
			if(tracker > 0)
				return selectedcampinfo;
			
			}	
			
            int tracker1 = 0;
			String [][]selectedcampinfo1 = new String[allcamplength][21];
	 
			for (int x1 = 0; x1 < allcamplength; x1++) {

				if (allcampinfo[x1][4] != null 
						&& allcampinfo[x1][0] != null) {	
				
					if ((allcampinfo[x1][31] == null && allcampinfo[x1][4].equals(chnlID)
									&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].isEmpty() && allcampinfo[x1][4].equals(chnlID)
											&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].equals("0") && allcampinfo[x1][4].equals(chnlID)
													&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].equals("1") && allcampinfo[x1][32] == null && allcampinfo[x1][4].equals(chnlID)
															&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].equals("1") && allcampinfo[x1][32].isEmpty() && allcampinfo[x1][4].equals(chnlID)
																	&& allcampinfo[x1][0].equals(siteID)) || (allcampinfo[x1][31].equals("1") && allcampinfo[x1][32].equals("0") && allcampinfo[x1][4].equals(chnlID)
															&& allcampinfo[x1][0].equals(siteID)) ) {
					selectedcampinfo1[tracker1][0] = allcampinfo[x1][0];
					selectedcampinfo1[tracker1][1] = allcampinfo[x1][1];
					selectedcampinfo1[tracker1][2] = allcampinfo[x1][2];
					selectedcampinfo1[tracker1][3] = allcampinfo[x1][3];
					selectedcampinfo1[tracker1][4] = allcampinfo[x1][4];
					selectedcampinfo1[tracker1][5] = allcampinfo[x1][5];
					selectedcampinfo1[tracker1][6] = allcampinfo[x1][6];
					selectedcampinfo1[tracker1][7] = allcampinfo[x1][7];
					selectedcampinfo1[tracker1][8] = allcampinfo[x1][8];
					selectedcampinfo1[tracker1][9] = allcampinfo[x1][9];
					selectedcampinfo1[tracker1][10] = allcampinfo[x1][10];
					selectedcampinfo1[tracker1][11] = allcampinfo[x1][11];
					selectedcampinfo1[tracker1][12] = allcampinfo[x1][12];
					selectedcampinfo1[tracker1][13] = allcampinfo[x1][13];
					selectedcampinfo1[tracker1][14] = allcampinfo[x1][14];
					tracker1++;
					System.out.println("Rotation Mode Campaigns : "+allcampinfo[x1][1] + "Is Match Campaigns : "+allcampinfo[x1][32]);
					
					}
				}
				
				}	
			
		     if (tracker1 > 0)
			 return selectedcampinfo1;
		} 
 	  }
		
		return allcampinfo;
	}
	
	private boolean audienceSegmentEnabled = GlobalConfiguration.getBoolean("segmentenabled");


}
