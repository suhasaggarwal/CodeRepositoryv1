package com.cuberoot.SBanner;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class ReTargetCalc {



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

		ReTargetCalc.client = esClient;
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
		
        int allcamplength = allcampinfo.length;

		Set<String> channelNames = new HashSet<String>();
	    Set<String> campaignIds = new HashSet<String>();
		
		System.out.println(fingerprintId);
	   
		
	    String [][]selectedcampinfo = new String[allcamplength][21];
		
	    int tracker = 0;
	    
		if (reTargetEnabled == true) 
		{
			
		 
			getEsClient(); 
			String campaignId = null;
			
			
			SearchHit[] fingerprintDetails1 = null;
			
		
			
		    System.out.println(fingerprintId);
		   	
				
				
				SearchHit[] fingerprintDetails = null;
				
				if(fingerprintId !=null && fingerprintId.isEmpty()==false && fingerprintId.contains(".")==false){
				
					fingerprintId.trim();
					fingerprintDetails1 = ElasticSearchAPIs.searchDocumentFingerprintIds(client,
							"enhanceduserdatabeta1", "core2",
							"mastercookie_id", fingerprintId);
					
					  String channel_name = null;
					  if(fingerprintDetails1 != null && fingerprintDetails1.length!=0){
							for (SearchHit hit : fingerprintDetails1) {
								Map<String, Object> result = hit.getSource();
								
								channel_name=(String)result.get("channel_name"); 
								if(channel_name !=null && channel_name.isEmpty()==false)
								channelNames.add(channel_name.trim());
								}
						}	
			
				
				  }	

				if(channelNames !=null && channelNames.isEmpty()==false){
				  for(String channelName : channelNames){
					  
		
					  for (int x1 = 0; x1 < allcamplength; x1++) {

							if (allcampinfo[x1][1] != null && channelName.trim().equalsIgnoreCase(
									(allcampinfo[x1][33]).trim()) && allcampinfo[x1][4].equals(chnlID)
									&& allcampinfo[x1][0].equals(siteID) && allcampinfo[x1][34].equals("1")) {
								
								if(campaignIds.contains(allcampinfo[x1][1])==false){
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
								campaignIds.add(allcampinfo[x1][1]);
								tracker++;
								System.out.println("RetargetMatch: " +channelName+ ":"+allcampinfo[x1][1]);
								}
								
								
							
							}
						}
					  
					  
				  }
			 }
		
		     if(tracker > 0)
		    	 return selectedcampinfo;
		
		
		
		
		
		}


		return null;
		
	}





	private boolean reTargetEnabled = GlobalConfiguration.getBoolean("retargetEnabled");



}
