package com.cuberoot.SBanner;

import net.sourceforge.wurfl.core.Device;

public class GetTargetingList {

	 public boolean audienceSegmentEnabled = GlobalConfiguration.getBoolean("segmentenabled");
	 public boolean GeoEnabled = GlobalConfiguration.getBoolean("geoenabled");  
     public boolean deviceEnabled = GlobalConfiguration.getBoolean("deviceenabled");
	 public boolean demographicsEnabled = GlobalConfiguration.getBoolean("demographicsenabled");
     public boolean retargetEnabled = GlobalConfiguration.getBoolean("retargetEnabled");
	 public boolean cpmtargetenabled = GlobalConfiguration.getBoolean("cpmtargetEnabled");
	 public boolean freqcapenabled =  GlobalConfiguration.getBoolean("freqcapEnabled");
	 
	 
	public String[][] getList(String allcampinfo[][],String siteID, String chnlID, GeoInfo ipc, Device device1, boolean MR, String userAgent, String fingerprintId){
		String mycampinfo[][] = new String[allcampinfo.length][36];
		mycampinfo = allcampinfo;
		String[][] geoList = new String[allcampinfo.length][36];
		String[][] deviceList = new String[allcampinfo.length][36];
		String[][] segList = new String[allcampinfo.length][36];
		String[][] demographicsList = new String[allcampinfo.length][36]; 
		String[][] reTargList = new String[allcampinfo.length][36]; 
        String[][] cpmTargList = new String[allcampinfo.length][36];
		String[][] fcapTargList = new String[allcampinfo.length][36];

        System.out.println("Inside GetTargetingList.");
		
		if(GeoEnabled){
		GeoCalc geoObj = new GeoCalc();
		try{
		geoList = geoObj.calcList(mycampinfo, siteID, chnlID, ipc, MR);
		}
		catch(Exception e){
			
			e.printStackTrace();
			geoList = allcampinfo;
			
		}
		}
		else{
			geoList = allcampinfo;
		}
		
		System.out.println("Get geoList Done. Length of geoList is :"+geoList.length +"\n" );
		
		if(deviceEnabled){
		DeviceCalc deviceObj = new DeviceCalc();
		
		try{
		deviceList = deviceObj.calcList(geoList, siteID, chnlID, device1,userAgent); 
		}
		catch(Exception e){
		
			 e.printStackTrace();
			 deviceList = geoList;
		}
		
		}
		else{
			deviceList = geoList;
		}
		System.out.println("Get deviceList Done.\n");
		
		
		
		if(demographicsEnabled){
			DemographicsCalc demoObj = new DemographicsCalc();
			try{
			demographicsList = demoObj.calcList(deviceList, siteID, chnlID, fingerprintId);
			//String[][] segList = deviceList;
			}
			catch(Exception e)
			{
				
				e.printStackTrace();
				demographicsList = deviceList;
			}
			}
			else{
				
			  demographicsList = deviceList;
			}
		
		
		System.out.println("Get DemographicsList Done.\n");
		
		
		
		if(audienceSegmentEnabled){
			SegmentCalc segObj = new SegmentCalc();
			try{
			segList = segObj.calcList(demographicsList, siteID, chnlID, fingerprintId);
			//String[][] segList = deviceList;
			}
			catch(Exception e)
			{
			 e.printStackTrace();
			 segList = demographicsList;
			}
			}
			else{
				
				segList = demographicsList;
			}
			System.out.println("Get SegmentList Done.\n");
				
		System.out.println("GetTargetingList Done. Returning from get targeting list module.\n");
		
		//String[][] finalList = merge(deviceList,segList);
		
		

		if(retargetEnabled){
			ReTargetCalc retargObj = new ReTargetCalc();
			try{
			reTargList = retargObj.calcList(allcampinfo, siteID, chnlID, fingerprintId);
			//String[][] segList = deviceList;
			if(reTargList!=null)
			return reTargList;
			}
			catch(Exception e)
			{
			 e.printStackTrace();
			 reTargList = segList;
			}
			}
			else{
				
				reTargList = segList;
			}
			System.out.println("Get ReTargList Done.\n");
				
		
			if(freqcapenabled){
				System.out.println("In Frequency Calc.\n"); 
				 FrequencyCapCalc fcapobj = new FrequencyCapCalc();
				 try{
					 fcapTargList  = fcapobj.calcList(geoList, siteID, chnlID, fingerprintId);
				 }
				 catch(Exception e)
					{
					 e.printStackTrace();
					 fcapTargList = segList;
					}
				 
				// return fcapTargList;
			 }	
			
			
			
		 if(cpmtargetenabled){
			 System.out.println("In CPM Target.\n"); 
			 CPMTargetCalc cpmObj = new CPMTargetCalc();
			 try{
				 cpmTargList = cpmObj.calcList(mycampinfo, siteID, chnlID, device1);
			 }
			 catch(Exception e)
				{
				 e.printStackTrace();
				 cpmTargList = segList;
				}
			 
			 return cpmTargList;
		 }
						
			System.out.println("GetTargetingList Done. Returning from get targeting list module.\n");
		
		
		
		
		
		
		
		return segList;
	}

	/*
	private String[][] merge(String[][] deviceList, String[][] segList) {
		// TODO Auto-generated method stub
		int camplen = deviceList.length;
		int camplen1 = segList.length;
		//String[] col = new String[camplen];
		ArrayList<String> col = new ArrayList<String>(); 
		for(int i = 0; i < camplen; i++){
			col.add(deviceList[i][1]);
		}
		int tracker = 0;
		String[][] newList =new String[camplen + camplen1][21];
		for(int i = 0;i < camplen; i++){
				newList[tracker][0] = deviceList[i][0];
				newList[tracker][1] = deviceList[i][1];
				newList[tracker][2] = deviceList[i][2];
				newList[tracker][3] = deviceList[i][3];
				newList[tracker][4] = deviceList[i][4];
				newList[tracker][5] = deviceList[i][5];
				newList[tracker][6] = deviceList[i][6];
				newList[tracker][7] = deviceList[i][7];
				newList[tracker][8] = deviceList[i][8];
				newList[tracker][9] = deviceList[i][9];
				newList[tracker][10] = deviceList[i][10];
				newList[tracker][11] = deviceList[i][11];
				newList[tracker][12] = deviceList[i][12];
				newList[tracker][13] = deviceList[i][13];
				newList[tracker][14] = deviceList[i][14];
				
				tracker++;
		}
		
		for(int i = 0;i < camplen1;i++){
			if(!(col.contains(segList[i][0]))){
					System.out.println("Val :"+segList[i][0]);
					newList[tracker][0] = segList[i][0];
					newList[tracker][1] = segList[i][1];
					newList[tracker][2] = segList[i][2];
					newList[tracker][3] = segList[i][3];
					newList[tracker][4] = segList[i][4];
					newList[tracker][5] = segList[i][5];
					newList[tracker][6] = segList[i][6];
					newList[tracker][7] = segList[i][7];
					newList[tracker][8] = segList[i][8];
					newList[tracker][9] = segList[i][9];
					newList[tracker][10] = segList[i][10];
					newList[tracker][11] = segList[i][11];
					newList[tracker][12] = segList[i][12];
					newList[tracker][13] = segList[i][13];
					newList[tracker][14] = segList[i][14];
		
					tracker++;
					}
		}
		
		System.out.println("Total No of campaigns. : "+tracker);
		return newList;
	}
	*/
}
