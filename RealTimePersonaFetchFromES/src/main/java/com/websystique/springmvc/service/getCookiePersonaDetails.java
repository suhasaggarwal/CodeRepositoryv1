package com.websystique.springmvc.service;



import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

import com.google.gson.Gson;
import com.websystique.springmvc.model.Pair;
import com.websystique.springmvc.model.UserProfile;




/**
 * Derive Categories
 * Takes Text as Parameter
 * 
 * 
 * 
 */

public class getCookiePersonaDetails {
	private static final long serialVersionUID = 1L;
	public static Map<String,String> citycodeMap;
	public static Map<String,String> citycodeMap2;
	public static Map<String,String> countrymap;
	public static Map<String,List<String>> countrystatemap;
	public static Map<String,List<String>> countrystatecitymap;
	public static Map<String,String> citylatlongMap1;
	
	static {
	      
	      String csvFilev1 = "/root/citylatlong.csv";
	      BufferedReader brv1 = null;
	      String linev1 = "";
	      String cvsSplitByv1 = ",";
	      String citykeyv1 = "";
	      Map<String, String> citylatlongMap2  = new HashMap<String,String>();
	      try {

	          brv1 = new BufferedReader(new FileReader(csvFilev1));
	         
	          while ((linev1 = brv1.readLine()) != null) {

	             try{
	          	// use comma as separator
	              linev1 = linev1.replace(",,",", , ");
	          	//   System.out.println(line);
	          	String[] cityDetailsv1 = linev1.split(cvsSplitByv1);
	              citykeyv1 = cityDetailsv1[1];
	              citylatlongMap2.put(citykeyv1,cityDetailsv1[5]+","+cityDetailsv1[6]);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	            
	             }
	             catch(Exception e)
	             {
	          	     
	            	 e.printStackTrace(); 
	                 continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      citylatlongMap1 = Collections.unmodifiableMap(citylatlongMap2);  
	  
	      //    System.out.println(citycodeMap);
	  }
	  
	  
	  
	  static {
	      Map<String, String> countryMap1 = new HashMap<String,String>();
	      String csvFile = "/root/countrycodes.csv";
	      BufferedReader br = null;
	      String line = "";
	      String cvsSplitBy = ",";
	      String countrykey = "";
	      Map<String, String> countryMap2  = new HashMap<String,String>();
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	         
	          while ((line = br.readLine()) != null) {

	             try{
	          	// use comma as separator
	              line = line.replace(",,",", , ");
	          	//   System.out.println(line);
	          	String[] countryDetails = line.split(cvsSplitBy);
	              countrykey = countryDetails[0];
	              countryMap1.put(countrykey,countryDetails[1]);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	            
	             }
	             catch(Exception e)
	             {
	          	     
	            	 e.printStackTrace(); 
	                 continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      countrymap = Collections.unmodifiableMap(countryMap1);  
	  
	      //    System.out.println(citycodeMap);
	  }
	  
	  
	  
	  static {
	      Map<String, List<String>> countrystateMap1 = new HashMap<String,List<String>>();
	      String csvFile1 = "/root/statecodes.csv";
	      BufferedReader br1 = null;
	      String line1 = "";
	      String cvsSplitBy1 = ",";
	      String countrykey1 = "";
	      Map<String, String> countrystateMap2  = new HashMap<String,String>();
	      List<String> list1 = new ArrayList<String>();
	      try {

	          br1 = new BufferedReader(new FileReader(csvFile1));
	         
	          while ((line1 = br1.readLine()) != null) {

	             try{
	          	// use comma as separator
	              line1 = line1.replace(",,",", , ");
	          	//   System.out.println(line);
	           	String[] countrystateDetails = line1.split(cvsSplitBy1);
	              countrykey1 = countrystateDetails[0];
	              if(countrystateMap1.containsKey(countrykey1)==false){
	            	  List<String> list = new ArrayList<String>();
	            	  list.add(countrystateDetails[1]+":"+countrystateDetails[2]);
	            	  countrystateMap1.put(countrykey1,list);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	              }
	              else{
	            	  list1 = countrystateMap1.get(countrykey1);
	            	  list1.add(countrystateDetails[1]+":"+countrystateDetails[2]);
	            	  countrystateMap1.put(countrykey1, list1);
	            	  
	              }
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      countrystatemap = Collections.unmodifiableMap(countrystateMap1);  
	  
	      //    System.out.println(citycodeMap);
	  }
	  
	  
	  
	   
	  static {
	      Map<String, String> cityMap = new HashMap<String,String>();
	      String csvFile2 = "/root/citycode1.csv";
	      BufferedReader br2 = null;
	      String line2 = "";
	      String cvsSplitBy2 = ",";
	      String key = "";
	      Map<String, String> cityMap1  = new HashMap<String,String>();
	      Map<String, List<String>> cityMap2  = new HashMap<String,List<String>>();
	      List<String> list2 = new ArrayList<String>();
	      
	      try {

	          br2 = new BufferedReader(new FileReader(csvFile2));
	         
	          while ((line2 = br2.readLine()) != null) {

	             try{
	          	// use comma as separator
	              line2 = line2.replace(",,",", , ");
	          	//   System.out.println(line);
	          	String[] geoDetails = line2.split(cvsSplitBy2);
	              key = geoDetails[6];
	              cityMap1.put(key,geoDetails[5]);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	              cityMap.put(geoDetails[5],key);
	              if(cityMap2.containsKey(geoDetails[0]+":"+geoDetails[1])==false){
	            	  List<String> list = new ArrayList<String>();
	            	  list.add(geoDetails[2]);
	            	  cityMap2.put(geoDetails[0]+":"+geoDetails[1],list);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	              }
	              else{
	            	  list2 = cityMap2.get(geoDetails[0]+":"+geoDetails[1]);
	            	  list2.add(geoDetails[2]);
	            	  cityMap2.put(geoDetails[0]+":"+geoDetails[1], list2);
	            	  
	              }             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      citycodeMap = Collections.unmodifiableMap(cityMap1);  
	      citycodeMap2 = Collections.unmodifiableMap(cityMap);  
	      countrystatecitymap = Collections.unmodifiableMap(cityMap2);
	      //    System.out.println(citycodeMap);
	  }
	  
	    
	  
	  
	  
	  public static Map<String,String> oscodeMap;
	  static {
	      Map<String, String> osMap = new HashMap<String,String>();
	      String csvFile = "/root/oscode2.csv";
	      BufferedReader br = null;
	      String line = "";
	      String cvsSplitBy = ",";
	      String key = "";
	      Map<String, String> osMap1  = new HashMap<String,String>();
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	         
	          while ((line = br.readLine()) != null) {

	             try{
	          	// use comma as separator
	              line = line.replace(",,",", , ");
	          	//   System.out.println(line);
	          	String[] osDetails = line.split(cvsSplitBy);
	              key = osDetails[0];
	              osMap1.put(key,osDetails[1]);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      oscodeMap = Collections.unmodifiableMap(osMap1);  
	      System.out.println(oscodeMap);
	  }
	   
	 
	  public static Map<String,String> oscodeMap1;
	  static {
	      Map<String, String> osMap2 = new HashMap<String,String>();
	      String csvFile = "/root/system_os.csv";
	      BufferedReader br = null;
	      String line = "";
	      String cvsSplitBy = ",";
	      String key = "";
	      Map<String, String> osMap3  = new HashMap<String,String>();
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	         
	          while ((line = br.readLine()) != null) {

	             try{
	          	// use comma as separator
	              line = line.replace(",,",", , ");
	          	//   System.out.println(line);
	          	String[] osDetails = line.split(cvsSplitBy);
	              key = osDetails[2];
	              osMap3.put(key,osDetails[0]+","+osDetails[1]);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      oscodeMap1 = Collections.unmodifiableMap(osMap3);  
	      System.out.println(oscodeMap1);
	  }
	  
	  
	  
	  
	  
	  public static Map<String,String> devicecodeMap;
	  static {
	      Map<String, String> deviceMap = new HashMap<String,String>();
	      String csvFile = "/root/devicecode2.csv";
	      BufferedReader br = null;
	      String line = "";
	      String cvsSplitBy = ",";
	      String key = "";
	      Map<String, String> deviceMap1  = new HashMap<String,String>();
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	         
	          while ((line = br.readLine()) != null) {

	             try{
	          	// use comma as separator
	              line = line.replace(",,",", , ");
	          	 //  System.out.println(line);
	          	String[] deviceDetails = line.split(cvsSplitBy);
	              key = deviceDetails[0];
	              deviceMap1.put(key,deviceDetails[1]+","+deviceDetails[4]+","+deviceDetails[8]);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      devicecodeMap = Collections.unmodifiableMap(deviceMap1);  
	   //   System.out.println(deviceMap);
	  }
	  
	  
	  
	  public static Map<String,String> audienceSegmentMap;
	  public static Map<String,String> audienceSegmentMap1;
	  public static Map<String,String> audienceSegmentMap2;
	  
	  static {
	      Map<String, String> audienceMap = new HashMap<String,String>();
	      String csvFile = "/root/subcategorymap5.csv";
	      BufferedReader br = null;
	      String line = "";
	      String cvsSplitBy = ",";
	      String key = "";
	      Map<String, String> audienceMap1  = new HashMap<String,String>();
	      Map<String, String> audienceMap2  = new HashMap<String,String>();
	      
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	         
	          while ((line = br.readLine()) != null) {

	             try{
	          	// use comma as separator
	             // line = line.replace(",,",", , ");
	          	 //  System.out.println(line);
	          	  String[] segmentDetails = line.split(cvsSplitBy);
	              key = segmentDetails[0];
	              audienceMap1.put(key,segmentDetails[1]);
	              audienceMap.put(segmentDetails[1],key);
	              //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	              audienceMap2.put(key, segmentDetails[1]);
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      audienceSegmentMap = Collections.unmodifiableMap(audienceMap1);  
	      audienceSegmentMap1 = Collections.unmodifiableMap(audienceMap);
	      audienceSegmentMap2 = Collections.unmodifiableMap(audienceMap2);
	//   System.out.println(deviceMap);
	  }
	  
	public static Map<String,String> AuthorMap;
	public static Map<String,String> AuthorMap1;
	  
	  static {
	      Map<String, String> authorMap = new HashMap<String,String>();
	      String csvFile5 = "/root/authorMap.csv";
	      BufferedReader br5 = null;
	      String line5 = "";
	      String cvsSplitBy5 = ",";
	      String key2 = "";
	      Map<String, String> authorMap1  = new HashMap<String,String>();
	      Map<String, String> authorMap2  = new HashMap<String,String>();
	      
	      try {

	          br5 = new BufferedReader(new FileReader(csvFile5));
	         
	          while ((line5 = br5.readLine()) != null) {

	             try{
	          	// use comma as separator
	              line5 = line5.replace(",,",", , ");
	          	 //  System.out.println(line);
	          	String[] authorDetails = line5.split(cvsSplitBy5);
	              key2 = authorDetails[0];
	              authorMap1.put(key2,authorDetails[1]);
	              authorMap2.put(authorDetails[1],key2);
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      AuthorMap = Collections.unmodifiableMap(authorMap1);  
	      AuthorMap1 = Collections.unmodifiableMap(authorMap2);
	//   System.out.println(deviceMap);
	  }
	   
	  public static Map<String,String> UrlMap;
	  public static Map<String,String> UrlMap1;
	    
	    static {
	        Map<String, String> authorMap = new HashMap<String,String>();
	        String csvFile5 = "/root/urlMap.csv";
	        BufferedReader br5 = null;
	        String line5 = "";
	        String cvsSplitBy5 = ",";
	        String key2 = "";
	        Map<String, String> urlMap1  = new HashMap<String,String>();
	        Map<String, String> urlMap2  = new HashMap<String,String>();
	        
	        try {

	            br5 = new BufferedReader(new FileReader(csvFile5));
	           
	            while ((line5 = br5.readLine()) != null) {

	               try{
	            	// use comma as separator
	                line5 = line5.replace(",,",", , ");
	            	 //  System.out.println(line);
	            	String[] urlDetails = line5.split(cvsSplitBy5);
	                key2 = urlDetails[0];
	                urlMap1.put(key2,urlDetails[1]);
	                urlMap2.put(urlDetails[1],key2);
	               }
	               catch(Exception e)
	               {
	            	   e.printStackTrace(); 
	                 continue;
	               }

	            }


	          
	        
	        }

	        
	        
	        
	  catch(Exception e){
	  	
	  	e.printStackTrace();
	  } 

	        
	        UrlMap = Collections.unmodifiableMap(urlMap1);  
	        UrlMap1 = Collections.unmodifiableMap(urlMap2);
	  //   System.out.println(deviceMap);
	    }
	      
	  
	  
	  public static Map<String,String> CountryMap;
	  public static Map<String,String> CountryMap1;
	    
	    static {
	        Map<String, String> countryMap = new HashMap<String,String>();
	        String csvFile5 = "/root/countryMap2.csv";
	        BufferedReader br5 = null;
	        String line5 = "";
	        String cvsSplitBy5 = ",";
	        String key2 = "";
	        Map<String, String> countryMapv1  = new HashMap<String,String>();
	        Map<String, String> countryMapv2  = new HashMap<String,String>();
	        
	        try {

	            br5 = new BufferedReader(new FileReader(csvFile5));
	           
	            while ((line5 = br5.readLine()) != null) {

	               try{
	            	// use comma as separator
	                line5 = line5.replace(",,",", , ");
	            	 //  System.out.println(line);
	            	String[] countryDetails = line5.split(cvsSplitBy5);
	                key2 = countryDetails[0];
	                countryMapv1.put(key2,countryDetails[1]);
	                countryMapv2.put(countryDetails[1],key2);
	               }
	               catch(Exception e)
	               {
	            	   e.printStackTrace(); 
	                 continue;
	               }

	            }


	          
	        
	        }

	        
	        
	        
	  catch(Exception e){
	  	
	  	e.printStackTrace();
	  } 

	        
	        CountryMap = Collections.unmodifiableMap(countryMapv1);  
	        CountryMap1 = Collections.unmodifiableMap(countryMapv2);
	  //   System.out.println(deviceMap);
	    }
	     
	  
	  

	    public static Map<String,String> StateMap;
	    public static Map<String,String> StateMap1;
	      
	      static {
	          Map<String, String> stateMap = new HashMap<String,String>();
	          String csvFile5 = "/root/statesMap5.csv";
	          BufferedReader br5 = null;
	          String line5 = "";
	          String cvsSplitBy5 = ",";
	          String key2 = "";
	          Map<String, String> stateMap1  = new HashMap<String,String>();
	          Map<String, String> stateMap2  = new HashMap<String,String>();
	          
	          try {

	              br5 = new BufferedReader(new FileReader(csvFile5));
	             
	              while ((line5 = br5.readLine()) != null) {

	                 try{
	              	// use comma as separator
	                  line5 = line5.replace(",,",", , ");
	              	 //  System.out.println(line);
	                  String[] stateDetails = line5.split(cvsSplitBy5);
	                  key2 = stateDetails[1]+","+stateDetails[2];
	                    // stateMap1.put(key2,stateDetails[1]);
	                     stateMap2.put(stateDetails[0],key2);
	                 }
	                 catch(Exception e)
	                 {
	              	   e.printStackTrace(); 
	                   continue;
	                 }

	              }


	            
	          
	          }

	          
	          
	          
	    catch(Exception e){
	    	
	    	e.printStackTrace();
	    } 

	          
	            //  StateMap = Collections.unmodifiableMap(stateMap1);  
	          StateMap1 = Collections.unmodifiableMap(stateMap2);
	    //   System.out.println(deviceMap);
	      }
	       
	    
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  public static Map<String,String> AgeMap;
	  public static Map<String,String> AgeMap1;
	    
	    static {
	        Map<String, String> ageMap = new HashMap<String,String>();
	        String csvFile5 = "/root/ageMap.csv";
	        BufferedReader br5 = null;
	        String line5 = "";
	        String cvsSplitBy5 = ",";
	        String key2 = "";
	        Map<String, String> ageMap1  = new HashMap<String,String>();
	        Map<String, String> ageMap2  = new HashMap<String,String>();
	        
	        try {

	            br5 = new BufferedReader(new FileReader(csvFile5));
	           
	            while ((line5 = br5.readLine()) != null) {

	               try{
	            	// use comma as separator
	                line5 = line5.replace(",,",", , ");
	            	 //  System.out.println(line);
	            	String[] ageDetails = line5.split(cvsSplitBy5);
	                key2 = ageDetails[0];
	                ageMap1.put(key2,ageDetails[1]);
	                ageMap2.put(ageDetails[1],key2);
	               }
	               catch(Exception e)
	               {
	            	   e.printStackTrace(); 
	                 continue;
	               }

	            }


	          
	        
	        }

	        
	        
	        
	  catch(Exception e){
	  	
	  	e.printStackTrace();
	  } 

	        
	        AgeMap = Collections.unmodifiableMap(ageMap1);  
	        AgeMap1 = Collections.unmodifiableMap(ageMap2);
	  //   System.out.println(deviceMap);
	    }
	     
	  
	    public static Map<String,String> GenderMap;
	    public static Map<String,String> GenderMap1;
	      
	      static {
	          Map<String, String> genderMap = new HashMap<String,String>();
	          String csvFile5 = "/root/genderMap.csv";
	          BufferedReader br5 = null;
	          String line5 = "";
	          String cvsSplitBy5 = ",";
	          String key2 = "";
	          Map<String, String> genderMap1  = new HashMap<String,String>();
	          Map<String, String> genderMap2  = new HashMap<String,String>();
	          
	          try {

	              br5 = new BufferedReader(new FileReader(csvFile5));
	             
	              while ((line5 = br5.readLine()) != null) {

	                 try{
	              	// use comma as separator
	                  line5 = line5.replace(",,",", , ");
	              	 //  System.out.println(line);
	              	String[] genderDetails = line5.split(cvsSplitBy5);
	                  key2 = genderDetails[0];
	                  genderMap1.put(key2,genderDetails[1]);
	                  genderMap2.put(genderDetails[1],key2);
	                 }
	                 catch(Exception e)
	                 {
	              	   e.printStackTrace(); 
	                   continue;
	                 }

	              }


	            
	          
	          }

	          
	          
	          
	    catch(Exception e){
	    	
	    	e.printStackTrace();
	    } 

	          
	          GenderMap = Collections.unmodifiableMap(genderMap1);  
	          GenderMap1 = Collections.unmodifiableMap(genderMap2);
	    //   System.out.println(deviceMap);
	      }
	       
	      public static Map<String,String> IncomeMap;
	      public static Map<String,String> IncomeMap1;
	        
	        static {
	            Map<String, String> incomeMap = new HashMap<String,String>();
	            String csvFile5 = "/root/incomeMap.csv";
	            BufferedReader br5 = null;
	            String line5 = "";
	            String cvsSplitBy5 = ",";
	            String key2 = "";
	            Map<String, String> incomeMap1  = new HashMap<String,String>();
	            Map<String, String> incomeMap2  = new HashMap<String,String>();
	            
	            try {

	                br5 = new BufferedReader(new FileReader(csvFile5));
	               
	                while ((line5 = br5.readLine()) != null) {

	                   try{
	                	// use comma as separator
	                    line5 = line5.replace(",,",", , ");
	                	 //  System.out.println(line);
	                	String[] incomeDetails = line5.split(cvsSplitBy5);
	                    key2 = incomeDetails[0];
	                    incomeMap1.put(key2,incomeDetails[1]);
	                    incomeMap2.put(incomeDetails[1],key2);
	                   }
	                   catch(Exception e)
	                   {
	                	   e.printStackTrace(); 
	                     continue;
	                   }

	                }


	              
	            
	            }

	            
	            
	            
	      catch(Exception e){
	      	
	      	e.printStackTrace();
	      } 

	            
	            IncomeMap = Collections.unmodifiableMap(incomeMap1);  
	            IncomeMap1 = Collections.unmodifiableMap(incomeMap2);
	      //   System.out.println(deviceMap);
	        }
	         
	  
	  public static Map<String,String> tagMap;
	  public static Map<String,String> tagMap1;
	    
	    static {
	        Map<String, String> TagMap = new HashMap<String,String>();
	        String csvFile5 = "/root/TagMap.csv";
	        BufferedReader br5 = null;
	        String line5 = "";
	        String cvsSplitBy5 = ",";
	        String key2 = "";
	        Map<String, String> TagMap1  = new HashMap<String,String>();
	        Map<String, String> TagMap2  = new HashMap<String,String>();
	        
	        try {

	            br5 = new BufferedReader(new FileReader(csvFile5));
	           
	            while ((line5 = br5.readLine()) != null) {

	               try{
	            	// use comma as separator
	                line5 = line5.replace(",,",", , ");
	            	 //  System.out.println(line);
	            	String[] TagDetails = line5.split(cvsSplitBy5);
	                key2 = TagDetails[0];
	                TagMap1.put(key2,TagDetails[1]);
	                TagMap2.put(TagDetails[1],key2);
	               }
	               catch(Exception e)
	               {
	            	   e.printStackTrace(); 
	                 continue;
	               }

	            }


	          
	        
	        }

	        
	        
	        
	  catch(Exception e){
	  	
	  	e.printStackTrace();
	  } 

	        
	        tagMap = Collections.unmodifiableMap(TagMap1);  
	        tagMap1 = Collections.unmodifiableMap(TagMap2);
	  //   System.out.println(deviceMap);
	    }
	  
	  
	    
	    
	    public static Map<String,String> tagMap2;
	    public static Map<String,String> tagMap3;
	      
	      static {
	          Map<String, String> TagMap = new HashMap<String,String>();
	          String csvFile5 = "/root/TagMap1.csv";
	          BufferedReader br5 = null;
	          String line5 = "";
	          String cvsSplitBy5 = ",";
	          String key2 = "";
	          Map<String, String> TagMap3  = new HashMap<String,String>();
	          Map<String, String> TagMap5  = new HashMap<String,String>();
	          
	          try {

	              br5 = new BufferedReader(new FileReader(csvFile5));
	             
	              while ((line5 = br5.readLine()) != null) {

	                 try{
	              	// use comma as separator
	                  line5 = line5.replace(",,",", , ");
	              	 //  System.out.println(line);
	              	String[] TagDetails = line5.split(cvsSplitBy5);
	                  key2 = TagDetails[0];
	                  TagMap3.put(key2,TagDetails[1]);
	                  TagMap5.put(TagDetails[1],key2);
	                 }
	                 catch(Exception e)
	                 {
	              	   e.printStackTrace(); 
	                   continue;
	                 }

	              }


	            
	          
	          }

	          
	          
	          
	    catch(Exception e){
	    	
	    	e.printStackTrace();
	    } 

	          
	          tagMap2 = Collections.unmodifiableMap(TagMap3);  
	          tagMap3 = Collections.unmodifiableMap(TagMap5);
	    //   System.out.println(deviceMap);
	      }
	     
	    
	      public static Map<String,String> referrerTypeMap;
	      public static Map<String,String> referrerTypeMap1;
	        
	        static {
	            Map<String, String> ReferrerTypeMap = new HashMap<String,String>();
	            String csvFile5 = "/root/referrerTypeMap.csv";
	            BufferedReader br5 = null;
	            String line5 = "";
	            String cvsSplitBy5 = ",";
	            String key2 = "";
	            Map<String, String> ReferrerTypeMap1  = new HashMap<String,String>();
	            Map<String, String> ReferrerTypeMap2  = new HashMap<String,String>();
	            
	            try {

	                br5 = new BufferedReader(new
	                		FileReader(csvFile5));
	               
	                while ((line5 = br5.readLine()) != null) {

	                   try{
	                	// use comma as separator
	                    line5 = line5.replace(",,",", , ");
	                	 //  System.out.println(line);
	                	String[] referrerTypeDetails = line5.split(cvsSplitBy5);
	                    key2 = referrerTypeDetails[0];
	                    ReferrerTypeMap1.put(key2,referrerTypeDetails[1]);
	                    ReferrerTypeMap2.put(referrerTypeDetails[1],key2);
	                   }
	                   catch(Exception e)
	                   {
	                	   e.printStackTrace(); 
	                     continue;
	                   }

	                }


	              
	            
	            }

	            
	            
	            
	      catch(Exception e){
	      	
	      	e.printStackTrace();
	      } 

	            
	            referrerTypeMap = Collections.unmodifiableMap(ReferrerTypeMap1);  
	            referrerTypeMap1 = Collections.unmodifiableMap(ReferrerTypeMap2);
	      //   System.out.println(deviceMap);
	        }
	         
	        
	        
	        public static Map<String,String> sectionMap;
	        public static Map<String,String> sectionMap1;
	          
	          static {
	              Map<String, String> SectionMap = new HashMap<String,String>();
	              String csvFile5 = "/root/sectionmap.csv";
	              BufferedReader br5 = null;
	              String line5 = "";
	              String cvsSplitBy5 = ",";
	              String key2 = "";
	              Map<String, String> SectionMap1  = new HashMap<String,String>();
	              Map<String, String> SectionMap2  = new HashMap<String,String>();
	              
	              try {

	                  br5 = new BufferedReader(new
	                  		FileReader(csvFile5));
	                 
	                  while ((line5 = br5.readLine()) != null) {

	                     try{
	                  	// use comma as separator
	                      line5 = line5.replace(",,",", , ");
	                  	 //  System.out.println(line);
	                  	String[] sectionDetails = line5.split(cvsSplitBy5);
	                      key2 = sectionDetails[0];
	                    SectionMap2.put(sectionDetails[1],key2);  
	                      
	                    if(sectionDetails[1].toLowerCase().equals("khel-khiladi"))	
	                    	  sectionDetails[1]="khel-khiladi";
	                 	if(sectionDetails[1].toLowerCase().equals("filmy-dunia"))	
	                 		sectionDetails[1]="filmy-dunia";
	                 	if(sectionDetails[1].toLowerCase().equals("news-digest"))	
	                 		sectionDetails[1]="news-digest";
	                      
	                      SectionMap1.put(key2,sectionDetails[1]);
	                     
	                     }
	                     catch(Exception e)
	                     {
	                  	   e.printStackTrace(); 
	                       continue;
	                     }

	                  }


	                
	              
	              }

	              
	              
	              
	        catch(Exception e){
	        	
	        	e.printStackTrace();
	        } 

	              
	              sectionMap = Collections.unmodifiableMap(SectionMap1);  
	              sectionMap1 = Collections.unmodifiableMap(SectionMap2);
	        //   System.out.println(deviceMap);
	          }
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        
	        public static Map<String,String> deviceMap;
	        public static Map<String,String> deviceMap1;
	          
	          static {
	              Map<String, String> DeviceMap = new HashMap<String,String>();
	              String csvFile5 = "/root/deviceMap.csv";
	              BufferedReader br5 = null;
	              String line5 = "";
	              String cvsSplitBy5 = ",";
	              String key2 = "";
	              Map<String, String> DeviceMap1  = new HashMap<String,String>();
	              Map<String, String> deviceMap2  = new HashMap<String,String>();
	              
	              try {

	                  br5 = new BufferedReader(new FileReader(csvFile5));
	                 
	                  while ((line5 = br5.readLine()) != null) {

	                     try{
	                  	// use comma as separator
	                      line5 = line5.replace(",,",", , ");
	                  	 //  System.out.println(line);
	                  	String[] deviceDetails = line5.split(cvsSplitBy5);
	                      key2 = deviceDetails[0];
	                      DeviceMap1.put(key2,deviceDetails[1]);
	                      deviceMap2.put(deviceDetails[1],key2);
	                     }
	                     catch(Exception e)
	                     {
	                  	   e.printStackTrace(); 
	                       continue;
	                     }

	                  }


	                
	              
	              }

	              
	              
	              
	        catch(Exception e){
	        	
	        	e.printStackTrace();
	        } 

	              
	              deviceMap = Collections.unmodifiableMap(DeviceMap1);  
	              deviceMap1 = Collections.unmodifiableMap(deviceMap2);
	        //   System.out.println(deviceMap);
	          }
  
	          
	          public static TransportClient client;
	          
	          public static void setUp()
	        		    throws Exception
	        		  {
	        		    if (client == null)
	        		    {

	        		  	  Settings settings = ImmutableSettings.settingsBuilder()
	        		                .put("cluster.name", "elasticsearch").build();
	        		   
	        		  	  
	        		      client = new TransportClient(settings)
	        		        .addTransportAddress(new InetSocketTransportAddress(
	        		        "172.16.105.231", 9300));
	        		      
	        		      NodesInfoResponse nodeInfos = (NodesInfoResponse)client.admin().cluster().prepareNodesInfo(new String[0]).get();
	        		      String clusterName = nodeInfos.getClusterName().value();
	        		   //   System.out.println(String.format("Found cluster... cluster name: %s", new Object[] { clusterName }));
	        		      
	        		      
	        		    }
	        		 //   System.out.println("Finished the setup process...");
	        		  }       
	          
	          
	          
	          
	          
	          
	          
	public static String getCookieData(HttpServletRequest request) {
		
		try {
			setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String document_id = null;	
		String city = null;
		String country = null;
		String json = null;
		HashMap<String,Integer> audienceSegmentCountMap = new HashMap<String,Integer>();
		HashMap<String,Integer> cityCountMap = new HashMap<String,Integer>();
		HashMap<String,Integer> countryCountMap = new HashMap<String,Integer>();
		HashMap<String,Integer> mobileCountMap = new HashMap<String,Integer>();
		HashMap<String,Integer> tagMap = new HashMap<String,Integer>();
		HashMap<String,Integer> sectionMap = new HashMap<String,Integer>();
		Map<String, Integer> gendermap = new HashMap<String, Integer>();
		Map<String, Integer> agemap = new HashMap<String, Integer>();
		Map<String, Integer> incomemap = new HashMap<String, Integer>();
		Set<String> latestaudienceSegment = new LinkedHashSet<String>();		
		Set<String> latestsection = new LinkedHashSet<String>();
		String mobiledevice = null;
		String name = null;
		String value = null;
		String audienceSegment = null;
		String audienceSegmentValue = null;
		String request_time = null;
		Integer count = 0;
		Integer value1 = 0;
		Integer count1 = 0;
		Integer value2 = 0;
		String agegroup = null;
		String gender = null;
		String incomelevel = null;
	    String tags = null;
	    String section = null;
	    String sectionvalue =null;
	    String [] tagparts =null;
	    
		Cookie[] cookies = request.getCookies();

		if(cookies != null){
		for (int i = 0; i < cookies.length; i++) {
		  name = cookies[i].getName();
		  
		  if(name.equals("spdsuid")){
		  value = cookies[i].getValue();
		  break;
		  }

		}	   
	}
		
		
		try {
			setUp();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
				
		if(value !=null && 
					!value.isEmpty()){
			
		//	value = "a3d8a83d_d5e0_4bac_a27d_eb8b8892b9db";
			value = value.replace("-", "_");
			
			//value = "d2ff0ee4-851e-4539-bbe8-90266571578a";
			SearchHit[] results = ElasticSearchAPIs.searchDocumentFingerprintIds(client, "enhanceduserdatabeta1", "core2", "cookie_id", value);
			for (SearchHit hit : results) {
			//	System.out.println("------------------------------");
				Map<String, Object> result = hit.getSource();
		        
		        document_id = (String) result.get("_id");
			    mobiledevice = (String) result.get("modelName");
			    city = (String) result.get("city");
			    country= (String) result.get("country");
			    audienceSegment = (String)result.get("subcategory");
			    request_time = (String)result.get("request_time");
			    tags = (String)result.get("tag");
			    sectionvalue = (String)result.get("section");
			    
			    if(tags !=null)
			    tagparts = tags.split(",");
			    
			    if(tags!=null){
			    for(int k=0; k<tagparts.length; k++){
			    if(tagparts[k]!= null && !tagparts[k].isEmpty()){
			    	tags = tagparts[k].trim();
				    
				    
				    if(tags != null && !tags.isEmpty()){
	     			if(tagMap.containsKey(tags) == false){
	     			    tagMap.put(tags,1);    
					//	System.out.println(result.get("request_time")+"\n");
					//	System.out.println("Adding Value First Time"+"\n");
	     			}
	     			 else{
							 
	     				     count1=tagMap.get(tags);	
	     				     tagMap.put(tags,count1+1);	
	     				     value2=count1+1;
	     				 //    System.out.println(result.get("request_time")+"\n");
						 //    System.out.println("Added Count to field count:"+value2+":"+tags+"\n");
	     			      }
	     			 
	     			    }
				    }
			    }
			    
			   }  
			    
			    
			    String dateStart = request_time;
				//String dateStop = "01/15/2012 10:31:48";

				//HH converts hour in 24 hours format (0-23), day calculation
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

				Date d1 = null;
			//	Date d2 = null;

				Date d2 = new Date();
				
				
					try {
						d1 = format.parse(dateStart);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//d2 = format.parse(date);

					//in milliseconds
					long diff = d2.getTime() - d1.getTime();

					long diffHours = diff / (60 * 60 * 1000) % 24;
			    
			    
			    
			    
			    
					 if((String)result.get("agegroup")!=null && !((String)result.get("agegroup")).isEmpty()){
					        if(diffHours > 3){
					    	agegroup = (String)result.get("agegroup");
					    	
					    	if (agegroup != null) {
								

								

									if (agegroup != null
											&& !agegroup.isEmpty()) {

										if (agemap.containsKey(agegroup) == false) {
											agemap.put(agegroup, 1);

										} else {

											count = agemap.get(agegroup);

											agemap.put(agegroup, count + 1);

										}

									}
								}
							
					       } 	
					 }
					        
					 if((String)result.get("gender")!=null && !((String)result.get("gender")).isEmpty()){
					        if(diffHours > 3){
					    	gender = (String)result.get("gender");
					    	if (gender != null && !gender.isEmpty()) {

								if (gendermap.containsKey(gender) == false) {
									gendermap.put(gender, 1);

							} else {

									count = gendermap.get(gender);

									gendermap.put(gender, count + 1);

								}

							}
					        
					        
					        }    
					    }
					   
					 if((String)result.get("incomelevel")!=null && !((String)result.get("incomelevel")).isEmpty()){
						    if(diffHours > 3){
					    	
						    	incomelevel = (String)result.get("incomelevel");
					    
						    	if (incomelevel != null && !incomelevel.isEmpty()) {

									if (incomemap.containsKey(incomelevel) == false) {
										incomemap.put(incomelevel, 1);

									} else {

										count = incomemap.get(incomelevel);

										incomemap.put(incomelevel, count + 1);

									}

								}
					    	
					    	
					    
						    }
					    
					    
					    
					    }
			    
			    
			    
			    if(audienceSegment!= null && !audienceSegment.isEmpty()){
			    audienceSegmentValue = audienceSegment.trim();
			    }
			    
			    if(audienceSegmentValue != null && !audienceSegmentValue.isEmpty()){
     			if(audienceSegmentCountMap.containsKey(audienceSegmentValue) == false){
     				audienceSegmentCountMap.put(audienceSegmentValue,1);    
					//System.out.println(result.get("request_time")+"\n");
					//System.out.println("Adding Value First Time"+"\n");
     			}
     			 else{
						 
     				     count=audienceSegmentCountMap.get(audienceSegmentValue);	
     				     audienceSegmentCountMap.put(audienceSegmentValue,count+1);	
     				     value1=count+1;
     				   //  System.out.println(result.get("request_time")+"\n");
					   //  System.out.println("Added Count to field count:"+value1+":"+ audienceSegmentValue+"\n");

     			 
     			    }
				
			
			  
			   if(latestaudienceSegment.size()<6)
			   latestaudienceSegment.add(audienceSegmentValue);
			   
			    }
			    
			
			    if(sectionvalue != null && !sectionvalue.isEmpty()){
	     			if(sectionMap.containsKey(sectionvalue) == false){
	     				sectionMap.put(sectionvalue,1);    
					//	System.out.println(result.get("request_time")+"\n");
					//	System.out.println("Adding Value First Time"+"\n");
	     			}
	     			 else{
							 
	     				     count1=sectionMap.get(sectionvalue);	
	     				     sectionMap.put(sectionvalue,count1+1);	
	     				     value2=count1+1;
	     				  //   System.out.println(result.get("request_time")+"\n");
						  //   System.out.println("Added Count to field count:"+value2+":"+ sectionvalue+"\n");

	     			 
	     			    }
					
				
				  
				   if(latestsection.size()<4)
					  latestsection.add(sectionvalue);
				   
				    }
			
			
			
			
			}
			
			Set<String> result = new HashSet<String>();
			Set<String> latestFrequentCategories = new LinkedHashSet<String>();
			Set<String> latestFrequentCategories1 = new LinkedHashSet<String>();
			ArrayList<String> mostFrequentCategories = new ArrayList<String>();
			ArrayList<String> mostFrequentCategories1 = new ArrayList<String>();
			ArrayList<String> taglist = new ArrayList<String>();
			Set<String> latestFrequentSection = new LinkedHashSet<String>();
			Set<String> latestFrequentSection1 = new LinkedHashSet<String>();
			ArrayList<String> mostFrequentSection = new ArrayList<String>();
			ArrayList<String> mostFrequentSection1 = new ArrayList<String>();
			
            if(audienceSegmentCountMap != null && !audienceSegmentCountMap.isEmpty()){
    			Iterator it = audienceSegmentCountMap .entrySet().iterator();
    		    while (it.hasNext()) {
    		        Map.Entry pair = (Map.Entry)it.next();
    		     //   System.out.println(pair.getKey() + " = " + pair.getValue());
    		        if(Integer.parseInt(pair.getValue().toString())>1){
    		        	
    		        	mostFrequentCategories.add(pair.getKey().toString());
    		        	
    		        }
    		        	
    			}
            }
    		    
    		    if(latestaudienceSegment != null && !latestaudienceSegment.isEmpty()){
        			
    		    	
    		    	if(mostFrequentCategories != null && !mostFrequentCategories.isEmpty()){
    		    	for(String segment : latestaudienceSegment){
                              if(mostFrequentCategories.contains(segment))
                            		  latestFrequentCategories.add(segment);
        			}
        		        	
    		    	}
    		    
    		    }    
    		   
    		    
    		    if(sectionMap != null && !sectionMap.isEmpty()){
        			Iterator it = sectionMap .entrySet().iterator();
        		    while (it.hasNext()) {
        		        Map.Entry pair = (Map.Entry)it.next();
        		    //    System.out.println(pair.getKey() + " = " + pair.getValue());
        		        if(Integer.parseInt(pair.getValue().toString())>1){
        		        	
        		        	mostFrequentSection.add(pair.getKey().toString());
        		        	
        		        }
        		        	
        			}
                }
        		    
        		    if(latestsection != null && !latestsection.isEmpty()){
            			
        		    	
        		    	if(mostFrequentSection != null && !mostFrequentSection.isEmpty()){
        		    	for(String section1 : latestsection){
                                  if(mostFrequentSection.contains(section1))
                                		  latestFrequentSection.add(section1.toLowerCase());
            			}
            		        	
        		    	}
        		    
        		    }    
    		    
    		    
    		    
    		    
    		    
    		    
    		    for(String s1:latestFrequentCategories){
    		    	//System.out.println(s1);
    		    //	latestFrequentCategories1.add(audienceSegmentMap2.get(s1));
    		    	latestFrequentCategories1.add(s1.toLowerCase().replace("_", " ").replaceAll("\\."," "));
    		    }
    		    
    		    
                for(String s2:mostFrequentCategories){
                  //  System.out.println(s2);    		    	
                //	mostFrequentCategories1.add(audienceSegmentMap2.get(s2));
                    mostFrequentCategories1.add(s2.toLowerCase().replace("_", " ").replaceAll("\\."," "));
    		    }
    		    
    		    
    		    
    		    
    		    //Create a Heap
    		    
    		    PriorityQueue<Pair> queue = new PriorityQueue<Pair>(1000,new Comparator<Pair>(){
    	            
    		    	public int compare(Pair a, Pair b){
    	                return a.count-b.count;
    	            }
    	        });
    	 
    		    if(tagMap != null && !tagMap.isEmpty()){  
    	        for(Map.Entry<String, Integer> entry: tagMap.entrySet()){
    	            Pair p = new Pair(entry.getKey().toLowerCase(), entry.getValue());
    	            queue.offer(p);
    	            if(queue.size()>15){
    	                queue.poll();
    	            }
    	        }
    	 
    	        
    	       
    	        while(queue.size()>0){
    	            result.add(queue.poll().topic);
    	        }
    	         
    		    }
    		    
    		    
    		    if(gendermap !=null && !gendermap.isEmpty())
    		    gender = getMaxEntry(gendermap).getKey();
    		    
    		    if(agemap !=null && !agemap.isEmpty())
    		    agegroup = getMaxEntry(agemap).getKey();
    		    
    		    if(incomemap !=null && !incomemap.isEmpty())
                incomelevel = getMaxEntry(incomemap).getKey();
                
    		    UserProfile profile = new UserProfile();
    		    
    		    
    		    if(city!=null)
    		    profile.setCity(city.toLowerCase());
    		    
    		    if(country !=null)
    		    profile.setCountry(country.toLowerCase());
    		    
    		    if(mobiledevice !=null)
    		    profile.setMobileDevice(mobiledevice.toLowerCase());
    		   
    		    if(latestFrequentCategories1 !=null)
    		    profile.setInMarketSegments(latestFrequentCategories1);
       		    
    		    if(mostFrequentCategories1 !=null)
    		    profile.setAffinitySegments(mostFrequentCategories1);
       		    
    		    if(latestFrequentSection !=null) 
    		    profile.setSection(latestFrequentSection);
    		   
    		    
    		    if(gender !=null)
    		    profile.setGender(gender.toLowerCase());
 		        
    		    
    		    if(agegroup !=null)
    		    profile.setAge(AgeMap1.get(agegroup));
 		        
    		    if(incomelevel !=null)
    		    profile.setIncomelevel(incomelevel.toLowerCase());
       		    
    		    if(result!=null)
    		    profile.setTags(result);
 		        
    		    json = new Gson().toJson(profile);
    		    
					
	}
		
	      return json;
	
	
	}

	
	 public static Entry<String, Integer> getMaxEntry(Map<String, Integer> map){        
		    Entry<String, Integer> maxEntry = null;
		    Integer max = Collections.max(map.values());

		    for(Entry<String, Integer> entry : map.entrySet()) {
		        Integer value = entry.getValue();
		        if(null != value && max == value) {
		            maxEntry = entry;
		        }
		    }
		    return maxEntry;
		}	
	
	
	
	
	
}
