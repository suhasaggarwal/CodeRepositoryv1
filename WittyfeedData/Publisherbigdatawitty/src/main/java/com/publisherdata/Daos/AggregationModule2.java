package com.publisherdata.Daos;

import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.publisherdata.model.Article;
import com.publisherdata.model.PublisherReport;
import com.publisherdata.model.Site;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javolution.util.FastMap;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.json.JSONArray;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import util.Convertor;
import util.GetMiddlewareData;
import util.ListtoResultSet;
import util.NestedJSON;

public class AggregationModule2
{
  private static TransportClient client;
  private static SearchDao searchDao;
  private static AggregationModule2 INSTANCE;
  
  public static Map<String,String> citycodeMap;
  public static Map<String,String> citycodeMap2;
  public static Map<String,String> countrymap;
  public static Map<String,List<String>> countrystatemap;
  public static Map<String,List<String>> countrystatecitymap;
  
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
      String csvFile = "/root/subcategorymap1.csv";
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
              line = line.replace(",,",", , ");
          	 //  System.out.println(line);
          	String[] segmentDetails = line.split(cvsSplitBy);
              key = segmentDetails[0];
              audienceMap1.put(key,segmentDetails[1]);
              audienceMap.put(segmentDetails[4],key);
              //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
              audienceMap2.put(key, segmentDetails[4]);
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
   
  
  
  public static String capitalizeString(String string) {
	  char[] chars = string.toLowerCase().toCharArray();
	  boolean found = false;
	  for (int i = 0; i < chars.length; i++) {
	    if (!found && Character.isLetter(chars[i])) {
	      chars[i] = Character.toUpperCase(chars[i]);
	      found = true;
	    } else if (Character.isWhitespace(chars[i]) || chars[i]=='.' || chars[i]=='\'') { // You can add other chars here
	      found = false;
	    }
	  }
	  return String.valueOf(chars);
	}
  
  
  
  
  
  public static String capitalizeFirstLetter(String original) {
	    if (original == null || original.length() == 0) {
	        return original;
	    }
	    return original.substring(0, 1).toUpperCase() + original.substring(1);
	}
  
  
  
  
  public static AggregationModule2 getInstance()
  {
    if (INSTANCE == null) {
      return new AggregationModule2();
    }
    return INSTANCE;
  }
  
  public static void main(String[] args)
    throws Exception
  {
	  
	
	 //setUp();
     AggregationModule2 mod = new  AggregationModule2();
     mod.setUp();
  //   mod.countBrandNameChannel("2016-01-13","2017-01-31", "Patrika_multiple_June_16");
    // Map<String,String> filter1 = new HashMap<String,String>();
   //  filter1.put("cookie_id","bba865ee_b360_4a58_a95a_74e6ba44e7b4");
   //  mod.getUserdetailsChannel("2017-01-13","2017-01-31","Womanseraindia_indiagate",filter1);
   /* 
     mod.countbenchmarktotalvisitorsChannel("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      mod.countbenchmarkfingerprintChannel("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      mod.countbenchmarktotalvisitorsChannelDateHourlywise("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      mod.countBenchmarktotalvisitorsChannelDatewise("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      mod.countbenchmarkfingerprintChannelDateHourwise("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      mod.countBenchmarkfingerprintChannelDatewise("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      mod.benchmarkengagementTimeChannel("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      mod.benchmarkengagementTimeChannelDateHourwise("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      mod.benchmarkengagementTimeChannelDatewise("2017-01-17","2017-01-18","Womanseraindia_indiagate");
      
     */ 
      
      
      
      
      /*
     mod.countOS("2017-01-01","2017-01-31"); */
    mod.counttotalvisitorsChannelSectionDateHourlywise("2017-01-19","2017-01-19","Womanseraindia_indiagate","http___womansera_com_entertainment");
  //   mod.counttotalvisitorsChannelSectionDateHourlyMinutewise("2017-01-16 13:00:01","2017-01-16 13:59:59","Womanseraindia_indiagate","http___womansera_com_entertainment");
  //   mod.countCityChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","shahid");
    /* 
     mod.countNewUsersChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countReturningUsersChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countLoyalUsersChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countNewUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
     mod.countReturningUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
     mod.countLoyalUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
     mod.getGenderChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.gettimeofdayChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     */
     
     /*
     mod.getChannelSectionArticleCount("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.getChannelSectionArticleList("2017-01-01","2017-01-31","Womanseraindia_indiagate","*");
     mod.getChannelArticleReferrerList("2017-01-01","2017-01-31","Womanseraindia_indiagate","amitabh");
     mod.getChannelArticleReferredPostsList("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.countfingerprintChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.countfingerprintChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.counttotalvisitorsChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.counttotalvisitorsChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.countAudiencesegmentChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate", "http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
  */   
     
  /*   
     mod.getChannelSectionArticleCount("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.getChannelSectionArticleList("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.getChannelSectionReferrerList("2017-01-01","2017-01-31","Womanseraindia_indiagate","entertainment");
     mod.getChannelSectionReferredPostsList("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countfingerprintChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countfingerprintChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.counttotalvisitorsChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.counttotalvisitorsChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
 */
 
 //    mod.countAudiencesegmentChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate", "http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
     /*
    
     mod.countOSChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/trending/amitabh-bachchan-dead-pictures-going-viral");
     
     mod.countCityChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/trending/amitabh-bachchan-dead-pictures-going-viral");
     
     mod.countModelChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/trending/amitabh-bachchan-dead-pictures-going-viral");
    
 mod.countOSChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.countCityChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.countModelChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.counttotalvisitorsChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.counttotalvisitorsChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.countfingerprintChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.countfingerprintChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.countLoyalUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.countReturningUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.countNewUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
    
     mod.getAgegroupChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.getGenderChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
    */ 
//     mod.countAudiencesegmentChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
    
     Map<String,String>filter = new HashMap<String,String>();
     filter.put("city","delhi,mumbai");
     filter.put("agegroup","35_44");
//     filter.put("incomelevel", "medium");
  //   filter.put("devicetype","tablet");
    // mod.getGenderChannelFilter("2017-01-01","2017-01-31","Womanseraindia_indiagate", filter);
    
     
     List<String> groupby = new ArrayList<String>();
     groupby.add("subcategory");
   
   //  groupby.add("incomelevel");
     mod.getQueryFieldChannelGroupBy("audience_segment","2017-01-01","2017-01-31","Womanseraindia_indiagate", groupby,"pageviews");
     /*	 
	 final long startTime1 = System.currentTimeMillis();
	 AggregationModule mod = new AggregationModule();
	 mod.countAudienceSegment("2016-08-20","2016-12-02");
	 mod.countAudienceSegment("2016-08-20","2016-12-02");  
	 mod.countAudienceSegment("2016-08-20","2016-12-02");
	 mod.countAudienceSegment("2016-08-20","2016-12-02");  
	 mod.countAudienceSegment("2016-08-20","2016-12-02");
	 mod.countAudienceSegment("2016-08-20","2016-12-02");  
	 mod.countAudienceSegment("2016-08-20","2016-12-02");
	 final long endTime1 = System.currentTimeMillis();
	 
	 
	 System.out.println("Total code execution time: " + (endTime1 - startTime1) );
		
	  */
	  //  countfingerprintChannel("2016-08-20","2016-12-02", "Mumbai_T1_airport");
	  
	//  countAudiencesegmentChannel("2016-08-20","2016-12-02", "Mumbai_T1_airport");
	
	  
	//  Integer countv1 = 500000;
	  
	  /*
	  
	  if(countv1 >= 500000)
	    {
	    double total_length = countv1 - 0;
	    double subrange_length = total_length/30;	
	    
	    double current_start = 0;
	    for (int i = 0; i < 20; ++i) {
	      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
	      current_start += subrange_length;
	    }
	   }
  */
	  
	  /*
	  
	    Double countv1 = Double.parseDouble("90000");
	    
	    Double n = 0.0;
	    if(countv1 >= 250000)
	       n=50.0;
	    
	    if(countv1 >= 100000 && countv1 <= 250000 )
	       n=20.0;
	    
	    if(countv1 < 100000)
           n=50.0;	    
	   
	    Double total_length = countv1 - 0;
	    Double subrange_length = total_length/n;	
	    String startdate= "Startdate";
	    String enddate= "endDate";
	    Double current_start = 0.0;
	    for (int i = 0; i < n; ++i) {
	      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
	      Double startlimit = current_start;
	      Double finallimit = current_start + subrange_length;
	      Double index = startlimit +1;
	      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdatabeta1 where date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit "+index.intValue()+","+finallimit.intValue();  	
		  System.out.println(query);
	  //    Query.add(query);
	      current_start += subrange_length;
	    //  Query.add(query);
	     
	    }
	   */ 
	    
  
  }
  
  public void setUp()
    throws Exception
  {
    if (client == null)
    {
      client = new TransportClient();
      client.addTransportAddress(getTransportAddress());
      
      NodesInfoResponse nodeInfos = (NodesInfoResponse)client.admin().cluster().prepareNodesInfo(new String[0]).get();
      String clusterName = nodeInfos.getClusterName().value();
      //System.out.println(String.format("Found cluster... cluster name: %s", new Object[] { clusterName }));
      
      searchDao = new SearchDao(client);
    }
    //System.out.println("Finished the setup process...");
  }
  
  public static SearchDao getSearchDao()
  {
    return searchDao;
  }
  
  
  
  public List<String> getcountryNames()

	{

		List<String> CountryNames = new ArrayList<String>();
		for (Map.Entry<String, String> entry : countrymap.entrySet()) {
			CountryNames.add(entry.getKey() + "," + entry.getValue());
		}

		System.out.println(CountryNames);
		return CountryNames;

	}

	public List<String> getcountryStateNames(String countrycode)

	{

		List<String> stateNames = new ArrayList<String>();
		stateNames = countrystatemap.get(countrycode);
		System.out.println(stateNames);
		return stateNames;

	}

	public List<String> getcountryCityNames(String countrycode, String statecode) {

		List<String> cityNames = new ArrayList<String>();
		cityNames = countrystatecitymap.get(countrycode.toLowerCase() + ":" + statecode.toLowerCase());
		System.out.println(cityNames);

		return cityNames;

	}


  
	public static int getDifferenceDays(Date d1, Date d2) {
		int daysdiff=0;
		long diff = d2.getTime() - d1.getTime();
		long diffDays = diff / (24 * 60 * 60 * 1000)+1;
		 daysdiff = (int) diffDays;
		return daysdiff;
		 }
  
	
	public static List<Long> getDaysBetweenDates(Date startdate, Date enddate)
	{
	    List<Long> dates = new ArrayList<Long>();
	    Calendar calendar = new GregorianCalendar();
	    calendar.setTime(startdate);

	    while (calendar.getTime().before(enddate))
	    {
	        Date result = calendar.getTime();
	        Long date  = result.getTime() / 1000;
	        dates.add(date);
	        calendar.add(Calendar.DATE, 1);
	    }
	    return dates;
	}
  
  
  public List<PublisherReport> countBrandName(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if (lines.size() > 0) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
        obj.setBrandname(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
       }
      }
    }
    //System.out.println(headers);
    //System.out.println(lines);
    
    return pubreport;
  }
  
  public List<PublisherReport> countBrowser(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
       
        obj.setBrowser(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
    }
    //System.out.println(headers);
    //System.out.println(lines);
    
    return pubreport;
  }
  
  public List<PublisherReport> countOS(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    System.out.println(headers);
    System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setOs(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countModel(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
        obj.setMobile_device_model_name(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
        }
        
        }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countCity(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setCity(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countPinCode(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    //System.out.println(headers);
    //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setPostalcode(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countLatLong(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    //System.out.println(headers);
    //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        String[] dashcount = data[0].split("_");
        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_') && 
          (!dashcount[2].isEmpty()))
        {
          obj.setLatitude_longitude(data[0]);
          obj.setCount(data[1]);
          pubreport.add(obj);
        }
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countfingerprint(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where date between '" + 
      startdate + "'" + " and " + "'" + enddate + "'" + " group by date", new Object[] {"enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setDate(data[0]);
        obj.setReach(data[1]);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countAudienceSegment(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
	
	  PrintStream out = new PrintStream(new FileOutputStream(
				"audiencesegmentcount.txt"));
		System.setOut(out);
	  
	  List<PublisherReport> pubreport = new ArrayList(); 
	  
	  String querya1 = "Select Count(DISTINCT(cookie_id)) FROM enhanceduserdata where date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";  
	  
	    //Divide count in different limits 
	
	  
	  List<String> Query = new ArrayList();
	  


	    System.out.println(querya1);
	    
	    final long startTime2 = System.currentTimeMillis();
		
	    
	    CSVResult csvResult1 = null;
		try {
			csvResult1 = AggregationModule2.getCsvResult(false, querya1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    final long endTime2 = System.currentTimeMillis();
		
	    List<String> headers = csvResult1.getHeaders();
	    List<String> lines = csvResult1.getLines();
	    
	    
	    String count = lines.get(0);
	    Double countv1 = Double.parseDouble(count);
	    Double n = 0.0;
	    if(countv1 >= 250000)
	       n=10.0;
	    
	    if(countv1 >= 100000 && countv1 <= 250000 )
	       n=10.0;
	    
	    if(countv1 < 100000)
           n=10.0;	    
	   
	    
	    if(countv1 <= 100)
	    	n=1.0;
	    
	    if(countv1 == 0)
	    {
	    	
	    	return pubreport;
	    	
	    }
	    
	    Double total_length = countv1 - 0;
	    Double subrange_length = total_length/n;	
	    
	    Double current_start = 0.0;
	    for (int i = 0; i < n; ++i) {
	      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
	      Double startlimit = current_start;
	      Double finallimit = current_start + subrange_length;
	      Double index = startlimit +1;
	      if(countv1 == 1)
	    	  index=0.0;
	      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdata where date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
		  System.out.println(query);
	  //    Query.add(query);
	      current_start += subrange_length;
	      Query.add(query);
	     
	    }
	    
	    
	    	
	    
	  
	  ExecutorService executorService = Executors.newFixedThreadPool(2000);
        
       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
    
       for(int i=0 ; i < Query.size(); i++ ){
       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
        
       // returns a list of Futures holding their status and results when all complete
       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
       }
       
       
       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
        
       System.out.println(maps.size() +" Responses recieved.\n");
        
       for(Future<FastMap<String,Double>> task : maps)
       {
    	   try{
           if(task!=null)
    	   System.out.println(task.get().toString());
    	   }
    	   catch(Exception e)
    	   {
    		   e.printStackTrace();
    		   continue;
    	   }
    	    
    	   
    	   }
        
       /* shutdown your thread pool, else your application will keep running */
       executorService.shutdown();
	  
	
	  //  //System.out.println(headers1);
	 //   //System.out.println(lines1);
	    
	    
       
       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
       
       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
       
       Double count1 = 0.0;
       
       Double count2 = 0.0;
       
       String key ="";
       String key1 = "";
       Double value = 0.0;
       Double vlaue1 = 0.0;
       
	    for (int i = 0; i < maps.size(); i++)
	    {
	    
	    	if(maps!=null && maps.get(i)!=null){
	        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
	    	
	       if(map.size() > 0){
	       
	       if(map.containsKey("audience_segment")==true){
	       for (Map.Entry<String, Double> entry : map.entrySet())
	    	 {
	    	  key = entry.getKey();
	    	  key = key.trim();
	    	  value=  entry.getValue();
	    	if(key.equals("audience_segment")==false) { 
	    	if(audiencemap.containsKey(key)==false)
	    	audiencemap.put(key,value);
	    	else
	    	{
	         count1 = audiencemap.get(key);
	         if(count1!=null)
	         audiencemap.put(key,count1+value);	
	    	}
	      }
	    }
	  }   

	       if(map.containsKey("subcategory")==true){
	       for (Map.Entry<String, Double> entry : map.entrySet())
	    	 {
	    	   key = entry.getKey();
	    	   key = key.trim();
	    	   value=  entry.getValue();
	    	if(key.equals("subcategory")==false) {    
	    	if(subcatmap.containsKey(key)==false)
	    	subcatmap.put(key,value);
	    	else
	    	{
	         count1 = subcatmap.get(key);
	         if(count1!=null)
	         subcatmap.put(key,count1+value);	
	    	}
	    }  
	    	
	   }
	      
	     	       }
	           
	       } 
	    
	    	} 	
	   }    
	    
	    String subcategory = null;
	   
	    if(audiencemap.size()>0){
	   
	    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
	    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
	    

	        PublisherReport obj = new PublisherReport();
	        
	   //     String[] data = ((String)lines.get(i)).split(",");
	        
	     //   if(data[0].trim().toLowerCase().contains("festivals"))
	      //  obj.setAudience_segment("");
	      //  else
	        obj.setAudience_segment( entry.getKey());	
	        obj.setCount(String.valueOf(entry.getValue()));
	      
	        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
	        {
	         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
	        	 
	        	    
	        	 
	        	 PublisherReport obj1 = new PublisherReport();
	            
	           
	            if (entry1.getKey().contains(entry.getKey()))
	            {
	              String substring = "_" + entry.getKey() + "_";
	              subcategory = entry1.getKey().replace(substring, "");
	           //   if(data[0].trim().toLowerCase().contains("festivals"))
	           //   obj1.setAudience_segment("");
	           //   else
	        
	              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
	              obj1.setAudience_segment(subcategory);
	              obj1.setCount(String.valueOf(entry1.getValue()));
	              obj.getAudience_segment_data().add(obj1);
	            }
	          }
	          pubreport.add(obj);
	        }
	      
	    }
	    }
	    return pubreport;
 
   
  }
  
  public List<PublisherReport> countISP(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,ISP FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by ISP", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
        obj.setISP(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
       }
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countOrg(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT organisation FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " and organisation NOT IN (Select DISTINCT(ISP) FROM enhanceduserdatabeta1)", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setOrganisation(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public Set<String> getChannelList(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    String query = String.format("SELECT channel_name FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " Group by channel_name", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<String> finallines = new ArrayList();
    Set<String> data = new HashSet();
    data.addAll(lines);
    
    //System.out.println(headers);
    //System.out.println(lines);
    
    return data;
  }
  
  public List<PublisherReport> gettimeofdayQuarter(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 WHERE date between '" + startdate + "'" + " and " + "'" + enddate + "' GROUP BY HOUR(request_time)";
    
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> gettimeofdayDaily(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 WHERE date between '" + startdate + "'" + " and " + "'" + enddate + "' GROUP BY date_histogram(field='request_time','interval'='1d')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> gettimeofday(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 WHERE date between '" + startdate + "'" + " and " + "'" + enddate + "' GROUP BY date_histogram(field='request_time','interval'='1h')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countGender(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,gender FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by gender", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    
    //System.out.println(headers);
    //System.out.println(lines);
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setGender(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countAgegroup(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,agegroup FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by agegroup", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    
    //System.out.println(headers);
    //System.out.println(lines);
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setAge(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getOrg(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
    CSVResult csvResult1 = getCsvResult(false, query1);
    List<String> headers1 = csvResult1.getHeaders();
    List<String> lines1 = csvResult1.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines1.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data1 = ((String)lines1.get(i)).split(",");
        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].contains("broadband")) && (!data1[0].contains("communication")) && (!data1[0].contains("cable")) && (!data1[0].contains("telecom")) && (!data1[0].contains("network")) && (!data1[0].contains("isp")) && (!data1[0].contains("hathway")) && (!data1[0].contains("internet")) && (!data1[0].contains("Sify")) && (!data1[0].toLowerCase().equals("_ltd")) && (!data1[0].equals("Googlebot")) && (!data1[0].equals("Bsnl")))
        {
          obj.setOrganisation(data1[0]);
          obj.setCount(data1[1]);
          
          pubreport.add(obj);
        }
      }
      //System.out.println(headers1);
      //System.out.println(lines1);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getdayQuarterdata(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if (data[0].equals("quarter1")) {
          data[0] = "quarter1 (00 - 04 AM)";
        }
        if (data[0].equals("quarter2")) {
          data[0] = "quarter2 (04 - 08 AM)";
        }
        if (data[0].equals("quarter3")) {
          data[0] = "quarter3 (08 - 12 AM)";
        }
        if (data[0].equals("quarter4")) {
          data[0] = "quarter4 (12 - 16 PM)";
        }
        if (data[0].equals("quarter5")) {
          data[0] = "quarter5 (16 - 20 PM)";
        }
        if (data[0].equals("quarter6")) {
          data[0] = "quarter6 (20 - 24 PM)";
        }
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countBrandNameChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    String query = "SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName";
    //System.out.println(query);
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
        obj.setBrandname(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
        } 
       }
  //    //System.out.println(headers);
  //    //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countBrowserChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = "SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where channel_name ='" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setBrowser(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countOSChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
  //  //System.out.println(headers);
  //  //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setOs(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countModelChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");

        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
        obj.setMobile_device_model_name(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
      }
        
        }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countCityChannel(String startdate, String enddate, String channel_name, String filter)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = "SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city order by count desc";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    Integer accumulatedCount = 0;
    
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(i<10){
        	String locationproperties = citycodeMap.get(data[0]);
	        data[0]=data[0].replace("_"," ").replace("-"," ");
	        data[0]=capitalizeString(data[0]);
	        obj.setCity(data[0]);
	        System.out.println(data[0]);
	        obj.setLocationcode(locationproperties);
        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
			obj.setCount(data[1]);
			if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
			obj.setEngagementTime(data[1]);
			if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
			obj.setVisitorCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
        }
        else
        {	   
        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
        	 
	    	  if(i == (lines.size()-1)){
	    		 obj.setCity("Others"); 
	    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			  //   String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
	    		  if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						obj.setCount(accumulatedCount.toString());
						if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						obj.setEngagementTime(accumulatedCount.toString());
						if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						obj.setVisitorCount(accumulatedCount.toString());
	    		 pubreport.add(obj);
	    	  }
	       }
      
      
      
      
      }
    }
    return pubreport;
  }
  
  
  public List<PublisherReport> countStateChannel(String startdate, String enddate, String channel_name, String filter)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,state FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by state order by count desc";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    Integer accumulatedCount = 0;
		    
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        if(i<10){
		        	data[0]=data[0].replace("_", " ");
	            	data[0] = capitalizeString(data[0]);
	            	obj.setState(data[0]);
		        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					obj.setCount(data[1]);
					if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
					obj.setEngagementTime(data[1]);
					if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					obj.setVisitorCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        pubreport.add(obj);
		        }
		        else
		        {	   
		        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
		        	 
			    	  if(i == (lines.size()-1)){
			    		 obj.setState("Others"); 
			    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					  //   String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			    		  if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(accumulatedCount.toString());
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(accumulatedCount.toString());
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(accumulatedCount.toString());
			    		 pubreport.add(obj);
			    	  }
			       }
		      
		      
		      
		      
		      }
		    }
		    return pubreport;
		  }
  
  
  public List<PublisherReport> countCountryChannel(String startdate, String enddate, String channel_name, String filter)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,country FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by country order by count desc";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    Integer accumulatedCount = 0;
		    
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        if(i<10){
		        	data[0]=data[0].replace("_", " ");
	            	data[0] = capitalizeString(data[0]);
	            	obj.setCountry(data[0]);
		        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					obj.setCount(data[1]);
					if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
					obj.setEngagementTime(data[1]);
					if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					obj.setVisitorCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        pubreport.add(obj);
		        }
		        else
		        {	   
		        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
		        	 
			    	  if(i == (lines.size()-1)){
			    		 obj.setCountry("Others"); 
			    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					  //   String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			    		  if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(accumulatedCount.toString());
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(accumulatedCount.toString());
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(accumulatedCount.toString());
			    		 pubreport.add(obj);
			    	  }
			       }
		      
		      
		      
		      
		      }
		    }
		    return pubreport;
		  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  public List<PublisherReport> countfingerprintChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
	  
	  
	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
	  
    
	  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
		      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
	  
		 CSVResult csvResult00 = getCsvResult(false, query00);
		 List<String> headers00 = csvResult00.getHeaders();
		 List<String> lines00 = csvResult00.getLines();
		 List<PublisherReport> pubreport00 = new ArrayList();  
			  
		//  //System.out.println(headers00);
		//  //System.out.println(lines00);  
		  
		  for (int i = 0; i < lines00.size(); i++)
	      {
	       
	        String[] data = ((String)lines00.get(i)).split(",");
	  //      //System.out.println(data[0]);
	      }
		  
		  
		  
		//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
	    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
	    CSVResult csvResult = getCsvResult(false, query);
	    List<String> headers = csvResult.getHeaders();
	    List<String> lines = csvResult.getLines();
	    List<PublisherReport> pubreport = new ArrayList();
	    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
	      for (int i = 0; i < lines.size(); i++)
	      {
	        PublisherReport obj = new PublisherReport();
	        
	        String[] data = ((String)lines.get(i)).split(",");
	        obj.setDate(data[0]);
	        obj.setReach(data[1]);
	        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
	        pubreport.add(obj);
	      }
	    }
	    
    return pubreport;
  }
  
  
  
  
  
  public List<PublisherReport> countbenchmarkfingerprintChannel(String startdate, String enddate, String channel_name)
		    throws CsvExtractorException, Exception
		  {
			  
			  
		//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
			  
		    /*
			  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
			  
				 CSVResult csvResult00 = getCsvResult(false, query00);
				 List<String> headers00 = csvResult00.getHeaders();
				 List<String> lines00 = csvResult00.getLines();
				 List<PublisherReport> pubreport00 = new ArrayList();  
					  
				//  //System.out.println(headers00);
				//  //System.out.println(lines00);  
				  
				  for (int i = 0; i < lines00.size(); i++)
			      {
			       
			        String[] data = ((String)lines00.get(i)).split(",");
			  //      //System.out.println(data[0]);
			      }
				  
				*/  
		
	  String time = startdate;
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      Date date1 = df.parse(time);
     
      String time1 = enddate;
      Date date2 = df.parse(time1);
            
     
     
      int days = getDifferenceDays(date2, date1)-2;
      Calendar cal = Calendar.getInstance();
      cal.setTime(date1);
      cal.add(Calendar.DAY_OF_YEAR, days);
      Date benchmarkStartDate1 = cal.getTime();
      cal.setTime(date1);
      cal.add(Calendar.DAY_OF_YEAR, -1);
      Date benchmarkEndDate1 = cal.getTime();


      String benchmarkStartDate = df.format(benchmarkStartDate1);	  
      String benchmarkEndDate  = df.format(benchmarkEndDate1);	
	  
	  
     //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		
				
				String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
			      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " group by date";
			    CSVResult csvResult = getCsvResult(false, query);
			    List<String> headers = csvResult.getHeaders();
			    List<String> lines = csvResult.getLines();
			    List<PublisherReport> pubreport = new ArrayList();
			    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
			      for (int i = 0; i < lines.size(); i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			        String[] data = ((String)lines.get(i)).split(",");
			        obj.setDate(data[0]);
			        obj.setReach(data[1]);
			        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			        pubreport.add(obj);
			      }
			    }
			    
		    return pubreport;
		  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  public List<PublisherReport> countAudiencesegmentChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
      List<PublisherReport> pubreport = new ArrayList(); 
	  
	  String querya1 = "SELECT COUNT(DISTINCT(cookie_id)) FROM enhanceduserdata where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";   
	  
	    //Divide count in different limits 
	
	  
	  List<String> Query = new ArrayList();
	  


	    System.out.println(querya1);
	    
	    final long startTime2 = System.currentTimeMillis();
		
	    
	    CSVResult csvResult1 = null;
		try {
			csvResult1 = AggregationModule2.getCsvResult(false, querya1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    final long endTime2 = System.currentTimeMillis();
		
	    List<String> headers = csvResult1.getHeaders();
	    List<String> lines = csvResult1.getLines();
	    
	    
	    String count = lines.get(0);
	    Double countv1 = Double.parseDouble(count);
	    Double n = 0.0;
	    if(countv1 >= 250000)
	       n=10.0;
	    
	    if(countv1 >= 100000 && countv1 <= 250000 )
	       n=10.0;
	    
	    if(countv1 <= 100000 && countv1 > 100)
           n=10.0;	    
	   
	    if(countv1 <= 100)
	    	n=1.0;
	    
	    if(countv1 == 0)
	    {
	    	
	    	return pubreport;
	    	
	    }
	    
	    Double total_length = countv1 - 0;
	    Double subrange_length = total_length/n;	
	    
	    Double current_start = 0.0;
	    for (int i = 0; i < n; ++i) {
	      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
	      Double startlimit = current_start;
	      Double finallimit = current_start + subrange_length;
	      Double index = startlimit +1;
	      if(countv1 == 1)
	    	  index=0.0;
	      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdata where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
		  System.out.println(query);
	  //    Query.add(query);
	      current_start += subrange_length;
	      Query.add(query);
	     
	    }
	    
	    
	    	
	    
	  
	  ExecutorService executorService = Executors.newFixedThreadPool(2000);
        
       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
    
       for(int i=0 ; i < Query.size(); i++ ){
       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
        
       // returns a list of Futures holding their status and results when all complete
       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
       }
       
       
       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
        
       System.out.println(maps.size() +" Responses recieved.\n");
        
       for(Future<FastMap<String,Double>> task : maps)
       {
    	   try{
           if(task!=null)
    	   System.out.println(task.get().toString());
    	   }
    	   catch(Exception e)
    	   {
    		   e.printStackTrace();
    		   continue;
    	   }
    	    
    	   
    	   }
        
       /* shutdown your thread pool, else your application will keep running */
       executorService.shutdown();
	  
	
	  //  //System.out.println(headers1);
	 //   //System.out.println(lines1);
	    
	    
       
       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
       
       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
       
       Double count1 = 0.0;
       
       Double count2 = 0.0;
       
       String key ="";
       String key1 = "";
       Double value = 0.0;
       Double vlaue1 = 0.0;
       
	    for (int i = 0; i < maps.size(); i++)
	    {
	    
	    	if(maps!=null && maps.get(i)!=null){
	        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
	    	
	       if(map.size() > 0){
	       
	       if(map.containsKey("audience_segment")==true){
	       for (Map.Entry<String, Double> entry : map.entrySet())
	    	 {
	    	  key = entry.getKey();
	    	  key = key.trim();
	    	  value=  entry.getValue();
	    	if(key.equals("audience_segment")==false) { 
	    	if(audiencemap.containsKey(key)==false)
	    	audiencemap.put(key,value);
	    	else
	    	{
	         count1 = audiencemap.get(key);
	         if(count1!=null)
	         audiencemap.put(key,count1+value);	
	    	}
	      }
	    }
	  }   

	       if(map.containsKey("subcategory")==true){
	       for (Map.Entry<String, Double> entry : map.entrySet())
	    	 {
	    	   key = entry.getKey();
	    	   key = key.trim();
	    	   value=  entry.getValue();
	    	if(key.equals("subcategory")==false) {    
	    	if(subcatmap.containsKey(key)==false)
	    	subcatmap.put(key,value);
	    	else
	    	{
	         count1 = subcatmap.get(key);
	         if(count1!=null)
	         subcatmap.put(key,count1+value);	
	    	}
	    }  
	    	
	   }
	      
	     	       }
	           
	       } 
	    
	    	} 	
	   }    
	    
	    String subcategory = null;
	   
	    if(audiencemap.size()>0){
	   
	    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
	    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
	    

	        PublisherReport obj = new PublisherReport();
	        
	   //     String[] data = ((String)lines.get(i)).split(",");
	        
	     //   if(data[0].trim().toLowerCase().contains("festivals"))
	      //  obj.setAudience_segment("");
	      //  else
	        obj.setAudience_segment( entry.getKey());	
	        obj.setCount(String.valueOf(entry.getValue()));
	      
	        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
	        {
	         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
	        	 
	        	    
	        	 
	        	 PublisherReport obj1 = new PublisherReport();
	            
	           
	            if (entry1.getKey().contains(entry.getKey()))
	            {
	              String substring = "_" + entry.getKey() + "_";
	              subcategory = entry1.getKey().replace(substring, "");
	           //   if(data[0].trim().toLowerCase().contains("festivals"))
	           //   obj1.setAudience_segment("");
	           //   else
	        
	              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
	              obj1.setAudience_segment(subcategory);
	              obj1.setCount(String.valueOf(entry1.getValue()));
	              obj.getAudience_segment_data().add(obj1);
	            }
	          }
	          pubreport.add(obj);
	        }
	      
	    }
	    }
	    return pubreport;
  }
  
  public List<PublisherReport> gettimeofdayChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countPinCodeChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = "SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    //System.out.println(headers);
    //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
	      for (int i = 0; i < lines.size(); i++)
	      {
	        PublisherReport obj = new PublisherReport();
	        
	        String[] data = ((String)lines.get(i)).split(",");
	        String[] data1 = data[0].split("_");
	        String locationproperties  = citycodeMap.get(data1[0]);
	        obj.setPostalcode(data[0]);
	        obj.setCount(data[1]);
	        obj.setLocationcode(locationproperties);
	        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
	      
	        pubreport.add(obj);
	      }
	    }
    return pubreport;
  }
  
  public List<PublisherReport> countLatLongChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    //System.out.println(headers);
    //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        String[] dashcount = data[0].split("_");
        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_'))
        {
          if (!dashcount[2].isEmpty())
          {
            obj.setLatitude_longitude(data[0]);
            obj.setCount(data[1]);
            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
          }
          pubreport.add(obj);
        }
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> gettimeofdayQuarterChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='4h')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> gettimeofdayDailyChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1d')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getdayQuarterdataChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if (data[0].equals("quarter1")) {
          data[0] = "quarter1 (00 - 04 AM)";
        }
        if (data[0].equals("quarter2")) {
          data[0] = "quarter2 (04 - 08 AM)";
        }
        if (data[0].equals("quarter3")) {
          data[0] = "quarter3 (08 - 12 AM)";
        }
        if (data[0].equals("quarter4")) {
          data[0] = "quarter4 (12 - 16 PM)";
        }
        if (data[0].equals("quarter5")) {
          data[0] = "quarter5 (16 - 20 PM)";
        }
        if (data[0].equals("quarter6")) {
          data[0] = "quarter6 (20 - 24 PM)";
        }
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  
  public static List<String> processList(List<String> lines)
  {
	  
	  
	  List<String> lines1 = new ArrayList<String>();
FastMap<String,Integer> aggregatedMap = new FastMap<String,Integer>();  
if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
{
  for (int i = 0; i < lines.size(); i++)
  {
	 try{ 
	 String[] data = lines.get(i).split(",");
     if(aggregatedMap.containsKey(data[0])==false)
	 aggregatedMap.put(data[0],1);
     else
    	 aggregatedMap.put(data[0], aggregatedMap.get(data[0])+1);
	 }
	 catch(Exception e)
	 {
		 continue;
		 
	 }
	 
	 }
}
  
  for(Map.Entry<String, Integer> entry: aggregatedMap.entrySet()){
	  lines1.add(entry.getKey()+","+entry.getValue());
	  
  }
  return lines1;
  
  } 
  
  
  public static List<String> processList1(List<String> lines)
  {
	  
	  
	  List<String> lines1 = new ArrayList<String>();
FastMap<String,Integer> aggregatedMap = new FastMap<String,Integer>();  
if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
{
  for (int i = 0; i < lines.size(); i++)
  {
	 try{ 
	 String[] data = lines.get(i).split(",");
     if(aggregatedMap.containsKey(data[0]+"@"+data[1])==false)
	 aggregatedMap.put(data[0]+"@"+data[1],1);
     else
    	 aggregatedMap.put(data[0]+"@"+data[1], aggregatedMap.get(data[0]+"@"+data[1])+1);
	 }
	 catch(Exception e)
	 {
		 continue;
		 
	 }
	 
	 }
}
  
  for(Map.Entry<String, Integer> entry: aggregatedMap.entrySet()){
	  String []parts = entry.getKey().split("@");
	  lines1.add(parts[0]+","+parts[1]+","+entry.getValue());
	  
  }
  return lines1;
  
  }  
  
  public static List<PublisherReport> SessionPageDepthChannel(String startdate, String enddate, String channel_name)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT session_id,referrer FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by session_id,referrer";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    //System.out.println(headers);
		    //System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    Map<String,Integer> sessiondepthMap = new HashMap<String,Integer>();
          Map<String,List<String>> depthSessionMap = new HashMap<String,List<String>>();
		    lines = processList(lines);
        
          Integer value = 0;
          for(Integer i=1; i < 21; i++){
		    	
		    	sessiondepthMap.put(i.toString(),0);
		    	
		    	
		    }
		    
		      sessiondepthMap.put("20+",0);
		      
		      
		    
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
			      for (int i = 0; i < lines.size(); i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			        String[] data = ((String)lines.get(i)).split(",");
			        Integer data1 = Integer.parseInt(data[1]);
			        
			        if(sessiondepthMap.containsKey(data1.toString())){
			        	
			        	value  =  sessiondepthMap.get(data1.toString());
			        	sessiondepthMap.put(data1.toString(),value+1);
			        	
			        	
			        	
			        	
			        	
			        }
			        else{
			        	
			        	value = sessiondepthMap.get("20+");
			        	sessiondepthMap.put("20+",value+1);
			        	
			        	
			        }
			        	String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			      
			        pubreport.add(obj);
			      }
			    }
		   
		   
            for (Map.Entry<String, Integer> entry : sessiondepthMap.entrySet())
		    {
		        System.out.println(entry.getKey() + "," + entry.getValue());
		    }

            
            pubreport.clear();
            for (Map.Entry<String, Integer> entry : sessiondepthMap.entrySet())
			    {
			        
            	    PublisherReport obj = new PublisherReport();
			        String key = entry.getKey();
			        if(key!=null && !key.isEmpty()){
			        obj.setSessionPageDepth(entry.getKey());
			        obj.setCount(entry.getValue().toString());
			        pubreport.add(obj);
			    }
			    }
		    
		    
		    return pubreport;
		  }


public List<PublisherReport> SessionFrequencyChannel(String startdate, String enddate, String channel_name)
		    throws CsvExtractorException, Exception
		  {
		   // //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT cookie_id,session_id FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by cookie_id,session_id";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    //System.out.println(headers);
		    //System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		   
		    
		    Map<String,Integer> sessionFrequencyMap = new HashMap<String,Integer>();
          Map<String,List<String>> FrequencySessionMap = new HashMap<String,List<String>>();
		    lines = processList(lines);
        
          Integer value = 0;
          for(Integer i=1; i < 205; i++){
		    	
		    	sessionFrequencyMap.put(i.toString(),0);
		    	
		    	
		    }
		    
		      sessionFrequencyMap.put("205",0);
		 //     sessionFrequencyMap.put("15-25",0);
		   //   sessionFrequencyMap.put("25-50",0);
		     // sessionFrequencyMap.put("51-100",0);
		    //  sessionFrequencyMap.put("101-200",0);
		     // sessionFrequencyMap.put("200+",0);      
		     		    
		    
		      
			    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        Integer data1 = Integer.parseInt(data[1]);
				        
				        if(sessionFrequencyMap.containsKey(data1.toString())){
				        	
				        	value  =  sessionFrequencyMap.get(data1.toString());
				        	sessionFrequencyMap.put(data1.toString(),value+1);
				        	
				        	
				        	
				        	
				        	
				        }
				        else{
				        	
				        	value = sessionFrequencyMap.get("205");
				        	sessionFrequencyMap.put("205",value+1);
				        	
				        	
				        }
				        	String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				      
				        pubreport.add(obj);
				      }
				    }
			   
			   
	              for (Map.Entry<String, Integer> entry : sessionFrequencyMap.entrySet())
			    {
			        System.out.println(entry.getKey() + "," + entry.getValue());
			    }


				    Map<String,Integer> FinalMap = new FastMap<String,Integer>();
				    FinalMap.put("1", 0);
				    FinalMap.put("2", 0);
				    FinalMap.put("3", 0);
				    FinalMap.put("4", 0);
				    FinalMap.put("5", 0);
				    FinalMap.put("6", 0);
				    FinalMap.put("7", 0);
				    FinalMap.put("8", 0);
				    FinalMap.put("9-14", 0);
				    FinalMap.put("15-25", 0);
				    FinalMap.put("25-50", 0);
				    FinalMap.put("51-100", 0);
				    FinalMap.put("101-200", 0);
				    FinalMap.put("200+", 0);
				    Integer value1 = 0;
				    
				    for (Map.Entry<String, Integer> entry : sessionFrequencyMap.entrySet())
				    {
				         if(Integer.parseInt(entry.getKey()) == 1){
				        	 value1 = FinalMap.get("1");
				        	 FinalMap.put("1", value1+entry.getValue());
				         }
				    
				         if(Integer.parseInt(entry.getKey()) == 2){
				        	 value1 = FinalMap.get("2");
				        	 FinalMap.put("2", value1+entry.getValue());
				         }
				         
				         if(Integer.parseInt(entry.getKey()) == 3){
				        	 value1 = FinalMap.get("3");
				        	 FinalMap.put("3", value1+entry.getValue());
				         }
				         
				         if(Integer.parseInt(entry.getKey()) == 4){
				        	 value1 = FinalMap.get("4");
				        	 FinalMap.put("4", value1+entry.getValue());
				         }
				         
				         
				         if(Integer.parseInt(entry.getKey()) == 5){
				        	 value1 = FinalMap.get("5");
				        	 FinalMap.put("5", value1+entry.getValue());
				         }
				         
				         
				         if(Integer.parseInt(entry.getKey()) == 6){
				        	 value1 = FinalMap.get("6");
				        	 FinalMap.put("6", value1+entry.getValue());
				         }
				         

				         if(Integer.parseInt(entry.getKey()) == 7){
				        	 value1 = FinalMap.get("7");
				        	 FinalMap.put("7", value1+entry.getValue());
				         }
				         
				         
				         if(Integer.parseInt(entry.getKey()) == 8){
				        	 value1 = FinalMap.get("8");
				        	 FinalMap.put("8", value1+entry.getValue());
				         }
				         
				         
				         
				         
				         if(Integer.parseInt(entry.getKey()) > 9  && Integer.parseInt(entry.getKey()) <= 14 ){
			            	 value1 = FinalMap.get("9-14");
				        	 FinalMap.put("9-14", value1+entry.getValue());
				         }
				         if(Integer.parseInt(entry.getKey()) > 14  && Integer.parseInt(entry.getKey()) <= 25){
				        	 value1 = FinalMap.get("15-25");
				        	 FinalMap.put("15-25", value1+entry.getValue());
				         }
				         if(Integer.parseInt(entry.getKey()) > 25  && Integer.parseInt(entry.getKey()) <= 50){
				        	 value1 = FinalMap.get("25-50");
				        	 FinalMap.put("25-50", value1+entry.getValue());
				         }
				         if(Integer.parseInt(entry.getKey()) > 50  && Integer.parseInt(entry.getKey()) <= 100){
				        	 value1 = FinalMap.get("51-100");
				        	 FinalMap.put("51-100", value1+entry.getValue());
				         }
				         if(Integer.parseInt(entry.getKey()) > 100 && Integer.parseInt(entry.getKey()) <= 200 ){
				        	 value1 = FinalMap.get("101-200");
				        	 FinalMap.put("101-200", value1+entry.getValue());
				         }
				         if(Integer.parseInt(entry.getKey()) > 200 ){
				        	 value1 = FinalMap.get("200+");
				        	 FinalMap.put("200+", value1+entry.getValue());
				         }
				         
				         
				         }
			    
				    
				    pubreport.clear();
				    for (Map.Entry<String, Integer> entry : FinalMap.entrySet())
				    {
				        PublisherReport obj = new PublisherReport();
				        String key = entry.getKey();
				        if(key!=null && !key.isEmpty()){
				        obj.setSessionFrequency(entry.getKey());
				        obj.setCount(entry.getValue().toString());
				        pubreport.add(obj);
				    }
				    } 
			    
			    return pubreport;
		  }


public List<PublisherReport> SessionDurationChannel(String startdate, String enddate, String channel_name)
		    throws CsvExtractorException, Exception
		  {
		   // //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query1 = "SELECT  session_id,max(request_time) FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by session_id";
		    CSVResult csvResult = getCsvResult(false, query1);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<String> lines2 = new ArrayList<String>();
		    //System.out.println(headers);
		    //System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		   
		    FastMap<String,String> aggregatedMap = new FastMap<String,String>();  
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		    	 try{ 
		    	 String[] data = lines.get(i).split(",");
		     
		    	 aggregatedMap.put(data[0],data[1]);
		    	 }
		    	 
		    	 catch(Exception e)
		    	 {
		    		 continue;
		    		 
		    	 }
		    	 
		    	 }
		    }		    
		    
		    
		    String query2 = "SELECT  session_id,min(request_time) FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by session_id";
		    CSVResult csvResult1 = getCsvResult(false, query2);
		    List<String> headers1 = csvResult1.getHeaders();
		    List<String> lines1 = csvResult1.getLines();
		    //System.out.println(headers);
		    //System.out.println(lines);
		    List<PublisherReport> pubreport1 = new ArrayList();
		    
		    FastMap<String,String> aggregatedMap1 = new FastMap<String,String>();  
		    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines1.size(); i++)
		      {
		    	 try{ 
		    	 String[] data = lines1.get(i).split(",");
		     
		    	 aggregatedMap1.put(data[0],data[1]);
		    	 }
		    	 
		    	 catch(Exception e)
		    	 {
		    		 continue;
		    		 
		    	 }
		    	 
		    	 }
		    }
		    
		   
		    
		    FastMap<String,String> aggregatedMap2 = new FastMap<String,String>();
		    
		    for (Map.Entry<String,String> entry : aggregatedMap.entrySet())
		    {
		        String t1 = aggregatedMap.get(entry.getKey());
		        		
		        String t2 = aggregatedMap1.get(entry.getKey());
		

		        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		        Date parse = sdf.parse(t1);
		        Date parse2 = sdf.parse(t2);

		        Long diff = parse.getTime() - parse2.getTime();
		     
		        diff = diff/1000000;
		        
		        
		        aggregatedMap2.put(entry.getKey(),diff.toString());
		      		    
		    }
		    
		    for(Map.Entry<String, String> entry: aggregatedMap2.entrySet()){
		  	  lines2.add(entry.getKey()+","+entry.getValue());
		  	  
		    }
		    
		    Map<String,Integer> sessionDurationMap = new HashMap<String,Integer>();
          Map<String,List<String>> DurationSessionMap = new HashMap<String,List<String>>();
		   
      
        Integer value = 0;
        for(Integer i=1; i < 100000; i++){
		    	
		    	sessionDurationMap.put(i.toString(),0);
		    	
		    	
		    }
		    
		      sessionDurationMap.put("100000",0);
		 //     sessionFrequencyMap.put("15-25",0);
		   //   sessionFrequencyMap.put("25-50",0);
		     // sessionFrequencyMap.put("51-100",0);
		    //  sessionFrequencyMap.put("101-200",0);
		     // sessionFrequencyMap.put("200+",0);      
		     		    
		    
		      
			    if ((lines2 != null) && (!lines2.isEmpty()) && (!((String)lines2.get(0)).isEmpty())) {
				      for (int i = 0; i < lines2.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines2.get(i)).split(",");
				        Integer data1 = Integer.parseInt(data[1]);
				        
				        if(sessionDurationMap.containsKey(data1.toString())){
				        	
				        	value  =  sessionDurationMap.get(data1.toString());
				        	sessionDurationMap.put(data1.toString(),value+1);
				        	
				        	
				        	
				        	
				        	
				        }
				        else{
				        	
				        	value = sessionDurationMap.get("100000");
				        	sessionDurationMap.put("100000",value+1);
				        	
				        	
				        }
				        	String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				      
				        pubreport.add(obj);
				      }
				    }
			   
			   
	              for (Map.Entry<String, Integer> entry : sessionDurationMap.entrySet())
			    {
			         if(entry.getValue()>0)
	            	  System.out.println(entry.getKey() + "," + entry.getValue());
			    }

			    Map<String,Integer> FinalMap = new FastMap<String,Integer>();
			    FinalMap.put("0-10 seconds", 0);
			    FinalMap.put("11-30 seconds", 0);
			    FinalMap.put("31-60 seconds", 0);
			    FinalMap.put("61-180 seconds", 0);
			    FinalMap.put("181-600 seconds", 0);
			    FinalMap.put("601-1800 seconds", 0);
			    FinalMap.put("1800+ seconds", 0);
			    
			    Integer value1 = 0;
			    
			    for (Map.Entry<String, Integer> entry : sessionDurationMap.entrySet())
			    {
			         if(Integer.parseInt(entry.getKey()) <= 10){
			        	 value1 = FinalMap.get("0-10 seconds");
			        	 FinalMap.put("0-10 seconds", value1+entry.getValue());
			         }
			    
			         if(Integer.parseInt(entry.getKey()) > 10  && Integer.parseInt(entry.getKey()) <= 30 ){
		            	 value1 = FinalMap.get("11-30 seconds");
			        	 FinalMap.put("11-30 seconds", value1+entry.getValue());
			         }
			         if(Integer.parseInt(entry.getKey()) > 30  && Integer.parseInt(entry.getKey()) <= 60){
			        	 value1 = FinalMap.get("31-60 seconds");
			        	 FinalMap.put("31-60 seconds", value1+entry.getValue());
			         }
			         if(Integer.parseInt(entry.getKey()) > 60  && Integer.parseInt(entry.getKey()) <= 180){
			        	 value1 = FinalMap.get("61-180 seconds");
			        	 FinalMap.put("61-180 seconds", value1+entry.getValue());
			         }
			         if(Integer.parseInt(entry.getKey()) > 180  && Integer.parseInt(entry.getKey()) <= 600){
			        	 value1 = FinalMap.get("181-600 seconds");
			        	 FinalMap.put("181-600 seconds", value1+entry.getValue());
			         }
			         if(Integer.parseInt(entry.getKey()) > 601 && Integer.parseInt(entry.getKey()) <= 1800 ){
			        	 value1 = FinalMap.get("601-1800 seconds");
			        	 FinalMap.put("601-1800 seconds", value1+entry.getValue());
			         }
			         if(Integer.parseInt(entry.getKey()) > 1800 ){
			        	 value1 = FinalMap.get("1800+ seconds");
			        	 FinalMap.put("1800+ seconds", value1+entry.getValue());
			         }
			         
			         
			         }
			    
			    pubreport.clear();
			    for (Map.Entry<String, Integer> entry : FinalMap.entrySet())
			    {
			        PublisherReport obj = new PublisherReport();
			        String key = entry.getKey();
			        if(key!=null && !key.isEmpty()){
			        obj.setSessionDuration(entry.getKey());
			        obj.setCount(entry.getValue().toString());
			        pubreport.add(obj);
			    }
			    
			    } 
			    
			    
			    
			    
			    
			    
			    
			    
			    return pubreport;
		  }


  
  
  
  
  public List<PublisherReport> getQueryFieldChannel(String queryfield,String startdate, String enddate, String channel_name, String filter)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
	String query = "";        
	  
	        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
			query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
		    
			if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
			query= "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
			
			if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
			query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' GROUP by "+ queryfield+"";  
    

    System.out.println(query);
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    
    
    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
        lines = processList(lines);
    
    Integer flag= 0;
    Integer accumulatedcount = 0;
    
    
    //System.out.println(headers);
    //System.out.println(lines);
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        try{
    	 
    	  PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
     //   String demographicproperties = demographicmap.get(data[0]);
            
            
        
            if(queryfield.equals("gender"))
        	obj.setGender(data[0]);
        
            
            if(queryfield.equals("state"))
            	{
            	
            	data[0]=data[0].replace("_", " ");
            	data[0] = capitalizeString(data[0]);
            	obj.setState(data[0]);
            	}
            
            
            if(queryfield.equals("country"))
        	  {
        	
            	data[0]=data[0].replace("_", " ");
            	data[0] = capitalizeString(data[0]);
            	obj.setCountry(data[0]);
             	}
        
            
            if(queryfield.equals("device"))
        	obj.setDevice_type(data[0]);
        	
        	if(queryfield.equals("city")){
        		try{
        		String locationproperties = citycodeMap.get(data[0]);
		        data[0]=data[0].replace("_"," ").replace("-"," ");
		        data[0]=capitalizeString(data[0]);
		        obj.setCity(data[0]);
		        System.out.println(data[0]);
		        obj.setLocationcode(locationproperties);
        		}
        		catch(Exception e){
        			continue;
        		}
        		
        		
        		
        		} 
        	if(queryfield.equals("audience_segment")){
        		String audienceSegment = audienceSegmentMap.get(data[0]);
        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
        		obj.setAudience_segment(audienceSegment);
        		obj.setAudienceSegmentCode(audienceSegmentCode);
        		}
        		else
        	    obj.setAudience_segment(data[0]);
        	}
        	if(queryfield.equals("reforiginal"))
	             obj.setReferrerSource(data[0]);
            	
        	if(queryfield.equals("agegroup"))
	             {
        		 data[0]=data[0].replace("_","-");
        		 data[0]=data[0]+ " Years";
        		 if(data[0].contains("medium")==false)
        		 obj.setAge(data[0]);
	             }
            	
        	if(queryfield.equals("incomelevel"))
	          obj.setIncomelevel(data[0]);
        
         	
        	if(queryfield.equals("ISP")){
        		if(data[0].trim().toLowerCase().equals("_ltd")==false){
        	        data[0] = data[0].replace("_", " ").replace("-", " ");
        			obj.setISP(data[0]);
        	}
        	}	
            
        	if(queryfield.equals("organisation")){
        
            	if((!data[0].trim().toLowerCase().contains("broadband")) && (!data[0].trim().toLowerCase().contains("communication")) && (!data[0].trim().toLowerCase().contains("cable")) && (!data[0].trim().toLowerCase().contains("telecom")) && (!data[0].trim().toLowerCase().contains("network")) && (!data[0].trim().toLowerCase().contains("isp")) && (!data[0].trim().toLowerCase().contains("hathway")) && (!data[0].trim().toLowerCase().contains("internet")) && (!data[0].trim().toLowerCase().equals("_ltd")) && (!data[0].trim().toLowerCase().contains("googlebot")) && (!data[0].trim().toLowerCase().contains("sify")) && (!data[0].trim().toLowerCase().contains("bsnl")) && (!data[0].trim().toLowerCase().contains("reliance")) && (!data[0].trim().toLowerCase().contains("broadband")) && (!data[0].trim().toLowerCase().contains("tata")) && (!data[0].trim().toLowerCase().contains("nextra"))){
            		data[0] = data[0].replace("_", " ").replace("-", " ");
            		obj.setOrganisation(data[0]);
            	}
            }
        	
            
            if(queryfield.equals("screen_properties")){
        		
        		obj.setScreen_properties(data[0]);
        		
        	}
            
            if(queryfield.equals("authorName")){
            	obj.setArticleAuthor(data[0]);
        		String authorId = AuthorMap1.get(data[0]);
        		obj.setAuthorId(authorId);
        		
        	}
        	
             if(queryfield.equals("tag")){
	        		
	        		obj.setArticleTags(data[0]);
	        	}
            
            
            
            
            
            if(queryfield.equals("system_os")){
        		String osproperties = oscodeMap.get(data[0]);
		        data[0]=data[0].replace("_"," ").replace("-", " ");
		        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
		        String [] osParts = oscodeMap1.get(osproperties).split(",");
		        obj.setOs(osParts[0]);
		        obj.setOSversion(osParts[1]);
		        obj.setOscode(osproperties);
        	}
         	
        	if(queryfield.equals("modelName")){
        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
	        	
		        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
		        System.out.println(mobiledeviceproperties[2]);
		        obj.setDevicecode(mobiledeviceproperties[0]);
		        System.out.println(mobiledeviceproperties[0]);
        	}
         	
        	if(queryfield.equals("brandName")){
	            data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
        		obj.setBrandname(data[0]);
        	}
	          
        	if(queryfield.equals("refcurrentoriginal"))
  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1); if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
        	
        	
     //   obj.setGender(data[0]);
     //   obj.setCode(code);
        
        	
        	
        Random random = new Random();	
        Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
        Integer max = (int)Double.parseDouble(data[1]);
        Integer randomNumber1 = random.nextInt(max) + 1;
        
        if(queryfield.equals("audience_segment"))	
        {
        obj.setCount(data[1]); 	
        obj.setExternalWorldCount(randomNumber.toString());	
        obj.setVisitorCount(randomNumber1.toString());
        obj.setAverageTime("0.0");	
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        
        
        pubreport.add(obj);
        
        }
       
        
        else if(queryfield.equals("agegroup")==true) {
        	
        	if(data[0].contains("medium")==false){
        		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
        	        	obj.setCount(data[1]);
        	       
        	            if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
        	            obj.setEngagementTime(data[1]);
        	        
        	            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
        	            obj.setVisitorCount(data[1]);
        		String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
                
                
                pubreport.add(obj);
        	}
        }
       
       
        	    else{
        	    	
        	    	 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
        	         	obj.setCount(data[1]);
        	        
        	             if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
        	             obj.setEngagementTime(data[1]);
        	         
        	             if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
        	             obj.setVisitorCount(data[1]);
            		String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
                    
                    
                    pubreport.add(obj);
        	    }
        
        	
        	
        	
        }
        catch(Exception e)
        {
        	continue;
        }
        }
      //System.out.println(headers);
      //System.out.println(lines);
    }
   
    
    
    if (queryfield.equals("sessiondepth")) {
        
  	  AggregationModule2 module =  AggregationModule2.getInstance();
  	    try {
  			module.setUp();
  		} catch (Exception e1) {
  			// TODO Auto-generated catch block
  			e1.printStackTrace();
  		}
		pubreport=module.SessionPageDepthChannel(startdate, enddate, channel_name);
		return pubreport;
  }
    
    
    
    if (queryfield.equals("sessionfrequency")) {
        
  	  AggregationModule2 module =  AggregationModule2.getInstance();
  	    try {
  			module.setUp();
  		} catch (Exception e1) {
  			// TODO Auto-generated catch block
  			e1.printStackTrace();
  		}
		pubreport=module.SessionFrequencyChannel(startdate, enddate, channel_name);
		return pubreport;
  }
    
    
    if (queryfield.equals("sessionduration")) {
        
    	  AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
  		pubreport=module.SessionDurationChannel(startdate, enddate, channel_name);
  		return pubreport;
    }
    
    
    
    
    
    
    
    
    
    if (queryfield.equals("LatLong")) {
        
  	  AggregationModule2 module =  AggregationModule2.getInstance();
  	    try {
  			module.setUp();
  		} catch (Exception e1) {
  			// TODO Auto-generated catch block
  			e1.printStackTrace();
  		}
		pubreport=module.countLatLongChannel(startdate, enddate, channel_name);
		return pubreport;
  }
    
    if (queryfield.equals("postalcode")) {
        
    	  AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
  		pubreport=module.countPinCodeChannel(startdate, enddate, channel_name);
  		return pubreport;
    }
      
    if (queryfield.equals("cityOthers")) {
        
  	  AggregationModule2 module =  AggregationModule2.getInstance();
  	    try {
  			module.setUp();
  		} catch (Exception e1) {
  			// TODO Auto-generated catch block
  			e1.printStackTrace();
  		}
		pubreport=module.countCityChannel(startdate, enddate, channel_name, filter);
		return pubreport;
  }
    
    
    if (queryfield.equals("stateOthers")) {
        
    	  AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
  		pubreport=module.countStateChannel(startdate, enddate, channel_name, filter);
  		return pubreport;
    }
    
    
    if (queryfield.equals("countryOthers")) {
        
    	  AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
  		pubreport=module.countCountryChannel(startdate, enddate, channel_name, filter);
  		return pubreport;
    }
    
    
    if (queryfield.equals("newVisitors")) {
        
  	  AggregationModule2 module =  AggregationModule2.getInstance();
  	    try {
  			module.setUp();
  		} catch (Exception e1) {
  			// TODO Auto-generated catch block
  			e1.printStackTrace();
  		}
		pubreport=module.countNewUsersChannelDatewise(startdate, enddate, channel_name,filter); 
		return pubreport;
  }
  
  if (queryfield.equals("returningVisitors")) {
      
  	 AggregationModule2 module =  AggregationModule2.getInstance();
	    try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		pubreport=module.countReturningUsersChannelDatewise(startdate, enddate, channel_name,filter); 
		return pubreport;
  }
  
  if (queryfield.equals("LoyalVisitors")) {
      
  	 AggregationModule2 module =  AggregationModule2.getInstance();
	    try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		pubreport=module.countLoyalUsersChannelDatewise(startdate, enddate, channel_name,filter); 
		return pubreport;
  }
  
    
    
    
    
   if(queryfield.equals("visitorType")){
		
        List<PublisherReport> pubreport1  = new ArrayList<PublisherReport>();
        List<PublisherReport> pubreport2  = new ArrayList<PublisherReport>();
        List<PublisherReport> pubreport3  = new ArrayList<PublisherReport>();
        
	   
    	AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
		
    	pubreport1=module.countNewUsersChannelDatewise(startdate, enddate, channel_name,filter); 
		
    
		pubreport2=module.countReturningUsersChannelDatewise(startdate, enddate, channel_name,filter); 
		
   
 		pubreport3=module.countLoyalUsersChannelDatewise(startdate, enddate, channel_name,filter); 
 		
  
        pubreport1.addAll(pubreport2);
        pubreport1.addAll(pubreport3);
   
   
        return pubreport1;
   }
    
    if (queryfield.equals("totalViews")) {
        
   	 AggregationModule2 module =  AggregationModule2.getInstance();
 	    try {
 			module.setUp();
 		} catch (Exception e1) {
 			// TODO Auto-generated catch block
 			e1.printStackTrace();
 		}
		pubreport=module.counttotalvisitorsChannel(startdate, enddate, channel_name); 
		return pubreport;
   }
    
    
    if (queryfield.equals("benchmarktotalViews")) {
        
      	 AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
   		pubreport=module.countbenchmarktotalvisitorsChannel(startdate, enddate, channel_name);
   		return pubreport;
      }
    
    
    if (queryfield.equals("totalViewsDatewise")) {
        
      	 AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
   		pubreport=module.counttotalvisitorsChannelDatewise(startdate, enddate, channel_name);
   		return pubreport;
      }
    
    if (queryfield.equals("benchmarktotalViewsDatewise")) {
        
     	 AggregationModule2 module =  AggregationModule2.getInstance();
   	    try {
   			module.setUp();
   		} catch (Exception e1) {
   			// TODO Auto-generated catch block
   			e1.printStackTrace();
   		}
  		pubreport=module.countBenchmarktotalvisitorsChannelDatewise(startdate, enddate, channel_name);
  		return pubreport;
     }
   
    
    
    
    
    if (queryfield.equals("totalViewsHourwise")) {
        
     	 AggregationModule2 module =  AggregationModule2.getInstance();
   	    try {
   			module.setUp();
   		} catch (Exception e1) {
   			// TODO Auto-generated catch block
   			e1.printStackTrace();
   		}
  		pubreport=module.counttotalvisitorsChannelDateHourlywise(startdate, enddate, channel_name);
  		return pubreport;
     }
    
      
    if (queryfield.equals("benchmarktotalViewsHourwise")) {
        
    	 AggregationModule2 module =  AggregationModule2.getInstance();
  	    try {
  			module.setUp();
  		} catch (Exception e1) {
  			// TODO Auto-generated catch block
  			e1.printStackTrace();
  		}
 		pubreport=module.countbenchmarktotalvisitorsChannelDateHourlywise(startdate, enddate, channel_name);
 		return pubreport;
    } 
    
    
           
    if (queryfield.equals("uniqueVisitorsDatewise")) {
        
      	 AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
   		pubreport=module.countfingerprintChannelDatewise(startdate, enddate, channel_name);
   		return pubreport;
      }
    
 
    if (queryfield.equals("benchmarkuniqueVisitorsDatewise")) {
        
     	 AggregationModule2 module =  AggregationModule2.getInstance();
   	    try {
   			module.setUp();
   		} catch (Exception e1) {
   			// TODO Auto-generated catch block
   			e1.printStackTrace();
   		}
  		pubreport=module.countBenchmarkfingerprintChannelDatewise(startdate, enddate, channel_name);
  		return pubreport;
     }
    
    
    
    if (queryfield.equals("uniqueVisitorsHourwise")) {
        
     	 AggregationModule2 module =  AggregationModule2.getInstance();
   	    try {
   			module.setUp();
   		} catch (Exception e1) {
   			// TODO Auto-generated catch block
   			e1.printStackTrace();
   		}
  		pubreport=module.countfingerprintChannelDateHourwise(startdate, enddate, channel_name);
  		return pubreport;
     }
    
    
    if (queryfield.equals("benchmarkuniqueVisitorsHourwise")) {
        
     	 AggregationModule2 module =  AggregationModule2.getInstance();
   	    try {
   			module.setUp();
   		} catch (Exception e1) {
   			// TODO Auto-generated catch block
   			e1.printStackTrace();
   		}
  		pubreport=module.countbenchmarkfingerprintChannelDateHourwise(startdate, enddate, channel_name);
  		return pubreport;
     }
    
    
    
    if (queryfield.equals("uniqueVisitors")) {
        
      	 AggregationModule2 module =  AggregationModule2.getInstance();
    	    try {
    			module.setUp();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
   		pubreport=module.countfingerprintChannel(startdate, enddate, channel_name); 
   		return pubreport;
      }
    
 
    if (queryfield.equals("benchmarkuniquevisitors")) {
        
     	 AggregationModule2 module =  AggregationModule2.getInstance();
   	    try {
   			module.setUp();
   		} catch (Exception e1) {
   			// TODO Auto-generated catch block
   			e1.printStackTrace();
   		}
  		pubreport=module.countbenchmarkfingerprintChannel(startdate, enddate, channel_name);
  		return pubreport;
     }
    
    
           if (queryfield.equals("reforiginal")) {

        	   String data0= null;
               String data1= null;   
               String data2 = null;
               String data3 = null;
               String data4 = null;
        	   String data5= null;
               pubreport.clear();
        	   
			
				PublisherReport obj = new PublisherReport();

				
					data0 = "http://m.facebook.com";
					data1 = "3026.0";
				    data2 = "Social";
				    data3 = "305";
				    data4 = "110";
				    data5 = "facebook.com";
				
				    obj.setReferrerMasterDomain(data0);
					
					obj.setReferrerType(data2);
				    obj.setShares(data3);
				    obj.setLikes(data4);
				    
				    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						{
				    	
				    	 obj.setCount(data1);
						  
						}
						if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						{
							
							obj.setEngagementTime(data1);
						}
						if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						{
							
							obj.setVisitorCount(data1);
					
						}	    
				    
				    

		        PublisherReport obj1 = new PublisherReport();
					
				    data0 = "http://www.facebook.com";
					data1 = "1001.0";
				    data2 = "Social";
				    data3=  "207";
				    data4 = "53";
			        data5 = "facebook.com";
				
			        obj1.setReferrerMasterDomain(data0);
					
					obj1.setReferrerType(data2);
				    obj1.setShares(data3);
				    obj1.setLikes(data4);
				    
				    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						{
				    	
				    	 obj1.setCount(data1);
						  
						}
						if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						{
							
							obj1.setEngagementTime(data1);
						}
						if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						{
							
							obj1.setVisitorCount(data1);
					
						}	
			        
			        
			        
			        
			        PublisherReport obj2 = new PublisherReport();
				
					data0 = "http://l.facebook.com";
				  	data1 = "360.0";
				    data2 = "Social";
				    data3 = "103";
				    data4 = "12";
			        data5 = "facebook.com";
				
			        
			        obj2.setReferrerMasterDomain(data0);
					
					obj2.setReferrerType(data2);
				    obj2.setShares(data3);
				    obj2.setLikes(data4);
				    
				    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						{
				    	
				    	 obj2.setCount(data1);
						  
						}
						if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						{
							
							obj2.setEngagementTime(data1);
						}
						if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						{
							
							obj2.setVisitorCount(data1);
					
						}	
			        
			        
			        
			   PublisherReport obj3 = new PublisherReport();
					data0 = "http://www.google.co.pk";
					data1 = "396.0";
				    data2 = "Search";
				    data3 = "0";
				    data4 = "0";
				    data5 = "google.com";
				
				    obj3.setReferrerMasterDomain(data0);
					
					obj3.setReferrerType(data2);
				    obj3.setShares(data3);
				    obj3.setLikes(data4);
				    
				    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						{
				    	
				    	 obj3.setCount(data1);
						  
						}
						if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						{
							
							obj3.setEngagementTime(data1);
						}
						if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						{
							
							obj3.setVisitorCount(data1);
					
						}	   
			
				
			   PublisherReport obj4 = new PublisherReport();	    
					data0 = "http://www.google.co.in";
					data1 = "2871.0";
				    data2 = "Search";
				    data3 = "0";
				    data4 = "0";
				    data5 = "google.com";
				
				
                obj4.setReferrerMasterDomain(data0);
				
				obj4.setReferrerType(data2);
			    obj4.setShares(data3);
			    obj4.setLikes(data4);
			    
			    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					{
			    	
			    	 obj4.setCount(data1);
					  
					}
					if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
					{
						
						obj4.setEngagementTime(data1);
					}
					if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					{
						
						obj4.setVisitorCount(data1);
				
					}	
			
				
					PublisherReport obj5 = new PublisherReport();

					
				//	data0 = "http://m.facebook.com";
					data1 = "4387.0";
				    data2 = "Social";
				    data3 = "615";
				    data4 = "175";
				    data0 = "facebook.com";
					
				    obj5.setReferrerMasterDomain(data0);
					
					obj5.setReferrerType(data2);
				    obj5.setShares(data3);
				    obj5.setLikes(data4);
				    
				    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						{
				    	
				    	 obj5.setCount(data1);
						  
						}
						if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						{
							
							obj5.setEngagementTime(data1);
						}
						if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						{
							
							obj5.setVisitorCount(data1);
					
						}	
				
				    
				    PublisherReport obj6 = new PublisherReport();

					
				   // data0 = "http://www.google.co.in";
					data1 = "3267.0";
				    data2 = "Search";
				    data3 = "0";
				    data4 = "0";
				    data0 = "google.com";
				
                    obj6.setReferrerMasterDomain(data0);
					
					obj6.setReferrerType(data2);
				    obj6.setShares(data3);
				    obj6.setLikes(data4);
				    
				    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						{
				    	
				    	 obj6.setCount(data1);
						  
						}
						if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						{
							
							obj6.setEngagementTime(data1);
						}
						if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						{
							
							obj6.setVisitorCount(data1);
					
						}	
					
				    
				String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);
				obj.setChannelName(channel_name1);
				obj1.setChannelName(channel_name1);
				obj2.setChannelName(channel_name1);
				obj3.setChannelName(channel_name1);
				obj4.setChannelName(channel_name1);
				obj5.setChannelName(channel_name1);
				obj6.setChannelName(channel_name1);
				
				obj5.getChildren().add(obj);
				obj5.getChildren().add(obj1);
				obj5.getChildren().add(obj2);
				
				obj6.getChildren().add(obj3);
				obj6.getChildren().add(obj4);
				
				
				
				pubreport.add(obj5);
				pubreport.add(obj6);

			

		}
    
        
           if (queryfield.equals("Author")) {

        	   String data0= null;
               String data1= null;   
        	   pubreport.clear();
        	   
        	   for (int i = 0; i < 6; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			        
			       
			          //if(data1[0].equals()) 
			         
			          if(i == 0){
			          data0="kishore";
			          data1 = "10078.0";
			          }
			          

			          if(i == 1){
			          data0="Medha Chawla";
			          data1 = "5097.0";
			          }
			          
			          
			          if(i == 2){
				          data0="Prabhleen Kaur";
				          data1 = "9231.0";
				      }
				    
			        
			          if(i == 3){
				          data0="Admin";
				          data1 = "10065.0";
				          }
				          

				          if(i == 4){
				          data0="Neha Nagpal";
				          data1 = "10031.0";
				          }
				          
				          
				          if(i == 5){
					          data0="Suman Bajpai";
					          data1 = "2021.0";
					      }
			          
			          
			          
			          obj.setArticleAuthor(data0);
			          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);			        
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			   }
    
           }
           
           
           
           
           
           
           
           
           
           
           
           
           if (queryfield.equals("tag")) {

        	   String data0= null;
               String data1= null;   
        	   pubreport.clear();
        	   
        	   for (int i = 0; i < 6; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			        
			       
			          //if(data1[0].equals()) 
			         
			          if(i == 0){
			          data0="womenlifestyle";
			          data1 = "20078.0";
			          }
			          

			          if(i == 1){
			          data0="hindibollywood";
			          data1 = "5093.0";
			          }
			          
			          
			          if(i == 2){
				          data0="politics";
				          data1 = "9678.0";
				      }
				    
			        
			          if(i == 3){
				          data0="household";
				          data1 = "10091.0";
				          }
				          

				          if(i == 4){
				          data0="fashion";
				          data1 = "10061.0";
				          }
				          
				          
				          if(i == 5){
					          data0="gaming";
					          data1 = "4045.0";
					      }
			          
			          
			          
			          obj.setArticleTags(data0);
			          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);			        
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			   }
    
           }
           
           
           
           
           if (queryfield.equals("section")) {

        	   String data0= null;
               String data1= null;   
        	   pubreport.clear();
        	   
        	   for (int i = 0; i < 6; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			        
			       
			          //if(data1[0].equals()) 
			         
			          if(i == 0){
			          data0="entertainment";
			          data1 = "10019.0";
			          }
			          

			          if(i == 1){
			          data0="fashion";
			          data1 = "2043.0";
			          }
			          
			          
			          if(i == 2){
				          data0="food";
				          data1 = "5678.0";
				      }
				    
			        
			          if(i == 3){
				          data0="lifestyle";
				          data1 = "10090.0";
				          }
				          

				          if(i == 4){
				          data0="trending";
				          data1 = "2061.0";
				          }
				          
				          
				          if(i == 5){
					          data0="biztech";
					          data1 = "3098.0";
					      }
			          
			          
			          
			          obj.setSection(data0);
			          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);			        
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			   }
    
           }
           
           
           
           /*  
           
           if (queryfield.equals("device")) {

        	   String data0= null;
               String data1= null;   
        	   pubreport.clear();
        	   
        	   for (int i = 0; i < 3; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			        
			       
			          //if(data1[0].equals()) 
			         
			          if(i == 0){
			          data0="Mobile";
			          data1 = "10005.0";
			          }
			          

			          if(i == 1){
			          data0="Tablet";
			          data1 = "2067.0";
			          }
			          
			          
			          if(i == 2){
				          data0="Desktop";
				          data1 = "3045.0";
				      }
				    
			        
			          obj.setDevice_type(data0);
			          obj.setCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);			        
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			   }
    
           }
    
           if (queryfield.equals("incomelevel")) {

        	   String data0= null;
               String data1= null;   
        	   pubreport.clear();
           
           for (int i = 0; i < 3; i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		       // String[] data1 = ((String)lines1.get(i)).split(",");
		       
		          //if(data1[0].equals()) 
		         
		          if(i == 0){
		          data0="Medium";
		          data1 = "10007.0";
		          }
		          

		          if(i == 1){
		          data0="High";
		          data1 = "3051.0";
		          }
		          
		          
		          if(i == 2){
			          data0="Low";
			          data1 = "1056.0";
			      }
			    
		        
		          obj.setIncomelevel(data0);
		          obj.setCount(data1);
		          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);

		          pubreport.add(obj);
		        
		   //   }
		    //  System.out.println(headers1);
		    //  System.out.println(lines1);
		      }
           }
         */  
           if(queryfield.equals("engagementTime"))	
           {
        	   
        	   AggregationModule2 module =  AggregationModule2.getInstance();
          	    try {
          			module.setUp();
          		} catch (Exception e1) {
          			// TODO Auto-generated catch block
          			e1.printStackTrace();
          		}
         		pubreport=module.engagementTimeChannel(startdate, enddate, channel_name);
         		return pubreport;
           }
           	
           if(queryfield.equals("benchmarkengagementTime"))	
           {
        	   
        	   AggregationModule2 module =  AggregationModule2.getInstance();
          	    try {
          			module.setUp();
          		} catch (Exception e1) {
          			// TODO Auto-generated catch block
          			e1.printStackTrace();
          		}
         		pubreport=module.benchmarkengagementTimeChannel(startdate, enddate, channel_name);
         		return pubreport;
           }
           
           
           
           
           if(queryfield.equals("engagementTimeDatewise"))	
           {
        	   
        	   
        	   AggregationModule2 module =  AggregationModule2.getInstance();
       	    try {
       			module.setUp();
       		} catch (Exception e1) {
       			// TODO Auto-generated catch block
       			e1.printStackTrace();
       		}
      		pubreport=module.engagementTimeChannelDatewise(startdate, enddate, channel_name);
      		return pubreport;
           
           }
           
           if(queryfield.equals("benchmarkengagementTimeDatewise"))	
           {
        	   
        	   AggregationModule2 module =  AggregationModule2.getInstance();
          	    try {
          			module.setUp();
          		} catch (Exception e1) {
          			// TODO Auto-generated catch block
          			e1.printStackTrace();
          		}
         		pubreport=module.benchmarkengagementTimeChannelDatewise(startdate, enddate, channel_name);
         		return pubreport;
           }
           
           
           
           if(queryfield.equals("engagementTimeHourwise"))	
           {
        	   
        	   
        	   AggregationModule2 module =  AggregationModule2.getInstance();
       	    try {
       			module.setUp();
       		} catch (Exception e1) {
       			// TODO Auto-generated catch block
       			e1.printStackTrace();
       		}
      		pubreport=module.benchmarkengagementTimeChannelDateHourwise(startdate, enddate, channel_name);
      		return pubreport;
           
           }
           
           	
           if(queryfield.equals("benchmarkengagementTimeHourwise"))	
           {
        	   
        	   
        	   AggregationModule2 module =  AggregationModule2.getInstance();
       	    try {
       			module.setUp();
       		} catch (Exception e1) {
       			// TODO Auto-generated catch block
       			e1.printStackTrace();
       		}
      		pubreport=module.engagementTimeChannelDateHourwise(startdate, enddate, channel_name);
      		return pubreport;
           
           }
           
           
           
           if(queryfield.equals("minutesVisitor"))	
           {
           	pubreport.clear();
           	PublisherReport obj1 = new PublisherReport();
           	Random random = new Random();	
               Integer randomNumber = random.nextInt(10) + 1;
              String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj1.setChannelName(channel_name1);
           	obj1.setMinutesperVisitor(randomNumber.toString());
           	pubreport.add(obj1);
               return pubreport;
           }
           
           
           
           
           
           
           if (queryfield.equals("referrerType")) {

        	   String data0= null;
               String data1= null;   
        	   pubreport.clear();
           
           for (int i = 0; i < 3; i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		       // String[] data1 = ((String)lines1.get(i)).split(",");
		       
		          //if(data1[0].equals()) 
		         
		          if(i == 0){
		          data0="Social";
		          data1 = "10007.0";
		          }
		          

		          if(i == 1){
		          data0="Search";
		          data1 = "3051.0";
		          }
		          
		          
		          if(i == 2){
			          data0="Direct";
			          data1 = "1056.0";
			      }
			    
		        
		          obj.setReferrerSource(data0);
		          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						obj.setCount(data1);
						if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						obj.setEngagementTime(data1);
						if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						obj.setVisitorCount(data1);
		          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);

		          pubreport.add(obj);
		        
		   //   }
		    //  System.out.println(headers1);
		    //  System.out.println(lines1);
		      }
           }       
      
    return pubreport;
  }
  
  
  public List<PublisherReport> getQueryFieldChannelLive(String queryfield,String startdate, String enddate, String channel_name, String filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
	  
	  
	   String query = "";
      
      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
      query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
	    
      if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
      query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
	    
      if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
      query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "' GROUP by "+ queryfield+"";  
      		
		    
		    
		    
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		   

		    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		    lines = processList(lines);
		    
		    
		    
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        try{
		    	 
		    	  PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            
		            
		        
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		            if(queryfield.equals("state"))
	            	{
	            	
	            	data[0]=data[0].replace("_", " ");
	            	 data[0] = capitalizeString(data[0]);
	            	obj.setState(data[0]);
	            	}
	            
	            
	            if(queryfield.equals("country"))
	        	  {
	        	
	            	data[0]=data[0].replace("_", " ");
	            	data[0] = capitalizeString(data[0]);
	            	obj.setCountry(data[0]);
	             	}
		            
		            
	            if(queryfield.equals("authorName")){
	        		
	        		obj.setArticleAuthor(data[0]);
	        		
	        	}
	        	
                 if(queryfield.equals("tag")){
		        		
		        		obj.setArticleTags(data[0]);
		        	}
                 
                 
		            if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			continue;
		        		}
		        		
		        		} 
		        	
		        	if(queryfield.equals("audience_segment"))
		             {
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		        
		         	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]);
		        	}
		         	
		        	if(queryfield.equals("brandName"))
			          {
		        		data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
			          }
		        
		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	

		            Random random = new Random();	
		            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
		            Integer max = (int)Double.parseDouble(data[1]);
		            Integer randomNumber1 = random.nextInt(max) + 1;
		            
		            if(queryfield.equals("audience_segment"))	
		            {
		            obj.setCount(data[1]); 	
		            obj.setExternalWorldCount(randomNumber.toString());	
		            obj.setVisitorCount(randomNumber1.toString());
		            obj.setAverageTime("0.0");	
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
		            
			        pubreport.add(obj);
		            
		            }
		            else if(queryfield.equals("agegroup")==true) {
		            	
		            	if(data[0].contains("medium")==false){
		            		    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		 		            	obj.setCount(data[1]);
		 		            
		 		                if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
		 		                obj.setEngagementTime(data[1]);
		 		           
		 		                if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		 		                obj.setVisitorCount(data[1]);
		            		String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		  		            
		  		            
		      		        pubreport.add(obj);
		            	}
		            }
		           		            
		            else{
		            	        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				            	obj.setCount(data[1]);
				            
				                if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
				                obj.setEngagementTime(data[1]);
				           
				                if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
				                obj.setVisitorCount(data[1]);
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
		            
			        pubreport.add(obj);
		            
		            
		            }
		          
		        }
		        catch(Exception e)
		        {
		        	continue;
		        }
		        }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    
		    
		    if (queryfield.equals("LatLong")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countLatLongChannelLive(startdate, enddate, channel_name);
		  		return pubreport;
		    }
		      
		    if (queryfield.equals("postalcode")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countPinCodeChannelLive(startdate, enddate, channel_name);
		  		return pubreport;
		    }
		      
		    
		    if (queryfield.equals("cityOthers")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countCityChannelLive(startdate, enddate, channel_name, filter);
		  		return pubreport;
		    }
		    
		    
		    if (queryfield.equals("stateOthers")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countStateChannelLive(startdate, enddate, channel_name, filter);
		  		return pubreport;
		    }
		    
		    if (queryfield.equals("countryOthers")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countCountryChannelLive(startdate, enddate, channel_name, filter);
		  		return pubreport;
		    }
		    
		    
		   
		    if (queryfield.equals("newContent")) {
		        
		    	 AggregationModule2 module =  AggregationModule2.getInstance();
		 	    try {
		 			module.setUp();
		 		} catch (Exception e1) {
		 			// TODO Auto-generated catch block
		 			e1.printStackTrace();
		 		}
				pubreport=module.getNewContentCountChannelLive(startdate, enddate, channel_name);
				return pubreport;
		    }	
		   
		   	    
		   
		    if (queryfield.equals("newVisitors")) {
			    
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
				pubreport=module.countNewUsersChannelLiveDatewise(startdate, enddate, channel_name,filter); 
				return pubreport;
		    }
		    
		    if (queryfield.equals("returningVisitors")) {
		        
		    	 AggregationModule2 module =  AggregationModule2.getInstance();
		 	    try {
		 			module.setUp();
		 		} catch (Exception e1) {
		 			// TODO Auto-generated catch block
		 			e1.printStackTrace();
		 		}
				pubreport=module.countReturningUsersChannelLiveDatewise(startdate, enddate, channel_name,filter); 
				return pubreport;
		    }
		    
		    if (queryfield.equals("LoyalVisitors")) {
		        
		    	 AggregationModule2 module =  AggregationModule2.getInstance();
		  	    try {
		  			module.setUp();
		  		} catch (Exception e1) {
		  			// TODO Auto-generated catch block
		  			e1.printStackTrace();
		  		}
		 		pubreport=module.countLoyalUsersChannelLiveDatewise(startdate, enddate, channel_name,filter); 
		 		return pubreport;
		    }
		    
		   	    
		    
		    
		    if (queryfield.equals("visitorType")) {
		    
		    	List<PublisherReport> pubreport1 = new ArrayList<PublisherReport>();
		    	List<PublisherReport> pubreport2 = new ArrayList<PublisherReport>();
		    	List<PublisherReport> pubreport3 = new ArrayList<PublisherReport>();
		    	
		    	
		    	AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
				pubreport1=module.countNewUsersChannelLiveDatewise(startdate, enddate, channel_name,filter); 
				
				
				pubreport2=module.countReturningUsersChannelLiveDatewise(startdate, enddate, channel_name,filter); 
				
		 		pubreport3=module.countLoyalUsersChannelLiveDatewise(startdate, enddate, channel_name,filter); 
		 		
		 		pubreport1.addAll(pubreport2);
		 		pubreport1.addAll(pubreport3);
		 		
		 		return pubreport1;
		    }
		    
		   
		    if(queryfield.equals("engagementTime"))	
	        {
		    	pubreport.clear();
		    	Random random = new Random();	
	            Integer randomNumber = random.nextInt(1500 + 1 - 500) + 500;
	            
	            PublisherReport obj1 = new PublisherReport();
	           String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj1.setChannelName(channel_name1);
	        	
	        	obj1.setEngagementTime(randomNumber.toString());
	            pubreport.add(obj1);
	            return pubreport;
	        
	        }
	        	
	        	
	        if(queryfield.equals("minutesVisitor"))	
	        {
	        	pubreport.clear();
	        	PublisherReport obj1 = new PublisherReport();
	        	Random random = new Random();	
	            Integer randomNumber = random.nextInt(10) + 1;
	           String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj1.setChannelName(channel_name1);
	        	obj1.setMinutesperVisitor(randomNumber.toString());
	        	pubreport.add(obj1);
	            return pubreport;
	        }
	        	
		    
		    
		    
		    if (queryfield.equals("totalViews")) {
		        
		   	 AggregationModule2 module =  AggregationModule2.getInstance();
		 	    try {
		 			module.setUp();
		 		} catch (Exception e1) {
		 			// TODO Auto-generated catch block
		 			e1.printStackTrace();
		 		}
				pubreport=module.counttotalvisitorsChannelLive(startdate, enddate, channel_name);
				return pubreport;
		   }
		    
		      
		           
		    if (queryfield.equals("uniqueVisitors")) {
		        
		      	 AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		   		pubreport=module.countUniqueVisitorsChannelLive(startdate, enddate, channel_name); 
		   		return pubreport;
		      }
		    
		    
		    
		    
		           if (queryfield.equals("reforiginal")) {

		        	   String data0= null;
		               String data1= null;  
		               String data2 = null;
		        	   pubreport.clear();
		        	   
					for (int i = 0; i < 5; i++) {
						PublisherReport obj = new PublisherReport();

						if (i == 0) {
							data0 = "http://m.facebook.com";
							data1 = "1006.0";
						    data2 = "Social";
						}

						if (i == 1) {
							data0 = "http://www.facebook.com";
							data1 = "1010.0";
						    data2 = "Social";
						}

						if (i == 2) {
							data0 = "http://l.facebook.com";
							data1 = "360.0";
						    data2 = "Social";
						}

						if (i == 3) {
							data0 = "http://www.google.co.pk";
							data1 = "48.0";
						    data2 = "Search";
						}

						if (i == 4) {
							data0 = "http://www.google.co.in";
							data1 = "4871.0";
						    data2 = "Search";
						}

						obj.setReferrerSource(data0);
						obj.setReferrerType(data2);
					    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
						String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						pubreport.add(obj);

					}

				}
		    
		          /* 
		           
		           if (queryfield.equals("device")) {

		        	   String data0= null;
		               String data1= null;   
		        	   pubreport.clear();
		        	   
		        	   for (int i = 0; i < 3; i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        
					       
					          //if(data1[0].equals()) 
					         
					          if(i == 0){
					          data0="Mobile";
					          data1 = "10005.0";
					          }
					          

					          if(i == 1){
					          data0="Tablet";
					          data1 = "2067.0";
					          }
					          
					          
					          if(i == 2){
						          data0="Desktop";
						          data1 = "3045.0";
						      }
						    
					        
					          obj.setDevice_type(data0);
					          obj.setCount(data1);
					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);			        
					          pubreport.add(obj);
					        
					   //   }
					    //  System.out.println(headers1);
					    //  System.out.println(lines1);
					   }
		    
		           }
		    
		           if (queryfield.equals("incomelevel")) {

		        	   String data0= null;
		               String data1= null;   
		        	   pubreport.clear();
		           
		           for (int i = 0; i < 3; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				       // String[] data1 = ((String)lines1.get(i)).split(",");
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="Medium";
				          data1 = "10007.0";
				          }
				          

				          if(i == 1){
				          data0="High";
				          data1 = "3051.0";
				          }
				          
				          
				          if(i == 2){
					          data0="Low";
					          data1 = "1056.0";
					      }
					    
				        
				          obj.setIncomelevel(data0);
				          obj.setCount(data1);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);

				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				      }
		           }
		          */
		           
		           
		           if (queryfield.equals("referrerType")) {

		        	   String data0= null;
		               String data1= null;   
		        	   pubreport.clear();
		           
		           for (int i = 0; i < 3; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				       // String[] data1 = ((String)lines1.get(i)).split(",");
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="Social";
				          data1 = "1047.0";
				          }
				          

				          if(i == 1){
				          data0="Search";
				          data1 = "6032.0";
				          }
				          
				          
				          if(i == 2){
					          data0="Direct";
					          data1 = "1011.0";
					      }
					    
				        
				          obj.setReferrerSource(data0);
				          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(data1);
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(data1);
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(data1);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);

				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				      }
		           }       
		      
		    return pubreport;
		  }
		  
  
  
  
  /*
  public List<PublisherReport> getQueryFieldChannelFilter(String queryfield,String startdate, String enddate, String channel_name, Map<String,String>filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	        int size = filter.size();
	        String queryfilterbuilder = "";
	        String formattedString = "";
	        String query = "";
	        int j =0;
	        for (Map.Entry<String, String> entry : filter.entrySet())
	        {
	        	if (j==0){
	                formattedString = addCommaString(entry.getValue());
	        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
	        	
	        	}
	            else{
	            formattedString = addCommaString(entry.getValue());	
	            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
	       
	            }
	            j++;
	         
	        }
	  
	  
	        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
	        query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
		    
	        if(filter != null && !filter.isEmpty() && filter.equals("engagementTime"))
	        query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
	        
	        if(filter != null && !filter.isEmpty() && filter.equals("visitorCount"))
		    query = "Select "+queryfield+",cookie_id"+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
		        
		       
	        
	        
	        System.out.println(query);
	        CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		   
		    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		        lines = processList(lines);
		    
		    
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        try{
		    	  
		    	  PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e)
		        		{
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("audience_segment"))
		             {
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		     
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
		        	
			        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
			        System.out.println(mobiledeviceproperties[2]);
			        obj.setDevicecode(mobiledeviceproperties[0]);
			        System.out.println(mobiledeviceproperties[0]);
		        	}
		         	
		        	if(queryfield.equals("brandName")){
		        		 data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
		        	}

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	

		            Random random = new Random();	
		            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
		            Integer max = (int)Double.parseDouble(data[1]);
		            Integer randomNumber1 = random.nextInt(max) + 1;
		            
		            if(queryfield.equals("audience_segment"))	
		            {
		            obj.setCount(data[1]); 	
		            obj.setExternalWorldCount(randomNumber.toString());	
		            obj.setVisitorCount(randomNumber1.toString());
		            obj.setAverageTime("0.0");	
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
	            	
			        pubreport.add(obj);
		            
		            }
		           
		            else if(queryfield.equals("agegroup")==true) {
		            	
		            	if(data[0].contains("medium")==false){
		            		if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		        	         	obj.setCount(data[1]);
		        	        
		        	             if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
		        	             obj.setEngagementTime(data[1]);
		        	         
		        	             if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		        	             obj.setVisitorCount(data[1]);
		            		String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            
			            	
		    		        pubreport.add(obj);
		            	}
		            }
		            
		            
		            else{
		            	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
	        	         	obj.setCount(data[1]);
	        	        
	        	             if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
	        	             obj.setEngagementTime(data[1]);
	        	         
	        	             if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
	        	             obj.setVisitorCount(data[1]);
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
	            	
			        pubreport.add(obj);
		            
		            }
		            
		        }
		        catch(Exception e){
		        	
		        	continue;
		        }
		        
		        
		        }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
  */
  
  public List<PublisherReport> getQueryFieldChannelFilter(String queryfield,String startdate, String enddate, String channel_name, Map<String,String>filter, String filtermetric)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	        if(queryfield.equals("userDetails"))
	        {
	        	List<PublisherReport> pubreport = new ArrayList();
	            
	        	AggregationModule2 module =  AggregationModule2.getInstance();
	    	    try {
	    			module.setUp();
	    		} catch (Exception e1) {
	    			// TODO Auto-generated catch block
	    			e1.printStackTrace();
	    		}
	   		PublisherReport obj=module.getUserdetailsChannel(startdate, enddate, channel_name, filter);
	   		
	   		pubreport.add(obj);
	   		
	   		return pubreport;	
	        	
	        }
	  
	        if(queryfield.equals("userDetailsCity"))
	        {
	        	List<PublisherReport> pubreport = new ArrayList();
	        	
	        	AggregationModule2 module =  AggregationModule2.getInstance();
	    	    try {
	    			module.setUp();
	    		} catch (Exception e1) {
	    			// TODO Auto-generated catch block
	    			e1.printStackTrace();
	    		}
	   		pubreport=module.getUserdetailsCityChannel(startdate, enddate, channel_name, filter); 
	   		return pubreport;	
	        	
	        }
	        
	        

	        if(queryfield.equals("userDetailsISP"))
	        {
	        	List<PublisherReport> pubreport = new ArrayList();
	        	AggregationModule2 module =  AggregationModule2.getInstance();
	    	    try {
	    			module.setUp();
	    		} catch (Exception e1) {
	    			// TODO Auto-generated catch block
	    			e1.printStackTrace();
	    		}
	   		pubreport=module.getUserdetailsISPChannel(startdate, enddate, channel_name, filter);
	   		return pubreport;	
	        	
	        }
	        
	        
	        
	        
	        if(queryfield.equals("userDetailsSegment"))
	        {
	        	

	        	List<PublisherReport> pubreport = new ArrayList();
	        	AggregationModule2 module =  AggregationModule2.getInstance();
	    	    try {
	    			module.setUp();
	    		} catch (Exception e1) {
	    			// TODO Auto-generated catch block
	    			e1.printStackTrace();
	    		}
	   		pubreport=module.getUserdetailsSegmentChannel(startdate, enddate, channel_name, filter);
	   		return pubreport;	
	        	
	        }
	  
	        int size = filter.size();
	        String queryfilterbuilder = "";
	        String formattedString = "";
	        String query = "";
	        int j =0;
	        for (Map.Entry<String, String> entry : filter.entrySet())
	        {
	        	if (j==0){
	                formattedString = addCommaString(entry.getValue());
	        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
	        	
	        	}
	            else{
	            formattedString = addCommaString(entry.getValue());	
	            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
	       
	            }
	            j++;
	         
	        }
	  
	  
	        if(filtermetric == null || filtermetric.isEmpty() ||  filtermetric.equals("pageviews"))
	        query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'"+" GROUP BY "+queryfield;
		    
	        if(filtermetric != null && !filtermetric.isEmpty() && filtermetric.equals("engagementTime"))
	        query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'"+" GROUP BY "+queryfield;
	        
	        if(filtermetric != null && !filtermetric.isEmpty() && filtermetric.equals("visitorCount"))
		    query = "Select "+queryfield+",cookie_id"+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'"+" GROUP BY "+queryfield+"";  
    		
		        
		       
	        
	        
	        System.out.println(query);
	        CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		   
		    if(filtermetric != null && !filtermetric.isEmpty()  && filtermetric.equals("visitorCount") )
		        lines = processList(lines);
		    
		    
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        try{
		    	  
		    	  PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e)
		        		{
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("audience_segment"))
		             {
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		     
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
		        	
			        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
			        System.out.println(mobiledeviceproperties[2]);
			        obj.setDevicecode(mobiledeviceproperties[0]);
			        System.out.println(mobiledeviceproperties[0]);
		        	}
		         	
		        	if(queryfield.equals("brandName")){
		        		 data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
		        	}

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	

		            Random random = new Random();	
		            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
		            Integer max = (int)Double.parseDouble(data[1]);
		            Integer randomNumber1 = random.nextInt(max) + 1;
		            
		            if(queryfield.equals("audience_segment"))	
		            {
		            obj.setCount(data[1]); 	
		            obj.setExternalWorldCount(randomNumber.toString());	
		            obj.setVisitorCount(randomNumber1.toString());
		            obj.setAverageTime("0.0");	
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
	            	
			        pubreport.add(obj);
		            
		            }
		           
		            else if(queryfield.equals("agegroup")==true) {
		            	
		            	if(data[0].contains("medium")==false){
		            		if(filtermetric == null || filtermetric.isEmpty() || filtermetric.equals("pageviews"))
		        	         	obj.setCount(data[1]);
		        	        
		        	             if(filtermetric != null && !filtermetric.isEmpty() && filtermetric.equals("engagementTime") )
		        	             obj.setEngagementTime(data[1]);
		        	         
		        	             if(filtermetric != null && !filtermetric.isEmpty()  && filtermetric.equals("visitorCount") )
		        	             obj.setVisitorCount(data[1]);
		            		String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            
			            	
		    		        pubreport.add(obj);
		            	}
		            }
		            
		            
		            else{
		            	if(filtermetric == null || filtermetric.isEmpty() ||  filtermetric.equals("pageviews"))
	        	         	obj.setCount(data[1]);
	        	        
	        	        if(filtermetric != null && !filtermetric.isEmpty() && filtermetric.equals("engagementTime") )
	        	             obj.setEngagementTime(data[1]);
	        	         
	        	        if(filtermetric != null && !filtermetric.isEmpty()  && filtermetric.equals("visitorCount") )
	        	             obj.setVisitorCount(data[1]);
		            
	        	        
	        	        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
	            	
			        pubreport.add(obj);
		            
		            }
		            
		        }
		        catch(Exception e){
		        	
		        	continue;
		        }
		        
		        
		        }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }

  public List<PublisherReport> getQueryFieldChannelFilterLive(String queryfield,String startdate, String enddate, String channel_name, Map<String,String>filter, String filterMetric)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	        int size = filter.size();
	        String queryfilterbuilder = "";
	        String formattedString = "";
	        int j =0;
	        for (Map.Entry<String, String> entry : filter.entrySet())
	        {
	        	if (j==0){
	                formattedString = addCommaString(entry.getValue());
	        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
	        	
	        	}
	            else{
	            formattedString = addCommaString(entry.getValue());	
	            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
	       
	            }
	            j++;
	         
	        }
	  
	  
	        
          String query = "";
	        
	        if(filterMetric == null || filterMetric.isEmpty() || filterMetric.equals("pageviews"))
	        query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
		    
	        
	        if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("engagementTime"))
		    query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
			    
	 
	        if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("visitorCount"))
		    query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "' GROUP by "+ queryfield+"";  
		    
	        System.out.println(query);
	        CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		    
		    if(filterMetric != null && !filterMetric.isEmpty()  && filterMetric.equals("visitorCount") )
		        lines = processList(lines);
		    
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        try{
		    	  
		    	  PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e)
		        		{
		        			continue;
		        		}
		        		
		        		}
		        	
		        	
		        	if(queryfield.equals("audience_segment"))
		        	{
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		        	}
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		     
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
		        	
			        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
			        System.out.println(mobiledeviceproperties[2]);
			        obj.setDevicecode(mobiledeviceproperties[0]);
			        System.out.println(mobiledeviceproperties[0]);
		        	}
		         	
		        	if(queryfield.equals("brandName")){
		        		 data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
		        	}

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	

		            Random random = new Random();	
		            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
		            Integer max = (int)Double.parseDouble(data[1]);
		            Integer randomNumber1 = random.nextInt(max) + 1;
		            
		            if(queryfield.equals("audience_segment"))	
		            {
		            obj.setCount(data[1]); 	
		            obj.setExternalWorldCount(randomNumber.toString());	
		            obj.setVisitorCount(randomNumber1.toString());
		            obj.setAverageTime("0.0");	
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
	            	
			        pubreport.add(obj);
		            
		            }
		            
		            else if(queryfield.equals("agegroup")==true) {
		            	
		            	if(data[0].contains("medium")==false){
		            		if(filterMetric == null || !filterMetric.isEmpty() || filterMetric.equals("pageviews"))
		        	         	obj.setCount(data[1]);
		        	        
		        	             if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("engagementTime") )
		        	             obj.setEngagementTime(data[1]);
		        	         
		        	             if(filterMetric != null && !filterMetric.isEmpty()  && filterMetric.equals("visitorCount") )
		        	             obj.setVisitorCount(data[1]);
		            		String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            
			            	
		    		        pubreport.add(obj);
		            	}
		            	
		            }
		            		            
		            else{
		            	if(filterMetric == null || filterMetric.isEmpty() ||  filterMetric.equals("pageviews"))
	        	         	obj.setCount(data[1]);
	        	        
	        	             if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("engagementTime") )
	        	             obj.setEngagementTime(data[1]);
	        	         
	        	             if(filterMetric != null && !filterMetric.isEmpty()  && filterMetric.equals("visitorCount") )
	        	             obj.setVisitorCount(data[1]);
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
	            	
			        pubreport.add(obj);
		            
		            }
		           
		        }
		        catch(Exception e){
		        	
		        	continue;
		        }
		        
		        
		        }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }


  
  
  /*
  public List<PublisherReport> getQueryFieldChannelFilterLive(String queryfield,String startdate, String enddate, String channel_name, Map<String,String>filter, String filterMetric)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	        int size = filter.size();
	        String queryfilterbuilder = "";
	        String formattedString = "";
	        int j =0;
	        for (Map.Entry<String, String> entry : filter.entrySet())
	        {
	        	if (j==0){
	                formattedString = addCommaString(entry.getValue());
	        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
	        	
	        	}
	            else{
	            formattedString = addCommaString(entry.getValue());	
	            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
	       
	            }
	            j++;
	         
	        }
	  
	  
	        
            String query = "";
	        
	        if(filterMetric == null || filterMetric.isEmpty() || filterMetric.equals("pageviews"))
	        query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
		    
	        
	        if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("engagementTime"))
		    query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
			    
	 
	        if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("visitorCount"))
		    query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "' GROUP by "+ queryfield+"";  
		    
	        System.out.println(query);
	        CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		    
		    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		        lines = processList(lines);
		    
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        try{
		    	  
		    	  PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e)
		        		{
		        			continue;
		        		}
		        		
		        		}
		        	
		        	
		        	if(queryfield.equals("audience_segment"))
		        	{
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		        	}
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		     
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
		        	
			        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
			        System.out.println(mobiledeviceproperties[2]);
			        obj.setDevicecode(mobiledeviceproperties[0]);
			        System.out.println(mobiledeviceproperties[0]);
		        	}
		         	
		        	if(queryfield.equals("brandName")){
		        		 data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
		        	}

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	

		            Random random = new Random();	
		            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
		            Integer max = (int)Double.parseDouble(data[1]);
		            Integer randomNumber1 = random.nextInt(max) + 1;
		            
		            if(queryfield.equals("audience_segment"))	
		            {
		            obj.setCount(data[1]); 	
		            obj.setExternalWorldCount(randomNumber.toString());	
		            obj.setVisitorCount(randomNumber1.toString());
		            obj.setAverageTime("0.0");	
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
	            	
			        pubreport.add(obj);
		            
		            }
		            
		            else if(queryfield.equals("agegroup")==true) {
		            	
		            	if(data[0].contains("medium")==false){
		            		if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		        	         	obj.setCount(data[1]);
		        	        
		        	             if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
		        	             obj.setEngagementTime(data[1]);
		        	         
		        	             if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		        	             obj.setVisitorCount(data[1]);
		            		String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            
			            	
		    		        pubreport.add(obj);
		            	}
		            	
		            }
		            		            
		            else{
		            	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
	        	         	obj.setCount(data[1]);
	        	        
	        	             if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
	        	             obj.setEngagementTime(data[1]);
	        	         
	        	             if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
	        	             obj.setVisitorCount(data[1]);
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
	            	
			        pubreport.add(obj);
		            
		            }
		           
		        }
		        catch(Exception e){
		        	
		        	continue;
		        }
		        
		        
		        }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
*/
  
  
  

  public static String convert(List<String> list) {
	    String res = "";
	    for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
	        res += iterator.next() + (iterator.hasNext() ? "," : "");
	    }
	    return res;
	}
  
  public static String addCommaString(String value) {
	    String res = "";
	    String [] parts = value.split("~");
	    for(int i =0; i<parts.length; i++){
	    	
	    	res = res+"'"+parts[i]+"'"+",";
	    	
	    }
        res = res.substring(0,res.length()-1);
       return res;
  }
  
  
  public List<PublisherReport> getQueryFieldChannelGroupBy(String queryfield,String startdate, String enddate, String channel_name, List<String> groupby, String filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	      
	  String querygroupbybuilder = convert(groupby);
      List<PublisherReport> pubreport = new ArrayList();
      String query = "";
      
      int  l=0;
      
      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    	 	query = "Select count(*),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
    	    
    	    
    	    if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime"))
    	     	query = "Select SUM(engagementTime),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
    	        
    	    
    	    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount"))
    	     	query = "Select "+queryfield+","+querygroupbybuilder+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' GROUP by "+ queryfield+","+querygroupbybuilder+"";  
    	        

    	    if(querygroupbybuilder.equals("hour")){
    	    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    	    	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
    	   
    	    	
    	    }
    	  
    	    if(querygroupbybuilder.equals("minute")){
    	    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    	     	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
    	   		    
    	    } 	
    	    
    	    if(querygroupbybuilder.equals("second")){
    	   // 	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    	    //	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
    	   		    
    	    } 	
    	    
    	    if(querygroupbybuilder.equals("hour")){
    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
    	    	query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
    	   
    	    	
    	    }
    	  
    	    if(querygroupbybuilder.equals("minute")){
    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
    	    	query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
    	   		    
    	    } 	
    	    
    	    if(querygroupbybuilder.equals("second")){
    	  //  	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
    	  //  	query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
    	   		    
    	    } 	
    	    
    	   
    	    if(querygroupbybuilder.equals("hour")){
    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
    	    	query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
    	   
    	    	
    	    }
    	  
    	    if(querygroupbybuilder.equals("minute")){
    	   	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
    	   	query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
    	    	   
    	    } 	
    	    
    	    if(querygroupbybuilder.equals("second")){
    	    //	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
    	    //	query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
    	    	   
    	   		    
    	    } 	
    	    
    	    
    	    
    	    
    	    
    	    
    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("totalViews"))
    	    {
    	    	
    	    	query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
    	    }
    	    
    	    	
    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("totalViews"))
    	    {
    	    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    	    	query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
    	    }
    	    	
    	    if(querygroupbybuilder.equals("second") && queryfield.equals("totalViews"))
    	    {
    	    //	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    	    //	query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
    	    }
    	    
    	    	
    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("uniqueVisitors"))
    	    {
    	    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
    	    	
    	    }
    	 		    
    			    	
    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("uniqueVisitors"))
    	    {
    	    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
    			      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
    	    	
    	    }
    	    	
    	    
    	    if(querygroupbybuilder.equals("second") && queryfield.equals("uniqueVisitors"))
    	    {
    	    //	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
    			//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
    	    	
    	    }
    	    
    	    
    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("engagementTime") )
    	    {
    	    	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where channel_name = '" + 
    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
    	    	
    	    }
    	 		    
    			    	
    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("engagementTime") )
    	    {
    	    	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where channel_name = '" + 
    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
    	    	
    	    }
    	    	
    	    
    	    if(querygroupbybuilder.equals("second") && queryfield.equals("engagementTime") )
    	    {
    	    //	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where channel_name = '" + 
    			//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
    	    	
    	    }
		    	
		    
    	    if(queryfield.equals("visitorType")){
    			
    	        List<PublisherReport> pubreport1  = new ArrayList<PublisherReport>();
    	        List<PublisherReport> pubreport2  = new ArrayList<PublisherReport>();
    	        List<PublisherReport> pubreport3  = new ArrayList<PublisherReport>();
    	        
    		   
    	    	AggregationModule2 module =  AggregationModule2.getInstance();
    	    	    try {
    	    			module.setUp();
    	    		} catch (Exception e1) {
    	    			// TODO Auto-generated catch block
    	    			e1.printStackTrace();
    	    		}
    			
    	    	pubreport1=module.countNewUsersChannelDatewisegroupby(startdate, enddate, channel_name, querygroupbybuilder);
    			
    	    
    			pubreport2=module.countReturningUsersChannelDatewisegroupby(startdate, enddate, channel_name,  querygroupbybuilder); 
    			
    	   
    	 		pubreport3=module.countLoyalUsersChannelDatewisegroupby(startdate, enddate, channel_name,  querygroupbybuilder);
    	 		
    	  
    	        pubreport1.addAll(pubreport2);
    	        pubreport1.addAll(pubreport3);
    	   
    	   
    	        return pubreport1;
    	   }
		    
		    
		    if(queryfield.equals("newVisitors")){
	    		
		    	 AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
				pubreport=module.countNewUsersChannelDatewisegroupby(startdate, enddate, channel_name, querygroupbybuilder);
		    	return pubreport;
	    	}
	       

	    	if(queryfield.equals("returningVisitors")){
	    		
	    		 AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
				pubreport=module.countReturningUsersChannelDatewisegroupby(startdate, enddate, channel_name,  querygroupbybuilder);
				return pubreport;
	    	
	    	
	    	}
	    
	    

	    	if(queryfield.equals("LoyalVisitors")){
	    		
	    		 AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
				pubreport=module.countLoyalUsersChannelDatewisegroupby(startdate, enddate, channel_name,  querygroupbybuilder);
				return pubreport;
	    		
	    	}	
		    	
		   /* 
	    	if(querygroupbybuilder.equals("hour") && queryfield.equals("totalViews"))
		    {
		    	query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
		    }
		    
	     	
		    if(querygroupbybuilder.equals("hour") && queryfield.equals("uniqueVisitors"))
		    {
		    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
					      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
		    	
		    }
		    */
	    	
	    	
	    	
		    System.out.println(query);
		    CSVResult csvResult = getCsvResult(false, query);
		   
		    
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    
		    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
			    lines = processList1(lines);
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		 
		    if(queryfield.equals("audience_segment") && groupby.get(0).equals("subcategory")== true){
		    
		    
		    List<List<String>> data = new ArrayList<List<String>>();
		    for (int i = 0; i < lines.size(); i++) {
	            List<String> objects = new ArrayList<String>();
	            String [] parts = lines.get(i).split(",");
	            for(int j =0; j< parts.length; j++)
	              objects.add(parts[j]);
	           
	            data.add(objects);
	        }
		    
		    
		    ResultSet obj = ListtoResultSet.getResultSet(headers, data);
		    
		    
		//    JSONArray json = Convertor.convertResultSetIntoJSON(obj);
		 //   String s = json.toString();
		    pubreport= NestedJSON.getNestedJSONObject(obj, queryfield, groupby,filter); 
		 //   System.out.println(nestedJson);
		    return pubreport;
		    
		    }
		    
		    
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		       
		    	 try{ 
		    	  PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		        
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		            if(queryfield.equals("state"))
	            	{
	            	
	            	data[0]=data[0].replace("_", " ");
	            	data[0] = capitalizeString(data[0]);
	            	obj.setState(data[0]);
	            	}
	            
	            
	            if(queryfield.equals("country"))
	        	  {
	        	
	            	data[0]=data[0].replace("_", " ");
	            	data[0] = capitalizeString(data[0]);
	            	obj.setCountry(data[0]);
	             	}
	        
		            
		            
		            
		            if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("audience_segment"))
		             {
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		            	
		        			        		        	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		     
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
			          obj.setMobile_device_model_name(data[0]);
			          String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]);
		        	}
		        	
		        	
		        	if(queryfield.equals("brandName")){
		        		 data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
		        	}
		        	

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	
		        	
		        	
		        	//   obj.setCode(code);
	            for(int k = 0; k < groupby.size(); k++)
	            {
	            	
	            	if(groupby.get(k).equals(queryfield)==false)
	            	{
	                try{
	            	if(groupby.get(k).equals("device"))
	            	obj.setDevice_type(data[k+1]);
	            	
	            	 if(groupby.get(k).equals("state"))
	             	{
	             	
	             	data[k+1]=data[k+1].replace("_", " ");
	             	data[k+1] = capitalizeString(data[k+1]);
	             	obj.setState(data[k+1]);
	             	}
	             
	             
	             if(groupby.get(k).equals("country"))
	         	  {
	         	
	             	data[k+1]=data[k+1].replace("_", " ");
	             	data[k+1] = capitalizeString(data[k+1]);
	             	obj.setCountry(data[k+1]);
	              	}
	            	
	            	
	            	
	            	if(groupby.get(k).equals("city")){
	            		try{
	            		String locationproperties = citycodeMap.get(data[k+1]);
	    		        data[k+1]=data[k+1].replace("_"," ").replace("-"," ");
	    		        data[k+1]=capitalizeString(data[k+1]);
	    		        obj.setCity(data[k+1]);
	    		        System.out.println(data[k+1]);
	    		        obj.setLocationcode(locationproperties);
	            		}
	            		catch(Exception e)
	            		{
	            			continue;
	            		}
	            	}
	            	if(groupby.get(k).equals("audience_segment"))
		             {
	            		String audienceSegment = audienceSegmentMap.get(data[k+1]);
	            		String audienceSegmentCode = audienceSegmentMap2.get(data[k+1]);
	            		if(audienceSegment!=null && !audienceSegment.isEmpty()){
	            		obj.setAudience_segment(audienceSegment);
	            		obj.setAudienceSegmentCode(audienceSegmentCode);
	            		}
	            		else
	            	    obj.setAudience_segment(data[k+1]);
	            		
		             }
	            	
	            	
	            	if(groupby.get(k).equals("gender"))
			             obj.setGender(data[k+1]);
	            	
	            	if(groupby.get(k).equals("hour"))
			             obj.setDate(data[k+1]);
	            	
	            	if(groupby.get(k).equals("minute"))
			             obj.setDate(data[k+1]);
	            	
	            	
	            	//if(groupby.get(k).equals("gender"))
			           //  obj.setGender(data[k+1]);
	            	
	            	
	            	if(groupby.get(k).equals("refcurrentoriginal"))
			             obj.setGender(data[k+1]);
		            	
	            	if(groupby.get(k).equals("date"))
			             obj.setDate(data[k+1]);
		            		            	
	            	if(groupby.get(k).equals("subcategory"))
			             {
	            		String audienceSegment = audienceSegmentMap.get(data[k+1]);
	            		String audienceSegmentCode = audienceSegmentMap2.get(data[k+1]);
	            		if(audienceSegment!=null && !audienceSegment.isEmpty()){
	            		obj.setSubcategory(audienceSegment);
	            		obj.setSubcategorycode(audienceSegmentCode);
	            		}
	            		else
	            	    obj.setSubcategory(data[k+1]);
			             }
	            	
	            	if(groupby.get(k).equals("agegroup"))
	            	{
		        		 data[k+1]=data[k+1].replace("_","-");
		        		 data[k+1]=data[k+1]+ " Years";
		        		 if(data[k+1].contains("medium")==false)
		        		 obj.setAge(data[k+1]);
		        	}
		            	
		            	
	            	if(groupby.get(k).equals("incomelevel"))
			          obj.setIncomelevel(data[k+1]);
		            	
                  l++;
	                }
	                catch(Exception e){
	                	continue;
	                }
	                
	                }
	            }
	           
	            
	            	            
	            if(l!=0){
	            	    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		            	obj.setCount(data[l+1]);
		             
			            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		                obj.setVisitorCount(data[l+1]);
		         

			            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
		                obj.setEngagementTime(data[l+1]);
		         
	            
	            
	            }       
			    String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        pubreport.add(obj);
		        l=0;
		    	 }
		    	 catch(Exception e){
		    		 continue;
		    	 }
		    	 
		    	 }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    
		    
		    
		    
		    
		    return pubreport;
		  }

/*
  public List<PublisherReport> getnestedList(List<PublisherReport> obj,String queryfield, List<String>groupby) {
	 
	  Map<PublisherReport,List<PublisherReport>> pubreport = new FastMap<PublisherReport,List<PublisherReport>>();
	  String key="";
	  
	  for(int i=0; i<obj.size(); i++ ){
		  if(queryfield.equals("gender")){
	        	key = obj.get(i).getGender();
	            
	     
	        	
	        	
		  }  	
	        	
	        	if(queryfield.equals("device"))
	            key = obj.get(i).getDevice_type();
	        	
	            if(queryfield.equals("state"))
          	{
          	
          
          
          if(queryfield.equals("country"))
      	  {
      	
           	}
      
	            
	            
	            
	            if(queryfield.equals("city")){
	        		
	        		
	        		}
	        	if(queryfield.equals("audience_segment"))
	             {
	        		
	             }
	        	
	        	if(queryfield.equals("reforiginal"))
		             obj.setReferrerSource(data[0]);
	            	
	        	if(queryfield.equals("agegroup"))
	        	{
	        		
	        		 obj.setAge(data[0]);
	        	}
	            	
	            	
	        			        		        	
	        	if(queryfield.equals("incomelevel"))
		          obj.setIncomelevel(data[0]);
	     
	        	
	        	if(queryfield.equals("system_os")){
	        		
	        	}
	         	
	        	if(queryfield.equals("modelName")){
		          
	        	}
	        	
	        	
	        	if(queryfield.equals("brandName")){
	        		
	        	}
	        	

	        	if(queryfield.equals("refcurrentoriginal"))
	        	{
	        		
	        	}
		  
		  
	  }
	  
  }
   
 */ 
  


public List<PublisherReport> getQueryFieldChannelGroupByLive(String queryfield,String startdate, String enddate, String channel_name, List<String> groupby, String filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	      
	String querygroupbybuilder = convert(groupby);
    
    String query = "";
    
    
    int  l=0;
    
    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
 	query = "Select count(*),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
    
    
    if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime"))
     	query = "Select SUM(engagementTime),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
        
    
    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount"))
     	query = "Select "+queryfield+","+querygroupbybuilder+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder+","+"cookie_id";
        

    if(querygroupbybuilder.equals("hour")){
    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
   
    	
    }
  
    if(querygroupbybuilder.equals("minute")){
    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
   		    
    } 	
    
    if(querygroupbybuilder.equals("second")){
    //	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    //	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
   		    
    } 	
    
    
    
    if(querygroupbybuilder.equals("hour") && queryfield.equals("totalViews"))
    {
    	
    	query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
    }
    
    	
    if(querygroupbybuilder.equals("minute") && queryfield.equals("totalViews"))
    {
    	
    	query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
    }
    	
    if(querygroupbybuilder.equals("second") && queryfield.equals("totalViews"))
    {
    //	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
    //	query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
    }
    
    	
    if(querygroupbybuilder.equals("hour") && queryfield.equals("uniqueVisitors"))
    {
    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
			      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
    	
    }
 		    
		    	
    if(querygroupbybuilder.equals("minute") && queryfield.equals("uniqueVisitors"))
    {
    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
			      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
    	
    }
    	
    
    if(querygroupbybuilder.equals("second") && queryfield.equals("uniqueVisitors"))
    {
    //	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
		//	      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
    	
    }
    
    
    if(querygroupbybuilder.equals("hour") && queryfield.equals("engagementTime"))
    {
    	query = "SELECT count(*) FROM enhanceduserdatabeta1 where channel_name = '" + 
			      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
    	
    }
 		    
		    	
    if(querygroupbybuilder.equals("minute") && queryfield.equals("engagementTime"))
    {
    	query = "SELECT count(*) FROM enhanceduserdatabeta1 where channel_name = '" + 
			      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
    	
    }
    	
    
    if(querygroupbybuilder.equals("second") && queryfield.equals("engagementTime"))
    {
    //	query = "SELECT count(*) FROM enhanceduserdatabeta1 where channel_name = '" + 
		//	      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
    	
    }
    
    
    System.out.println(query);
         	CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		  
		    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
			    lines = processList1(lines);
		    
		    
		    if(queryfield.equals("totalViews"))
		    {
		    	if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
			    {
			      for (int i = 0; i < lines.size(); i++)
			      {
			       
			    	 try{ 
			    	  PublisherReport obj = new PublisherReport();
			    	  String[] data = ((String)lines.get(i)).split(",");
			    	  obj.setDate(data[0]);
			    	  obj.setCount(data[1]);
			    	  pubreport.add(obj);
			    	 
			    	 }
			    	 catch(Exception e) 
			    	 {
			    		 continue;
			    	 }
			      
			      }
			      
			    } 
		    
		       return pubreport;
		    }
		    	
		    if(queryfield.equals("uniqueVisitors"))
		    {
		    	if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
			    {
			      for (int i = 0; i < lines.size(); i++)
			      {
			       
			    	 try{ 
			    	  PublisherReport obj = new PublisherReport();
			    	  String[] data = ((String)lines.get(i)).split(",");
			    	  obj.setDate(data[0]);
			    	  obj.setCount(data[1]);
			    	  pubreport.add(obj);
			    	 
			    	 }
			    	 catch(Exception e) 
			    	 {
			    		 continue;
			    	 }
			      
			      }
			      
			    } 
		    
		       return pubreport;
		    	
		    	
		    }
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		       
		    	 try{ 
		    	  PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		        
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("state"))
	            	{
	            	
	            	data[0]=data[0].replace("_", " ");
	            	data[0] = capitalizeString(data[0]);
	            	obj.setState(data[0]);
	            	}
	            
	            
	            if(queryfield.equals("country"))
	        	  {
	        	
	            	data[0]=data[0].replace("_", " ");
	            	data[0] = capitalizeString(data[0]);
	            	obj.setCountry(data[0]);
	             	}
	        
		            
		            
		            
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			
		        			continue;
		        		}
		        		
		        		}
		        	
		        	if(queryfield.equals("audience_segment"))
		             {
		        		
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		        		
		             }
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		     
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
			          obj.setMobile_device_model_name(data[0]);
			          String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]);
		        	}
		        	
		        	
		        	if(queryfield.equals("brandName"))
		        	{ 
		        		data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
		        	}
		        	

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	
		        	
		        	
		        	//   obj.setCode(code);
	            for(int k = 0; k < groupby.size(); k++)
	            {
	            	
	            	if(groupby.get(k).equals(queryfield)==false)
	            	{
	                try{
	            	if(groupby.get(k).equals("device"))
	            	obj.setDevice_type(data[k+1]);
	            	
	            	if(groupby.get(k).equals("city")){
	            		try{
	            		String locationproperties = citycodeMap.get(data[k+1]);
	    		        data[k+1]=data[k+1].replace("_"," ").replace("-"," ");
	    		        data[k+1] = capitalizeString(data[k+1]);
	    		        obj.setCity(data[k+1]);
	    		        System.out.println(data[k+1]);
	    		        obj.setLocationcode(locationproperties);
	            		}
	            		catch(Exception e)
	            		{
	            			continue;
	            		}
	            	}
	            	
	            	if(groupby.get(k).equals("audience_segment"))
		             {
	            		String audienceSegment = audienceSegmentMap.get(data[k+1]);
	            		String audienceSegmentCode = audienceSegmentMap2.get(data[k+1]);
	            		if(audienceSegment!=null && !audienceSegment.isEmpty()){
	            		obj.setAudience_segment(audienceSegment);
	            		obj.setAudienceSegmentCode(audienceSegmentCode);
	            		}
	            		else
	            	    obj.setAudience_segment(data[k+1]);
	            		
	            		
		             }
	            	
	            	
	            	if(groupby.get(k).equals("gender"))
			             obj.setGender(data[k+1]);
	            	
	            	 if(groupby.get(k).equals("state"))
		             	{
		             	
		             	data[k+1]=data[k+1].replace("_", " ");
		             	data[k+1] = capitalizeString(data[k+1]);
		             	obj.setState(data[k+1]);
		             	}
		             
		             
		             if(groupby.get(k).equals("country"))
		         	  {
		         	
		             	data[k+1]=data[k+1].replace("_", " ");
		             	data[k+1] = capitalizeString(data[k+1]);
		             	obj.setCountry(data[k+1]);
		              	}
	            	
	            	
	            	if(groupby.get(k).equals("hour"))
			             obj.setDate(data[k+1]);
	            	
	            	if(groupby.get(k).equals("minute"))
			             obj.setDate(data[k+1]);
	            	
	            	
	            	
	            	if(groupby.get(k).equals("refcurrentoriginal"))
			             obj.setGender(data[k+1]);
		           
	            	if(groupby.get(k).equals("date"))
			             obj.setDate(data[k+1]);
		            	
	            	
	            	
	            	if(groupby.get(k).equals("subcategory"))
	            	 {
	            		String audienceSegment = audienceSegmentMap.get(data[k+1]);
	            		String audienceSegmentCode = audienceSegmentMap2.get(data[k+1]);
	            		if(audienceSegment!=null && !audienceSegment.isEmpty()){
	            		obj.setSubcategory(audienceSegment);
	            		obj.setSubcategorycode(audienceSegmentCode);
	            		}
	            		else
	            	    obj.setSubcategory(data[k+1]);
			             
	                }
	                
	            	if(groupby.get(k).equals("agegroup"))
	            	{
		        		 data[k+1]=data[k+1].replace("_","-");
		        		 data[k+1]=data[k+1]+ " Years";
		        		 if(data[k+1].contains("medium")==false)
		        		 obj.setAge(data[k+1]);
		        	}
		            	
		            	
	            	if(groupby.get(k).equals("incomelevel"))
			          obj.setIncomelevel(data[k+1]);
		            	
                    l++;
	                }
	                catch(Exception e){
	                	continue;
	                }
	                
	                }
	            }
	           
	            if(l!=0){
	            
	            	        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
			            	obj.setCount(data[l+1]);
			             
				            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
			                obj.setVisitorCount(data[l+1]);
			         

				            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
			                obj.setEngagementTime(data[l+1]);
	            }
	 		    String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        pubreport.add(obj);
		        l=0;
		    	 }
		    	 catch(Exception e){
		    		 continue;
		    	 }
		    	 
		    	 }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
  
  
  
  public List<PublisherReport> getQueryFieldChannelArticle(String queryfield,String startdate, String enddate, String channel_name,String articlename, String filter,String filtertype)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
	  String query= "";
      
      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
	    query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
	    
      if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
      query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
	    
      if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
      query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' GROUP by "+ queryfield+"";  
		   
      
      
      
            CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    Integer accumulatedcount = 0;
		    Integer flag = 0;
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		        lines = processList(lines);
		    
		    
		    
		    
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		    	try{  
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		            
		            if(queryfield.equals("state"))
	            	{
	            	
	            	data[0]=data[0].replace("_", " ");
	            	 data[0] = capitalizeString(data[0]);
	            	obj.setState(data[0]);
	            	}
	            
	            
	               if(queryfield.equals("country"))
	        	  {
	        	
	            	data[0]=data[0].replace("_", " ");
	            	data[0] = capitalizeString(data[0]);
	            	obj.setCountry(data[0]);
	             	}
		            
		            
		            
		            
		            if(queryfield.equals("city")){
		        		
		            	
		            	try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e)
		        		{
		        			continue;
		        		}
		        		
		        		
		            
		            
		            }
		        	
		        	if(queryfield.equals("audience_segment"))
		        	{
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		        		
		        	}
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		        
		        	
		        	if(queryfield.equals("ISP")){
		        		if(data[0].trim().toLowerCase().equals("_ltd")==false){
		        			data[0]=data[0].replace("_"," ");
		        			obj.setISP(data[0]);
		        		}
		        	}
		        		
		            if(queryfield.equals("organisation")){
		        
		            	if((!data[0].trim().toLowerCase().contains("broadband")) && (!data[0].trim().toLowerCase().contains("communication")) && (!data[0].trim().toLowerCase().contains("cable")) && (!data[0].trim().toLowerCase().contains("telecom")) && (!data[0].trim().toLowerCase().contains("network")) && (!data[0].trim().toLowerCase().contains("isp")) && (!data[0].trim().toLowerCase().contains("hathway")) && (!data[0].trim().toLowerCase().contains("internet")) && (!data[0].trim().toLowerCase().equals("_ltd")) && (!data[0].trim().toLowerCase().contains("googlebot")) && (!data[0].trim().toLowerCase().contains("sify")) && (!data[0].trim().toLowerCase().contains("bsnl")) && (!data[0].trim().toLowerCase().contains("reliance")) && (!data[0].trim().toLowerCase().contains("broadband")) && (!data[0].trim().toLowerCase().contains("tata")) && (!data[0].trim().toLowerCase().contains("nextra")))
		            	{
		            		data[0]=data[0].replace("_"," ");
		            		obj.setOrganisation(data[0]);
		            	}
		            
		            }
		        	
		            
		            if(queryfield.equals("screen_properties")){
		        		
		        		obj.setScreen_properties(data[0]);
		        		
		        	}
		        	
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]);
		        
		        	}
		        	if(queryfield.equals("brandName"))
		        	{
		        		 data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
		        	}
		        
		        	

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	
		        	

		            Random random = new Random();	
		            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
		            Integer max = (int)Double.parseDouble(data[1]);
		            Integer randomNumber1 = random.nextInt(max) + 1;
		            
		            if(queryfield.equals("audience_segment"))	
		            {
		            obj.setCount(data[1]); 	
		            obj.setExternalWorldCount(randomNumber.toString());	
		            obj.setVisitorCount(randomNumber1.toString());
		            obj.setAverageTime("0.0");	
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
		            
			        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			     //   Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(articleAuthor);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}
			        pubreport.add(obj);
		            
		            }
		           
		            else if(queryfield.equals("agegroup")==true) {
		            	
		            	if(data[0].contains("medium")==false){
		            		    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				            	 obj.setCount(data[1]);
				               
				                 if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
				                 obj.setEngagementTime(data[1]);
				        

				                 if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
				                 obj.setVisitorCount(data[1]);
				            
		            		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		 		            
		 		            
		     		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		     		        pubreport.add(obj);
		            	}
		            }
		            
		            else{
		            	     if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
			            	 obj.setCount(data[1]);
			               
			                 if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
			                 obj.setEngagementTime(data[1]);
			        

			                 if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
			                 obj.setVisitorCount(data[1]);
			            
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
		            
			        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			        pubreport.add(obj);
		            
		            
		            }
		           
		    	}
		    	catch(Exception e){
		    		continue;
		    	}
		    	
		    	
		    	}
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    
		    if (queryfield.equals("LatLong")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countLatLongChannelArticle(startdate, enddate, channel_name, articlename,filter);
		  		return pubreport;
		    }
		    
		    if (queryfield.equals("postalcode")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countPinCodeChannelArticle(startdate, enddate, channel_name, articlename);
		  		return pubreport;
		    }
		    
		    
		    if (queryfield.equals("cityOthers")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countCityChannelArticle(startdate, enddate, channel_name, articlename,filter);
		  		return pubreport;
		    }
		    
		    
		    if (queryfield.equals("stateOthers")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countStateChannelArticle(startdate, enddate, channel_name, articlename,filter);
		  		return pubreport;
		    }
		    
		    if (queryfield.equals("countryOthers")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		  		pubreport=module.countCountryChannelArticle(startdate, enddate, channel_name, articlename,filter);
		  		return pubreport;
		    }
		    
		    
		    if (queryfield.equals("newVisitors")) {
		        
		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
				pubreport=module.countNewUsersChannelArticleDatewise(startdate, enddate, channel_name, articlename,filter); 
		        return pubreport;
		    }
		    
		    
		    if (queryfield.equals("returningVisitors")) {
		        
		    	 AggregationModule2 module =  AggregationModule2.getInstance();
		 	    try {
		 			module.setUp();
		 		} catch (Exception e1) {
		 			// TODO Auto-generated catch block
		 			e1.printStackTrace();
		 		}
				pubreport=module.countReturningUsersChannelArticleDatewise(startdate, enddate, channel_name, articlename,filter); 
		        return pubreport;
		    }
		    
		    if (queryfield.equals("LoyalVisitors")) {
		        
		    	 AggregationModule2 module =  AggregationModule2.getInstance();
		  	    try {
		  			module.setUp();
		  		} catch (Exception e1) {
		  			// TODO Auto-generated catch block
		  			e1.printStackTrace();
		  		}
		 		pubreport=module.countLoyalUsersChannelArticleDatewise(startdate, enddate, channel_name, articlename,filter); 
		        return pubreport;
		    }
		    
		    
		    
		    
		    if(queryfield.equals("visitorType")){
		    
		       List<PublisherReport> pubreport1 = new ArrayList<PublisherReport>();
		       List<PublisherReport> pubreport2 = new ArrayList<PublisherReport>();
		       List<PublisherReport> pubreport3 = new ArrayList<PublisherReport>();
		    	

		    	  AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		    	
		    	
				pubreport1=module.countNewUsersChannelArticleDatewise(startdate, enddate, channel_name, articlename,filter); 
		        
		   
		    
		    
		    	 
				pubreport2=module.countReturningUsersChannelArticleDatewise(startdate, enddate, channel_name, articlename,filter); 
		       
		    
		  
		        
		    	
		 		pubreport3=module.countLoyalUsersChannelArticleDatewise(startdate, enddate, channel_name, articlename,filter); 
		       
		        pubreport1.addAll(pubreport2);
		        pubreport1.addAll(pubreport3);
		 		
		        return pubreport1;
		 		
		    }   
		    
		    
		    if(queryfield.equals("engagementTime"))	
	        {
		    	 AggregationModule2 module =  AggregationModule2.getInstance();
			  	    try {
			  			module.setUp();
			  		} catch (Exception e1) {
			  			// TODO Auto-generated catch block
			  			e1.printStackTrace();
			  		}
			 		pubreport=module.EngagementTimeChannelArticle(startdate, enddate, channel_name, articlename);
			        return pubreport;
	        
	        }
	        	
	        	
	        if(queryfield.equals("minutesVisitor"))	
	        {
	        	pubreport.clear();
	        	PublisherReport obj1 = new PublisherReport();
	        	Random random = new Random();	
	            Integer randomNumber = random.nextInt(10) + 1;
	           String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj1.setChannelName(channel_name1);
	            String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj1.setArticleTitle(articleTitle);obj1.setArticle(articlename);
	            obj1.setMinutesperVisitor(randomNumber.toString());
	        	pubreport.add(obj1);
	            return pubreport;
	        }
		    
		    
		    if (queryfield.equals("totalViews")) {
		        
		   	 AggregationModule2 module =  AggregationModule2.getInstance();
		 	    try {
		 			module.setUp();
		 		} catch (Exception e1) {
		 			// TODO Auto-generated catch block
		 			e1.printStackTrace();
		 		}
				pubreport=module.counttotalvisitorsChannelArticle(startdate, enddate, channel_name, articlename);
		       return pubreport;
		   }
		    
		    
		    
		    if (queryfield.equals("totalViewsDatewise")) {
		        
			   	 AggregationModule2 module =  AggregationModule2.getInstance();
			 	    try {
			 			module.setUp();
			 		} catch (Exception e1) {
			 			// TODO Auto-generated catch block
			 			e1.printStackTrace();
			 		}
					pubreport=module.counttotalvisitorsChannelArticleDatewise(startdate, enddate, channel_name, articlename);
			        return pubreport;
		    }
			
		    
		    if (queryfield.equals("totalViewsHourwise")) {
		        
			   	 AggregationModule2 module =  AggregationModule2.getInstance();
			 	    try {
			 			module.setUp();
			 		} catch (Exception e1) {
			 			// TODO Auto-generated catch block
			 			e1.printStackTrace();
			 		}
					pubreport=module.counttotalvisitorsChannelArticleHourwise(startdate, enddate, channel_name, articlename);
			        return pubreport;
		    }
		    
		    
			           
		    if (queryfield.equals("uniqueVisitors")) {
		        
		      	 AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		   		pubreport=module.countfingerprintChannelArticle(startdate, enddate, channel_name, articlename);
		        return pubreport; 
		    } 
		    		    
		           
		    if (queryfield.equals("uniqueVisitorsDatewise")) {
		        
		      	 AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		   		pubreport=module.countfingerprintChannelArticleDatewise(startdate, enddate, channel_name, articlename);
		        return pubreport;  
		    }
		    
		   
		    if (queryfield.equals("uniqueVisitorsHourwise")) {
		        
		      	 AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		   		pubreport=module.countfingerprintChannelArticleHourwise(startdate, enddate, channel_name, articlename);
		        return pubreport;  
		    }
		    
		    
	           if(queryfield.equals("engagementTimeDatewise"))	
	           {
	        	   
	        	   
	        	   AggregationModule2 module =  AggregationModule2.getInstance();
	       	    try {
	       			module.setUp();
	       		} catch (Exception e1) {
	       			// TODO Auto-generated catch block
	       			e1.printStackTrace();
	       		}
	      		pubreport=module.EngagementTimeChannelArticleDatewise(startdate, enddate, channel_name, articlename);
	      		return pubreport;
	           
	           }
	           
	           if(queryfield.equals("engagementTimeHourwise"))	
	           {
	        	   
	        	   
	        	   AggregationModule2 module =  AggregationModule2.getInstance();
	       	    try {
	       			module.setUp();
	       		} catch (Exception e1) {
	       			// TODO Auto-generated catch block
	       			e1.printStackTrace();
	       		}
	      		pubreport=module.EngagementTimeChannelArticleHourwise(startdate, enddate, channel_name, articlename);
	      		return pubreport;
	           
	           }
	           
	           
	           
		    
		    if (queryfield.equals("clickedArticles")) {
		        
		      	 AggregationModule2 module =  AggregationModule2.getInstance();
		    	    try {
		    			module.setUp();
		    		} catch (Exception e1) {
		    			// TODO Auto-generated catch block
		    			e1.printStackTrace();
		    		}
		   		pubreport=module.getChannelArticleReferredPostsListInternal(startdate, enddate, channel_name, articlename, filter, filtertype);
		        return pubreport;  
		      }
		    
		    
		    

	           if (queryfield.equals("reforiginal")) {

	        	   String data0= null;
	               String data1= null;   
	               String data2 = null;
	        	   pubreport.clear();
	        	   
				for (int i = 0; i < 5; i++) {
					PublisherReport obj = new PublisherReport();

					if (i == 0) {
						data0 = "http://m.facebook.com";
						data1 = "107.0";
					    data2 = "Social";
					   }

					if (i == 1) {
						data0 = "http://www.facebook.com";
						data1 = "59.0";
					    data2 = "Social";
					}

					if (i == 2) {
						data0 = "http://l.facebook.com";
						data1 = "17.0";
					    data2 = "Social";
					}

					if (i == 3) {
						data0 = "http://www.google.co.pk";
						data1 = "12.0";
					    data2 = "Search";
					}

					if (i == 4) {
						data0 = "http://www.google.co.in";
						data1 = "101.0";
					    data2 = "Search";
					}

					obj.setReferrerSource(data0);
					obj.setReferrerType(data2);
					
					if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					obj.setCount(data1);
					if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
					obj.setEngagementTime(data1);
					if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					obj.setVisitorCount(data1);
					
					String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					pubreport.add(obj);

				}

			}
	    
	           /*
	           
	           if (queryfield.equals("device")) {

	        	   String data0= null;
	               String data1= null;   
	        	   pubreport.clear();
	        	   
	        	   for (int i = 0; i < 3; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="Mobile";
				          data1 = "202.0";
				          }
				          

				          if(i == 1){
				          data0="Tablet";
				          data1 = "19.0";
				          }
				          
				          
				          if(i == 2){
					          data0="Desktop";
					          data1 = "137.0";
					      }
					    
				        
				          obj.setDevice_type(data0);
				          obj.setCount(data1);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);			        
				          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				   }
	    
	           }
	    
	           if (queryfield.equals("incomelevel")) {

	        	   String data0= null;
	               String data1= null;   
	        	   pubreport.clear();
	           
	           for (int i = 0; i < 3; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			       // String[] data1 = ((String)lines1.get(i)).split(",");
			       
			          //if(data1[0].equals()) 
			         
			          if(i == 0){
			          data0="Medium";
			          data1 = "79.0";
			          }
			          

			          if(i == 1){
			          data0="High";
			          data1 = "55.0";
			          }
			          
			          
			          if(i == 2){
				          data0="Low";
				          data1 = "15.0";
				      }
				    
			        
			          obj.setIncomelevel(data0);
			          obj.setCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			      }
	           }
	          */
	           
	           
	           if (queryfield.equals("referrerType")) {

	        	   String data0= null;
	               String data1= null;   
	        	   pubreport.clear();
	           
	           for (int i = 0; i < 3; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			       // String[] data1 = ((String)lines1.get(i)).split(",");
			       
			          //if(data1[0].equals()) 
			         
			          if(i == 0){
			          data0="Social";
			          data1 = "95.0";
			          }
			          

			          if(i == 1){
			          data0="Search";
			          data1 = "125.0";
			          }
			          
			          
			          if(i == 2){
				          data0="Direct";
				          data1 = "67.0";
				      }
				    
			        
			          obj.setReferrerSource(data0);
			                if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			      }
	           }       
		    
		    
		    
	           if (queryfield.equals("trafficType")) {

	        	   String data0= null;
	               String data1= null;   
	        	   pubreport.clear();
	           
	           for (int i = 0; i < 3; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			       // String[] data1 = ((String)lines1.get(i)).split(",");
			       
			          //if(data1[0].equals()) 
			         
			          if(i == 0){
			          data0="Site";
			          data1 = "495.0";
			          }
			          

			          if(i == 1){
			          data0="FB Instant Article";
			          data1 = "125.0";
			          }
			          
			          
			          if(i == 2){
				          data0="Mobile App";
				          data1 = "367.0";
				      }
				    
			        
			          obj.setTrafficType(data0);
			                if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			      }
	           }       
		    
		     
	           if (queryfield.equals("siteExperience")) {

	        	   String data0= null;
	               String data1= null;   
	        	   pubreport.clear();
	           
	           for (int i = 0; i < 2; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			       // String[] data1 = ((String)lines1.get(i)).split(",");
			       
			          //if(data1[0].equals()) 
			         
			          if(i == 0){
			          data0="Standard";
			          data1 = "1595.0";
			          }
			          

			          if(i == 1){
			          data0="AMP";
			          data1 = "263.0";
			          }
			         			    
			        
			          obj.setSiteExperience(data0);
			          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			      }
	           }       
		    
		    
	           if (queryfield.equals("retention")) {

	        	   String data0= null;
	               String data1= null;   
	        	   pubreport.clear();
	           
	           for (int i = 0; i < 4; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			       // String[] data1 = ((String)lines1.get(i)).split(",");
			       
			          //if(data1[0].equals()) 
			         
			        
			          if(i == 0){
			          data0="Other Post";
			          data1 = "85.0";
			          }
			          

			          if(i == 1){
			          data0="Section Page";
			          data1 = "155.0";
			          }
			          
			          
			          if(i == 2){
				          data0="Home Page";
				          data1 = "567.0";
				      }
			        
			          
			          if(i == 3){
				          data0="Exit";
				          data1 = "67.0";
				      }
			          
			          
			          
			          obj.setRetention(data0);
			          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			      }
	           }       
	           
	           if (queryfield.equals("pageVersion")) {

	        	   String data0= null;
	               String data1= null;   
	        	   pubreport.clear();
	           
	           for (int i = 0; i < 4; i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			       // String[] data1 = ((String)lines1.get(i)).split(",");
			       
			          //if(data1[0].equals()) 
			         
			        
			          if(i == 0){
			          data0="Main Page/2345";
			          data1 = "385.0";
			          }
			          

			          if(i == 1){
			          data0="/2346";
			          data1 = "165.0";
			          }
			          
			          
			          if(i == 2){
				          data0="/2347";
				          data1 = "167.0";
				      }
			        
			          
			          if(i == 3){
				          data0="/2348";
				          data1 = "87.0";
				      }
			          
			          
			          
			          obj.setArticleVersion(data0);
			          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data1);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data1);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data1);
			          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			          pubreport.add(obj);
			        
			   //   }
			    //  System.out.println(headers1);
			    //  System.out.println(lines1);
			      }
	           }       
	           
	           
	           
	           
	           
		    
		    return pubreport;
		  }
		  
  
  public List<PublisherReport> getQueryFieldChannelArticleFilter(String queryfield,String startdate, String enddate, String channel_name,String articlename, Map<String,String>filter, String filterMetric)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	        int size = filter.size();
	        String queryfilterbuilder = "";
	        String formattedString = "";
	        int j =0;
	        for (Map.Entry<String, String> entry : filter.entrySet())
	        {
	        	if (j==0){
	                formattedString = addCommaString(entry.getValue());
	        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
	        	
	        	}
	            else{
	            formattedString = addCommaString(entry.getValue());	
	            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
	       
	            }
	            j++;
	         
	        }
	  
	        String query = "";
	        
	        if(filterMetric == null || filterMetric.isEmpty() ||  filterMetric.equals("pageviews"))
	         query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
		    
	        if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("engagementTime"))
		     query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
	        
	        if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("visitorCount"))
		     query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+"";
	        
	        
	        
	        System.out.println(query);
	        CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		    if(filterMetric != null && !filterMetric.isEmpty()  && filterMetric.equals("visitorCount") )
		        lines = processList(lines);
		    
		    
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        
		    	try{
		    	  
		    	PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        data[0] = capitalizeString(data[0]);
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			continue;
		        		}
		        		
		        		}
		        	
		        	if(queryfield.equals("audience_segment"))
		        	{
		        		String audienceSegment = audienceSegmentMap.get(data[0]);
		        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
		        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
		        		obj.setAudience_segment(audienceSegment);
		        		obj.setAudienceSegmentCode(audienceSegmentCode);
		        		}
		        		else
		        	    obj.setAudience_segment(data[0]);
		        		
		        	}
		        	
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
		        	{
		        		 data[0]=data[0].replace("_","-");
		        		 data[0]=data[0]+ " Years";
		        		 if(data[0].contains("medium")==false)
		        		 obj.setAge(data[0]);
		        	}
		            	
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		    
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        String [] osParts = oscodeMap1.get(osproperties).split(",");
				        obj.setOs(osParts[0]);
				        obj.setOSversion(osParts[1]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
				        System.out.println(mobiledeviceproperties[2]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]);
		        
		        	}
		        	if(queryfield.equals("brandName")){
		        		 data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
		        		obj.setBrandname(data[0]);
		        	}
		        
		        	if(queryfield.equals("refcurrentoriginal"))
		  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
		        	
		        	
		        	
		        	

		            Random random = new Random();	
		            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
		            Integer max = (int)Double.parseDouble(data[1]);
		            Integer randomNumber1 = random.nextInt(max) + 1;
		            
		            if(queryfield.equals("audience_segment"))	
		            {
		            obj.setCount(data[1]); 	
		            obj.setExternalWorldCount(randomNumber.toString());	
		            obj.setVisitorCount(randomNumber1.toString());
		            obj.setAverageTime("0.0");	
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
		            
			        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			        pubreport.add(obj);
		            
		            }
		           
		            else if(queryfield.equals("agegroup")==true) {
		            	
		            	if(data[0].contains("medium")==false){
		            		if(filterMetric == null || filterMetric.isEmpty() ||  filterMetric.equals("pageviews"))
				            	obj.setCount(data[1]);
				            
				                if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("engagementTime") )
				                obj.setEngagementTime(data[1]);
				           
				                if(filterMetric != null && !filterMetric.isEmpty()  && filterMetric.equals("visitorCount") )
				                obj.setVisitorCount(data[1]);
		            		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					            
					            
						        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
						        pubreport.add(obj);
		            	}
		            }
		            				            
		            else{
		            	if(filterMetric == null || filterMetric.isEmpty() ||  filterMetric.equals("pageviews"))
			            	obj.setCount(data[1]);
			            
			                if(filterMetric != null && !filterMetric.isEmpty() && filterMetric.equals("engagementTime") )
			                obj.setEngagementTime(data[1]);
			           
			                if(filterMetric != null && !filterMetric.isEmpty()  && filterMetric.equals("visitorCount") )
			                obj.setVisitorCount(data[1]);
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            
		            
			        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			        pubreport.add(obj);
		            
		            
		            }
		           
		    	}
		    	catch(Exception e)
		    	{
		    		continue;
		    	}
		    	
		    	}
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
  

		 
  
  /* 
		  public List<PublisherReport> getQueryFieldChannelArticleFilter(String queryfield,String startdate, String enddate, String channel_name,String articlename, Map<String,String>filter)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    
			        int size = filter.size();
			        String queryfilterbuilder = "";
			        String formattedString = "";
			        int j =0;
			        for (Map.Entry<String, String> entry : filter.entrySet())
			        {
			        	if (j==0){
			                formattedString = addCommaString(entry.getValue());
			        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
			        	
			        	}
			            else{
			            formattedString = addCommaString(entry.getValue());	
			            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
			       
			            }
			            j++;
			         
			        }
			  
			        String query = "";
			        
			        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
			         query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
				    
			        if(filter != null && !filter.isEmpty() && filter.equals("engagementTime"))
				     query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
			        
			        if(filter != null && !filter.isEmpty() && filter.equals("visitorCount"))
				     query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
			        
			        
			        
			        System.out.println(query);
			        CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
				        lines = processList(lines);
				    
				    
				    
				    //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        
				    	try{
				    	  
				    	PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				     //   String demographicproperties = demographicmap.get(data[0]);
				            if(queryfield.equals("gender"))
				        	obj.setGender(data[0]);
				        
				            if(queryfield.equals("device"))
				        	obj.setDevice_type(data[0]);
				        	
				        	if(queryfield.equals("city")){
				        		try{
				        		String locationproperties = citycodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-"," ");
						        data[0] = capitalizeString(data[0]);
						        obj.setCity(data[0]);
						        System.out.println(data[0]);
						        obj.setLocationcode(locationproperties);
				        		}
				        		catch(Exception e){
				        			continue;
				        		}
				        		
				        		}
				        	
				        	if(queryfield.equals("audience_segment"))
				        	{
				        		String audienceSegment = audienceSegmentMap.get(data[0]);
				        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
				        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
				        		obj.setAudience_segment(audienceSegment);
				        		obj.setAudienceSegmentCode(audienceSegmentCode);
				        		}
				        		else
				        	    obj.setAudience_segment(data[0]);
				        		
				        	}
				        	
				        	
				        	if(queryfield.equals("reforiginal"))
					             obj.setReferrerSource(data[0]);
				            	
				        	if(queryfield.equals("agegroup"))
				        	{
				        		 data[0]=data[0].replace("_","-");
				        		 data[0]=data[0]+ " Years";
				        		 if(data[0].contains("medium")==false)
				        		 obj.setAge(data[0]);
				        	}
				            	
				            	
				        	if(queryfield.equals("incomelevel"))
					          obj.setIncomelevel(data[0]);
				    
				        	
				        	if(queryfield.equals("system_os")){
				        		String osproperties = oscodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-", " ");
						        data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
						        String [] osParts = oscodeMap1.get(osproperties).split(",");
						        obj.setOs(osParts[0]);
						        obj.setOSversion(osParts[1]);
						        obj.setOscode(osproperties);
				        	}
				         	
				        	if(queryfield.equals("modelName")){
				        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
					        	
						        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
						        System.out.println(mobiledeviceproperties[2]);
						        obj.setDevicecode(mobiledeviceproperties[0]);
						        System.out.println(mobiledeviceproperties[0]);
				        
				        	}
				        	if(queryfield.equals("brandName")){
				        		 data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
				        		obj.setBrandname(data[0]);
				        	}
				        
				        	if(queryfield.equals("refcurrentoriginal"))
				  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
				        	
				        	
				        	
				        	

				            Random random = new Random();	
				            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
				            Integer max = (int)Double.parseDouble(data[1]);
				            Integer randomNumber1 = random.nextInt(max) + 1;
				            
				            if(queryfield.equals("audience_segment"))	
				            {
				            obj.setCount(data[1]); 	
				            obj.setExternalWorldCount(randomNumber.toString());	
				            obj.setVisitorCount(randomNumber1.toString());
				            obj.setAverageTime("0.0");	
				            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            
				            
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
				            
				            }
				           
				            else if(queryfield.equals("agegroup")==true) {
				            	
				            	if(data[0].contains("medium")==false){
				            		if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
						            	obj.setCount(data[1]);
						            
						                if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
						                obj.setEngagementTime(data[1]);
						           
						                if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						                obj.setVisitorCount(data[1]);
				            		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							            
							            
								        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
								        pubreport.add(obj);
				            	}
				            }
				            				            
				            else{
				            	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					            	obj.setCount(data[1]);
					            
					                if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
					                obj.setEngagementTime(data[1]);
					           
					                if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					                obj.setVisitorCount(data[1]);
				            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            
				            
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
				            
				            
				            }
				           
				    	}
				    	catch(Exception e)
				    	{
				    		continue;
				    	}
				    	
				    	}
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
		  */

	
		  

		  public List<PublisherReport> getQueryFieldChannelArticleGroupBy(String queryfield,String startdate, String enddate, String channel_name, String articlename, List<String> groupby, String filter)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    
			      
			    
		      String querygroupbybuilder = convert(groupby);
		      List<PublisherReport> pubreport = new ArrayList();
		      String query = "";
		      
		      int  l=0;
		      
		      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		    	 	query = "Select count(*),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
		    	    
		    	    
		    	    if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime"))
		    	     	query = "Select SUM(engagementTime),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
		    	        
		    	    
		    	    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount"))
		    	     	query = "Select "+queryfield+","+querygroupbybuilder+",cookie_id  from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder+"";
		    	        

		    	    if(querygroupbybuilder.equals("hour")){
		    	    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		    	    	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
		    	   
		    	    	
		    	    }
		    	  
		    	    if(querygroupbybuilder.equals("minute")){
		    	    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		    	    	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
		    	   		    
		    	    } 	
		    	    
		    	    if(querygroupbybuilder.equals("second")){
		    	    //	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		    	    //	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
		    	   		    
		    	    } 	
		    	    
		    	    if(querygroupbybuilder.equals("hour")){
		    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
		    	    	query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
		    	   
		    	    	
		    	    }
		    	  
		    	    if(querygroupbybuilder.equals("minute")){
		    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
		    	    	query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
		    	   		    
		    	    } 	
		    	    
		    	    if(querygroupbybuilder.equals("second")){
		    	    //	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
		    	    	//query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
		    	   		    
		    	    } 	
		    	    
		    	   
		    	    if(querygroupbybuilder.equals("hour")){
		    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
		    	    	query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
		    	   
		    	    	
		    	    }
		    	  
		    	    if(querygroupbybuilder.equals("minute")){
		    	   	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
		    	    	query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
		    	   		    
		    	    } 	
		    	    
		    	    if(querygroupbybuilder.equals("second")){
		    	    //	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
		    	    	//query = "Select "
		    	    	//		+ queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
		    	   		    
		    	    } 	
		    	    
		    	    
		    	    
		    	    
		    	    
		    	    
		    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("totalViews"))
		    	    {
		    	    	
		    	    	query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
		    	    }
		    	    
		    	    	
		    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("totalViews"))
		    	    {
		    	   	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		    	    	query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
		    	    }
		    	    	
		    	    if(querygroupbybuilder.equals("second") && queryfield.equals("totalViews"))
		    	    {
		    	    	//if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		    	    	//query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
		    	    }
		    	    
		    	    	
		    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("uniqueVisitors"))
		    	    {
		    	    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + 
		    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
		    	    	
		    	    }
		    	 		    
		    			    	
		    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("uniqueVisitors"))
		    	    {
		    	    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + 
		    			      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +  " GROUP BY date_histogram(field='request_time','interval'='1m')";
		    	    
		    	    }
		    	    	
		    	    
		    	    if(querygroupbybuilder.equals("second") && queryfield.equals("uniqueVisitors"))
		    	    {
		    	    //	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + 
		    			//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
		    	    	
		    	    }
		    	    
		    	    
		    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("engagementTime"))
		    	    {
		    	    	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + 
		    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
		    	    	
		    	    }
		    	 		    
		    			    	
		    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("engagementTime") )
		    	    {
		    	    	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + 
		    			      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
		    	    	
		    	    }
		    	    	
		    	    
		    	    if(querygroupbybuilder.equals("second") && queryfield.equals("engagementTime") )
		    	    {
		    	    //	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and  channel_name = '" + 
		    			//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
		    	    	
		    	    }
				    	
				    
	         	 
	         	
	         	System.out.println(query);
		         	CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				  
				    
				    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					    lines = processList1(lines);
				    
				    
				    if(queryfield.equals("audience_segment") && groupby.get(0).equals("subcategory")== true){
					    
					    
					    List<List<String>> data = new ArrayList<List<String>>();
					    for (int i = 0; i < lines.size(); i++) {
				            List<String> objects = new ArrayList<String>();
				            String [] parts = lines.get(i).split(",");
				            for(int j =0; j< parts.length; j++)
				              objects.add(parts[j]);
				           
				            data.add(objects);
				        }
					    
					    
					    ResultSet obj = ListtoResultSet.getResultSet(headers, data);
					    
					    
					//    JSONArray json = Convertor.convertResultSetIntoJSON(obj);
					 //   String s = json.toString();
					    pubreport= NestedJSON.getNestedJSONObject(obj, queryfield, groupby,filter); 
					 //   System.out.println(nestedJson);
					    return pubreport;
					    
					    }
				    
				    
				    //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				       try{
				    	  
				    	PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				     //   String demographicproperties = demographicmap.get(data[0]);
				        
				            if(queryfield.equals("gender"))
				        	obj.setGender(data[0]);
				        
				            if(queryfield.equals("device"))
				        	obj.setDevice_type(data[0]);
				        	
				        	if(queryfield.equals("city")){
				        		try{
				        		String locationproperties = citycodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-"," ");
						        data[0] = capitalizeString(data[0]);
						        obj.setCity(data[0]);
						        System.out.println(data[0]);
						        obj.setLocationcode(locationproperties);
				        		}
				        		catch(Exception e)
				        		{
				        			continue;
				        			
				        		}
				        		
				        		}
				        	
				        	if(queryfield.equals("audience_segment"))
				        	{
				        		String audienceSegment = audienceSegmentMap.get(data[0]);
				        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
				        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
				        		obj.setAudience_segment(audienceSegment);
				        		obj.setAudienceSegmentCode(audienceSegmentCode);
				        		}
				        		else
				        	    obj.setAudience_segment(data[0]);
				        		
				        		
				        	}
				        	
				        	
				        	  if(queryfield.equals("state"))
				            	{
				            	
				            	data[0]=data[0].replace("_", " ");
				            	data[0] = capitalizeString(data[0]);
				            	obj.setState(data[0]);
				            	}
				            
				            
				            if(queryfield.equals("country"))
				        	  {
				        	
				            	data[0]=data[0].replace("_", " ");
				            	data[0] = capitalizeString(data[0]);
				            	obj.setCountry(data[0]);
				             	}
				        
				        	
				        	
				        	
				        	if(queryfield.equals("reforiginal"))
					             obj.setReferrerSource(data[0]);
				            	
				        	if(queryfield.equals("agegroup"))
				        	{
				        		 data[0]=data[0].replace("_","-");
				        		 data[0]=data[0]+ " Years";
				        		 if(data[0].contains("medium")==false)
				        		 obj.setAge(data[0]);
				        	}
				            	
				            	
				        	if(queryfield.equals("incomelevel"))
					          obj.setIncomelevel(data[0]);
				    
				        	
				        	
				        	if(queryfield.equals("system_os")){
				        		String osproperties = oscodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-", " ");
						        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
						        String [] osParts = oscodeMap1.get(osproperties).split(",");
						        obj.setOs(osParts[0]);
						        obj.setOSversion(osParts[1]);
						        obj.setOscode(osproperties);
				        	}
				         	
				        	if(queryfield.equals("modelName")){
				        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
					        	
						        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
						        System.out.println(mobiledeviceproperties[2]);
						        obj.setDevicecode(mobiledeviceproperties[0]);
						        System.out.println(mobiledeviceproperties[0]);
				        	}
				         	
				        	if(queryfield.equals("brandName"))
					          {
				        		data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        		obj.setBrandname(data[0]);
					          }
				        
				        	
				        	if(queryfield.equals("refcurrentoriginal"))
				  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
				        	
				        	
				        	//   obj.setCode(code);
			            for(int k = 0; k < groupby.size(); k++)
			            {
			            	
			            	if(groupby.get(k).equals(queryfield)==false)
			            	{
			            	try{
			            	if(groupby.get(k).equals("device"))
			            	obj.setDevice_type(data[k+1]);
			            	
			            	
			            	 if(groupby.get(k).equals("state"))
				             	{
				             	
				             	data[k+1]=data[k+1].replace("_", " ");
				             	data[k+1] = capitalizeString(data[k+1]);
				             	obj.setState(data[k+1]);
				             	}
				             
				             
				             if(groupby.get(k).equals("country"))
				         	  {
				         	
				             	data[k+1]=data[k+1].replace("_", " ");
				             	data[k+1] = capitalizeString(data[k+1]);
				             	obj.setCountry(data[k+1]);
				              	}
			            	
			            	
			            	if(groupby.get(k).equals("city"))
				            {
			            		try{
			            		String locationproperties = citycodeMap.get(data[k+1]);
			    		        data[k+1]=data[k+1].replace("_"," ").replace("-"," ");
			    		        data[k+1] = capitalizeString(data[k+1]);
			    		        obj.setCity(data[k+1]);
			    		        System.out.println(data[k+1]);
			    		        obj.setLocationcode(locationproperties);
			            		}
			            		catch(Exception e)
			            		{
			            			continue;
			            		}
			            		}
			            	
			            	if(groupby.get(k).equals("audience_segment")){
			            		
			            		String audienceSegment = audienceSegmentMap.get(data[k+1]);
			            		String audienceSegmentCode = audienceSegmentMap2.get(data[k+1]);
			            		if(audienceSegment!=null && !audienceSegment.isEmpty()){
			            		obj.setAudience_segment(audienceSegment);
			            		obj.setAudienceSegmentCode(audienceSegmentCode);
			            		}
			            		else
			            	    obj.setAudience_segment(data[k+1]);
			            	}
			            	
			            	if(groupby.get(k).equals("gender"))
					             obj.setGender(data[k+1]);
			            	
			            	if(groupby.get(k).equals("subcategory"))
			            	 {
			            		String audienceSegment = audienceSegmentMap.get(data[k+1]);
			            		String audienceSegmentCode = audienceSegmentMap2.get(data[k+1]);
			            		if(audienceSegment!=null && !audienceSegment.isEmpty()){
			            		obj.setSubcategory(audienceSegment);
			            		obj.setSubcategorycode(audienceSegmentCode);
			            		}
			            		else
			            	    obj.setSubcategory(data[k+1]);
					             }
			            	
			            	if(groupby.get(k).equals("date"))
					             obj.setDate(data[k+1]);
			            	
			            	if(groupby.get(k).equals("hour"))
					             obj.setDate(data[k+1]);
			            	
			            	if(groupby.get(k).equals("minute"))
					             obj.setDate(data[k+1]);	
			            	
			            	if(groupby.get(k).equals("refcurrentoriginal"))
					             obj.setGender(data[k+1]);
				            	
			            	if(groupby.get(k).equals("agegroup"))
			            	{
				        		 data[k+1]=data[k+1].replace("_","-");
				        		 data[k+1]=data[k+1]+ " Years";
				        		 if(data[k+1].contains("medium")==false)
				        		 obj.setAge(data[k+1]);
				        	}
				            	
				            	
			            	if(groupby.get(k).equals("incomelevel"))
					          obj.setIncomelevel(data[k+1]);
				            	
		                    l++;
			            	}
			            	catch(Exception e){
			            		continue;
			            	}
			            	
			            	}
			            }
				        
			            
			           
			            	            
			            if(l!=0){
			            	        
			            	        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					            	obj.setCount(data[l+1]);
					             
						            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					                obj.setVisitorCount(data[l+1]);
					         

						            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
					                obj.setEngagementTime(data[l+1]);
					         
			            }
			 		         
			 		    String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				        pubreport.add(obj);
				        l=0;
				       }
				       catch(Exception e)
				       {
				    	   continue;
				       }
				       
				       }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
		  
		  public List<PublisherReport> getQueryFieldChannelSection(String queryfield,String startdate, String enddate, String channel_name,String sectionid, String filter)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
			
				{
			    
			    String query = "";
			  
		        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		        query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionid+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
			   
		        if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
		        query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionid+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
				   
		        if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		        query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionid+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' GROUP by "+ queryfield+"";  
		        		
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				 
				    List<PublisherReport> pubreport = new ArrayList();
				    

				    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
				    lines = processList(lines);
				  
				  
				  
				  
				  //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				       
				    	try{  
				    	  
				    	  PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				     //   String demographicproperties = demographicmap.get(data[0]);
				            
				            if(queryfield.equals("gender"))
				        	obj.setGender(data[0]);
				        
				            if(queryfield.equals("device"))
				        	obj.setDevice_type(data[0]);
				        	
				            
				            if(queryfield.equals("state"))
			            	{
			            	
			            	data[0]=data[0].replace("_", " ");
			            	data[0] = capitalizeString(data[0]);
			            	obj.setState(data[0]);
			            	}
			            
			            
			                if(queryfield.equals("country"))
			        	  {
			        	
			            	data[0]=data[0].replace("_", " ");
			            	 data[0] = capitalizeString(data[0]);
			            	obj.setCountry(data[0]);
			             	}
				            
				            
				        	if(queryfield.equals("city")){
				        		try{
				        		String locationproperties = citycodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-"," ");
						        data[0] = capitalizeString(data[0]);
						        obj.setCity(data[0]);
						        System.out.println(data[0]);
						        obj.setLocationcode(locationproperties);
				        		}
				        		catch(Exception e)
				        		{
				        			continue;
				        		}
				        		
				        		}
				        	if(queryfield.equals("audience_segment"))
				        	{
				        		String audienceSegment = audienceSegmentMap.get(data[0]);
				        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
				        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
				        		obj.setAudience_segment(audienceSegment);
				        		obj.setAudienceSegmentCode(audienceSegmentCode);
				        		}
				        		else
				        	    obj.setAudience_segment(data[0]);
				        		
				        	}
				        	
				        	if(queryfield.equals("reforiginal"))
					             obj.setReferrerSource(data[0]);
				            	
				        	if(queryfield.equals("agegroup"))
				        	{ 
				        		data[0]=data[0].replace("_","-");
				        		data[0]=data[0]+ " Years";
				        		if(data[0].contains("medium")==false)
				        		obj.setAge(data[0]);
				        	}
				            	
				        	if(queryfield.equals("incomelevel"))
					          obj.setIncomelevel(data[0]);
				        
				        	
				        	if(queryfield.equals("ISP")){
				        		if(data[0].trim().toLowerCase().equals("_ltd")==false)
				        		{
				        			data[0]=data[0].replace("_"," ");
				        			obj.setISP(data[0]);
				            	}
				        	}	
				            if(queryfield.equals("organisation")){
				        
				            	if((!data[0].trim().toLowerCase().contains("broadband")) && (!data[0].trim().toLowerCase().contains("communication")) && (!data[0].trim().toLowerCase().contains("cable")) && (!data[0].trim().toLowerCase().contains("telecom")) && (!data[0].trim().toLowerCase().contains("network")) && (!data[0].trim().toLowerCase().contains("isp")) && (!data[0].trim().toLowerCase().contains("hathway")) && (!data[0].trim().toLowerCase().contains("internet")) && (!data[0].trim().toLowerCase().equals("_ltd")) && (!data[0].trim().toLowerCase().contains("googlebot")) && (!data[0].trim().toLowerCase().contains("sify")) && (!data[0].trim().toLowerCase().contains("bsnl")) && (!data[0].trim().toLowerCase().contains("reliance")) && (!data[0].trim().toLowerCase().contains("broadband")) && (!data[0].trim().toLowerCase().contains("tata")) && (!data[0].trim().toLowerCase().contains("nextra")))
				            	{
				            		data[0]=data[0].replace("_"," ");
				            		obj.setOrganisation(data[0]);
				            	}
				            
				            }
				        	
				        	if(queryfield.equals("screen_properties")){
				        		
				        		obj.setScreen_properties(data[0]);
				        		
				        	}
				            
                             if(queryfield.equals("authorName")){
				        		
				        		obj.setArticleAuthor(data[0]);
				        		String authorId = AuthorMap1.get(data[0]);
				        		obj.setAuthorId(authorId);
				        	}
				        	
                             if(queryfield.equals("tag")){
 				        		
 				        		obj.setArticleTags(data[0]);
 				        	}
                             
				        	
				        	
				        	if(queryfield.equals("system_os")){
				        		String osproperties = oscodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-", " ");
						        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
						        String [] osParts = oscodeMap1.get(osproperties).split(",");
						        obj.setOs(osParts[0]);
						        obj.setOSversion(osParts[1]);
						        obj.setOscode(osproperties);
				        	}
				         	
				        	if(queryfield.equals("modelName")){
				        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
					        	
						        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
						        System.out.println(mobiledeviceproperties[2]);
						        obj.setDevicecode(mobiledeviceproperties[0]);
						        System.out.println(mobiledeviceproperties[0]);
				        
				        	}
				        	
				        	if(queryfield.equals("brandName")){
				        		data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        		obj.setBrandname(data[0]);
				        	}

				        	if(queryfield.equals("refcurrentoriginal"))
				  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
				        	
				        	

				            Random random = new Random();	
				            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
				            Integer max = (int)Double.parseDouble(data[1]);
				            Integer randomNumber1 = random.nextInt(max) + 1;
				            
				            if(queryfield.equals("audience_segment"))	
				            {
				            obj.setCount(data[1]); 	
				            obj.setExternalWorldCount(randomNumber.toString());	
				            obj.setVisitorCount(randomNumber1.toString());
				            obj.setAverageTime("0.0");	
				            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        obj.setSection(sectionid);
					        pubreport.add(obj);
				            
				            }
				            
				            else if(queryfield.equals("agegroup")==true) {
				            	
				            	if(data[0].contains("medium")==false){
				            		        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))	
								            obj.setCount(data[1]);
								            
								            
								            if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								            obj.setEngagementTime(data[1]);
								            	
								           

								            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								            obj.setVisitorCount(data[1]);
				            		String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        obj.setSection(sectionid);
							        pubreport.add(obj);
				            	}
				            }
				            
				            else if(queryfield.equals("refcurrentoriginal")==true) {
				            	
                    	        obj.setCount(data[1]);
                    	        Random randomv1 = new Random();	
    				            Integer randomNumberv1 = random.nextInt(1000 + 1 - 500) + 500;
    				            Integer maxv1 = (int)Double.parseDouble(data[1]);
    				            Integer randomNumber2 = random.nextInt(maxv1) + 1;
    				            Integer randomNumber3 = random.nextInt(maxv1) + 1;
    				            Integer value = (Integer)(randomNumber2/randomNumber3) + 1;
    				            obj.setAverageTime(value.toString());
    				            obj.setEngagementTime(randomNumber2.toString());
    				            obj.setVisitorCount(randomNumber3.toString());
                    	        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						       
						        pubreport.add(obj);
			            	}
				            
				            else{
				            	        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))	
							            obj.setCount(data[1]);
							            
							            
							            if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							            obj.setEngagementTime(data[1]);
							            	
							           

							            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							            obj.setVisitorCount(data[1]);
				            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        obj.setSection(sectionid);
					        pubreport.add(obj);
				            
				            }
				        
				    	}
				    	catch(Exception e)
				    	{
				    		continue;
				    	}
				    	
				    	}
				      
				      
				    	
				    	
				    	//System.out.println(headers);
				      //System.out.println(lines);
				    }
				   

				    if (queryfield.equals("LatLong")) {
				        
				    	  AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
				  		pubreport=module.countLatLongChannelSection(startdate, enddate, channel_name, sectionid);
				  		return pubreport;
				    }
				    
				   
				    if (queryfield.equals("postalcode")) {
				        
				    	  AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
				  		pubreport=module.countPinCodeChannelSection(startdate, enddate, channel_name, sectionid);
				  		return pubreport;
				    }
				   
				    if (queryfield.equals("cityOthers")) {
				        
				    	  AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
				  		pubreport=module.countCityChannelSection(startdate, enddate, channel_name, sectionid,filter);
				  		return pubreport;
				    }
				    
				    
				    if (queryfield.equals("stateOthers")) {
				        
				    	  AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
				  		pubreport=module.countStateChannelSection(startdate, enddate, channel_name, sectionid,filter);
				  	
				  		return pubreport;
				    }
				    
				    if (queryfield.equals("countryOthers")) {
				        
				    	  AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
				  		pubreport=module.countCountryChannelSection(startdate, enddate, channel_name, sectionid,filter);
				  		return pubreport;
				    }
				    
				    if (queryfield.equals("newVisitors")) {
				        
				    	  AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
						pubreport=module.countNewUsersChannelSectionDatewise(startdate, enddate, channel_name, sectionid,filter);
						return pubreport;
				    }
				    
				    if (queryfield.equals("returningVisitors")) {
				        
				    	 AggregationModule2 module =  AggregationModule2.getInstance();
				 	    try {
				 			module.setUp();
				 		} catch (Exception e1) {
				 			// TODO Auto-generated catch block
				 			e1.printStackTrace();
				 		}
						pubreport=module.countReturningUsersChannelSectionDatewise(startdate, enddate, channel_name, sectionid,filter);
						return pubreport;
				    }
				    
				    if (queryfield.equals("LoyalVisitors")) {
				        
				    	 AggregationModule2 module =  AggregationModule2.getInstance();
				  	    try {
				  			module.setUp();
				  		} catch (Exception e1) {
				  			// TODO Auto-generated catch block
				  			e1.printStackTrace();
				  		}
				 		pubreport=module.countLoyalUsersChannelSectionDatewise(startdate, enddate, channel_name, sectionid,filter);
				 		return pubreport;
				    }
				      
				    
				    
				
				  if(queryfield.equals("visitorType")) { 
				    
				    
					  List<PublisherReport> pubreport1 = new ArrayList<PublisherReport>();
				      List<PublisherReport> pubreport2 = new ArrayList<PublisherReport>();
				      List<PublisherReport> pubreport3 = new ArrayList<PublisherReport>();
				       
					  
					  
				    	  AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
						pubreport1=module.countNewUsersChannelSectionDatewise(startdate, enddate, channel_name, sectionid,filter);
						
						pubreport2=module.countReturningUsersChannelSectionDatewise(startdate, enddate, channel_name, sectionid,filter);
					
						pubreport3=module.countLoyalUsersChannelSectionDatewise(startdate, enddate, channel_name, sectionid,filter);
				 		
						
						pubreport1.addAll(pubreport2);
						pubreport1.addAll(pubreport3);
						
						return pubreport1;
				    }
				    
				    
				  
				    if (queryfield.equals("totalViews")) {
				        
				   	 AggregationModule2 module =  AggregationModule2.getInstance();
				 	    try {
				 			module.setUp();
				 		} catch (Exception e1) {
				 			// TODO Auto-generated catch block
				 			e1.printStackTrace();
				 		}
						pubreport=module.counttotalvisitorsChannelSection(startdate, enddate, channel_name, sectionid);
						return pubreport;
				   }
				    
				    if(queryfield.equals("engagementTime"))	
			        {
				    	AggregationModule2 module =  AggregationModule2.getInstance();
				 	    try {
				 			module.setUp();
				 		} catch (Exception e1) {
				 			// TODO Auto-generated catch block
				 			e1.printStackTrace();
				 		}
						pubreport=module.engagementTimeChannelSection(startdate, enddate, channel_name, sectionid);
						return pubreport;
			        
			        }
			        	
			        	
			        if(queryfield.equals("minutesVisitor"))	
			        {
			        	pubreport.clear();
			        	PublisherReport obj1 = new PublisherReport();
			        	Random random = new Random();	
			            Integer randomNumber = random.nextInt(10) + 1;
			           String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj1.setChannelName(channel_name1);
			            obj1.setSection(sectionid);
			            obj1.setMinutesperVisitor(randomNumber.toString());
			        	pubreport.add(obj1);
			            return pubreport;
			        }
				    
				    
				    
				    
				    if (queryfield.equals("totalViewsDatewise")) {
				        
					   	 AggregationModule2 module =  AggregationModule2.getInstance();
					 	    try {
					 			module.setUp();
					 		} catch (Exception e1) {
					 			// TODO Auto-generated catch block
					 			e1.printStackTrace();
					 		}
							pubreport=module.counttotalvisitorsChannelSectionDatewise(startdate, enddate, channel_name, sectionid);
							return pubreport;
					   }
					    
				    
				    if (queryfield.equals("totalViewsHourwise")) {
				        
					   	 AggregationModule2 module =  AggregationModule2.getInstance();
					 	    try {
					 			module.setUp();
					 		} catch (Exception e1) {
					 			// TODO Auto-generated catch block
					 			e1.printStackTrace();
					 		}
							pubreport=module.counttotalvisitorsChannelSectionDateHourwise(startdate, enddate, channel_name, sectionid);
							return pubreport;
					   }
					    
				    
				    
				    if (queryfield.equals("uniqueVisitors")) {
				        
				      	 AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
				   		pubreport=module.countfingerprintChannelSection(startdate, enddate, channel_name, sectionid);
				   		return pubreport; 
				      }
				    
					
				    if (queryfield.equals("uniqueVisitorsDatewise")) {
				        
				      	 AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
				   		pubreport=module.countfingerprintChannelSectionDatewise(startdate, enddate, channel_name, sectionid);
				   		return pubreport;
				      }
				    
				    
				    
				    if (queryfield.equals("uniqueVisitorsHourwise")) {
				        
				      	 AggregationModule2 module =  AggregationModule2.getInstance();
				    	    try {
				    			module.setUp();
				    		} catch (Exception e1) {
				    			// TODO Auto-generated catch block
				    			e1.printStackTrace();
				    		}
				   		pubreport=module.countfingerprintChannelSectionDateHourwise(startdate, enddate, channel_name, sectionid);
				   		return pubreport;
				      }
				    
				    
				    
				    
				    if(queryfield.equals("engagementTimeDatewise"))	
			           {
			        	   
			        	   
			        	   AggregationModule2 module =  AggregationModule2.getInstance();
			       	    try {
			       			module.setUp();
			       		} catch (Exception e1) {
			       			// TODO Auto-generated catch block
			       			e1.printStackTrace();
			       		}
			      		pubreport=module.engagementTimeChannelSectionDatewise(startdate, enddate, channel_name, sectionid);
			      		return pubreport;
			           
			           }
			            
				    

				    if(queryfield.equals("engagementTimeHourwise"))	
			           {
			        	   
			        	   
			        	   AggregationModule2 module =  AggregationModule2.getInstance();
			       	    try {
			       			module.setUp();
			       		} catch (Exception e1) {
			       			// TODO Auto-generated catch block
			       			e1.printStackTrace();
			       		}
			      		pubreport=module.engagementTimeChannelSectionDateHourwise(startdate, enddate, channel_name, sectionid);
			      		return pubreport;
			           
			           }
			            
				    
				    
				    
				    
			           if (queryfield.equals("reforiginal")) {

			        	   String data0= null;
			               String data1= null;  
			               String data2= null;
			               String data3 = null;
			               String data4 = null;
			               String data5 = null;
			        	   pubreport.clear();
			        	   
			        	   PublisherReport obj = new PublisherReport();

							
							data0 = "http://m.facebook.com";
							data1 = "3026.0";
						    data2 = "Social";
						    data3 = "305";
						    data4 = "110";
						    data5 = "facebook.com";
						
						    obj.setReferrerMasterDomain(data0);
							
							obj.setReferrerType(data2);
						    obj.setShares(data3);
						    obj.setLikes(data4);
						    
						    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								{
						    	
						    	 obj.setCount(data1);
								  
								}
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								{
									
									obj.setEngagementTime(data1);
								}
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								{
									
									obj.setVisitorCount(data1);
							
								}	    
						    
						    

				        PublisherReport obj1 = new PublisherReport();
							
						    data0 = "http://www.facebook.com";
							data1 = "1001.0";
						    data2 = "Social";
						    data3=  "207";
						    data4 = "53";
					        data5 = "facebook.com";
						
					        obj1.setReferrerMasterDomain(data0);
							
							obj1.setReferrerType(data2);
						    obj1.setShares(data3);
						    obj1.setLikes(data4);
						    
						    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								{
						    	
						    	 obj1.setCount(data1);
								  
								}
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								{
									
									obj1.setEngagementTime(data1);
								}
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								{
									
									obj1.setVisitorCount(data1);
							
								}	
					        
					        
					        
					        
					        PublisherReport obj2 = new PublisherReport();
						
							data0 = "http://l.facebook.com";
						  	data1 = "360.0";
						    data2 = "Social";
						    data3 = "103";
						    data4 = "12";
					        data5 = "facebook.com";
						
					        
					        obj2.setReferrerMasterDomain(data0);
							
							obj2.setReferrerType(data2);
						    obj2.setShares(data3);
						    obj2.setLikes(data4);
						    
						    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								{
						    	
						    	 obj2.setCount(data1);
								  
								}
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								{
									
									obj2.setEngagementTime(data1);
								}
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								{
									
									obj2.setVisitorCount(data1);
							
								}	
					        
					        
					        
					   PublisherReport obj3 = new PublisherReport();
							data0 = "http://www.google.co.pk";
							data1 = "396.0";
						    data2 = "Search";
						    data3 = "0";
						    data4 = "0";
						    data5 = "google.com";
						
						    obj3.setReferrerMasterDomain(data0);
							
							obj3.setReferrerType(data2);
						    obj3.setShares(data3);
						    obj3.setLikes(data4);
						    
						    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								{
						    	
						    	 obj3.setCount(data1);
								  
								}
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								{
									
									obj3.setEngagementTime(data1);
								}
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								{
									
									obj3.setVisitorCount(data1);
							
								}	   
					
						
					   PublisherReport obj4 = new PublisherReport();	    
							data0 = "http://www.google.co.in";
							data1 = "2871.0";
						    data2 = "Search";
						    data3 = "0";
						    data4 = "0";
						    data5 = "google.com";
						
						
		                obj4.setReferrerMasterDomain(data0);
						
						obj4.setReferrerType(data2);
					    obj4.setShares(data3);
					    obj4.setLikes(data4);
					    
					    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							{
					    	
					    	 obj4.setCount(data1);
							  
							}
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							{
								
								obj4.setEngagementTime(data1);
							}
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							{
								
								obj4.setVisitorCount(data1);
						
							}	
					
						
							PublisherReport obj5 = new PublisherReport();

							
						//	data0 = "http://m.facebook.com";
							data1 = "4387.0";
						    data2 = "Social";
						    data3 = "615";
						    data4 = "175";
						    data0 = "facebook.com";
							
						    obj5.setReferrerMasterDomain(data0);
							
							obj5.setReferrerType(data2);
						    obj5.setShares(data3);
						    obj5.setLikes(data4);
						    
						    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								{
						    	
						    	 obj5.setCount(data1);
								  
								}
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								{
									
									obj5.setEngagementTime(data1);
								}
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								{
									
									obj5.setVisitorCount(data1);
							
								}	
						
						    
						    PublisherReport obj6 = new PublisherReport();

							
						   // data0 = "http://www.google.co.in";
							data1 = "3267.0";
						    data2 = "Search";
						    data3 = "0";
						    data4 = "0";
						    data0 = "google.com";
						
		                    obj6.setReferrerMasterDomain(data0);
							
							obj6.setReferrerType(data2);
						    obj6.setShares(data3);
						    obj6.setLikes(data4);
						    
						    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								{
						    	
						    	 obj6.setCount(data1);
								  
								}
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								{
									
									obj6.setEngagementTime(data1);
								}
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								{
									
									obj6.setVisitorCount(data1);
							
								}	
							
						    
						String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);
						obj.setChannelName(channel_name1);
						obj1.setChannelName(channel_name1);
						obj2.setChannelName(channel_name1);
						obj3.setChannelName(channel_name1);
						obj4.setChannelName(channel_name1);
						obj5.setChannelName(channel_name1);
						obj6.setChannelName(channel_name1);
						obj1.setSection(sectionid);
						obj2.setSection(sectionid);	
						obj3.setSection(sectionid);
						obj4.setSection(sectionid);
						obj5.setSection(sectionid);
						obj6.setSection(sectionid);
						
						obj5.getChildren().add(obj);
						obj5.getChildren().add(obj1);
						obj5.getChildren().add(obj2);
						
						obj6.getChildren().add(obj3);
						obj6.getChildren().add(obj4);
						
						
						
						pubreport.add(obj5);

						pubreport.add(obj6);
					}
			    
			          /* 
			           
			           if (queryfield.equals("device")) {

			        	   String data0= null;
			               String data1= null;   
			        	   pubreport.clear();
			        	   
			        	   for (int i = 0; i < 3; i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        
						       
						          //if(data1[0].equals()) 
						         
						          if(i == 0){
						          data0="Mobile";
						          data1 = "1067.0";
						          }
						          

						          if(i == 1){
						          data0="Tablet";
						          data1 = "305.0";
						          }
						          
						          
						          if(i == 2){
							          data0="Desktop";
							          data1 = "743.0";
							      }
							    
						        
						          obj.setDevice_type(data0);
						          obj.setCount(data1);
						          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);			        
						          obj.setSection(sectionid);
						          pubreport.add(obj);
						        
						   //   }
						    //  System.out.println(headers1);
						    //  System.out.println(lines1);
						   }
			    
			           }
			    
			           if (queryfield.equals("incomelevel")) {

			        	   String data0= null;
			               String data1= null;   
			        	   pubreport.clear();
			           
			           for (int i = 0; i < 3; i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					       // String[] data1 = ((String)lines1.get(i)).split(",");
					       
					          //if(data1[0].equals()) 
					         
					          if(i == 0){
					          data0="Medium";
					          data1 = "700.0";
					          }
					          

					          if(i == 1){
					          data0="High";
					          data1 = "904.0";
					          }
					          
					          
					          if(i == 2){
						          data0="Low";
						          data1 = "67.0";
						      }
						    
					        
					          obj.setIncomelevel(data0);
					          obj.setCount(data1);
					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					          obj.setSection(sectionid);
					          pubreport.add(obj);
					        
					   //   }
					    //  System.out.println(headers1);
					    //  System.out.println(lines1);
					      }
			           }
			         
			           */
			           
			           if (queryfield.equals("referrerType")) {

			        	   String data0= null;
			               String data1= null;   
			        	   pubreport.clear();
			           
			           for (int i = 0; i < 3; i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					       // String[] data1 = ((String)lines1.get(i)).split(",");
					       
					          //if(data1[0].equals()) 
					         
					          if(i == 0){
					          data0="Social";
					          data1 = "806.0";
					          }
					          

					          if(i == 1){
					          data0="Search";
					          data1 = "1077.0";
					          }
					          
					          
					          if(i == 2){
						          data0="Direct";
						          data1 = "115.0";
						      }
						    
					        
					          obj.setReferrerSource(data0);
					          if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
									obj.setCount(data1);
									if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
									obj.setEngagementTime(data1);
									if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
									obj.setVisitorCount(data1);
					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					          obj.setSection(sectionid);
					          pubreport.add(obj);
					        
					   //   }
					    //  System.out.println(headers1);
					    //  System.out.println(lines1);
					      }
			           }       
				    
				    
				    
				    return pubreport;
				  }
				  
				 
		  public List<PublisherReport> getQueryFieldChannelSectionFilter(String queryfield,String startdate, String enddate, String channel_name, String sectionname, Map<String,String>filter, String filtermetric)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    
			        int size = filter.size();
			        String queryfilterbuilder = "";
			        String formattedString = "";
			        int j =0;
			        for (Map.Entry<String, String> entry : filter.entrySet())
			        {
			        	if (j==0){
			                formattedString = addCommaString(entry.getValue());
			        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
			        	
			        	}
			            else{
			            formattedString = addCommaString(entry.getValue());	
			            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
			       
			            }
			            j++;
			         
			        }
			  
			        String query = "";
			  
			        if(filtermetric == null || filtermetric.isEmpty() ||  filtermetric.equals("pageviews"))
			        query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
				    
			        if(filtermetric != null && !filtermetric.isEmpty() && filtermetric.equals("engagementTime") )
			        query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
				    
			        if(filtermetric != null && !filtermetric.isEmpty() && filtermetric.equals("visitorCount"))
				    query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+"";
			        
			        
			        System.out.println(query);
			        CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    if(filtermetric != null && !filtermetric.isEmpty()  && filtermetric.equals("visitorCount") )
				        lines = processList(lines);
				    
				    
				    
				    
				    //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        try{
				    	  
				    	  PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				     //   String demographicproperties = demographicmap.get(data[0]);
				            if(queryfield.equals("gender"))
				        	obj.setGender(data[0]);
				        
				            if(queryfield.equals("device"))
				        	obj.setDevice_type(data[0]);
				        	
				        	if(queryfield.equals("city")){
				        	    try{
				        		String locationproperties = citycodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-"," ");
						        data[0] = capitalizeString(data[0]);
						        obj.setCity(data[0]);
						        System.out.println(data[0]);
						        obj.setLocationcode(locationproperties);
				        	    }
				        	    catch(Exception e)
				        	    {
				        	    	continue;
				        	    }
				        	    
				        	    }
				        	if(queryfield.equals("audience_segment"))
				             {
				        		String audienceSegment = audienceSegmentMap.get(data[0]);
				        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
				        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
				        		obj.setAudience_segment(audienceSegment);
				        		obj.setAudienceSegmentCode(audienceSegmentCode);
				        		}
				        		else
				        	    obj.setAudience_segment(data[0]);
				        		
				             }
				        	
				        	if(queryfield.equals("reforiginal"))
					             obj.setReferrerSource(data[0]);
				            	
				        	if(queryfield.equals("agegroup"))
				        	{
				        		 data[0]=data[0].replace("_","-");
				        		 data[0]=data[0]+ " Years";
				        		 if(data[0].contains("medium")==false)
				        		 obj.setAge(data[0]);
				        	}
				            	
				            	
				        	if(queryfield.equals("incomelevel"))
					          obj.setIncomelevel(data[0]);
				     
				        	
				        	if(queryfield.equals("system_os")){
				        		String osproperties = oscodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-", " ");
						        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
						        String [] osParts = oscodeMap1.get(osproperties).split(",");
						        obj.setOs(osParts[0]);
						        obj.setOSversion(osParts[1]);
						        obj.setOscode(osproperties);
				        	}
				         	
				        	if(queryfield.equals("modelName")){
				        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
					        	
						        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
						        System.out.println(mobiledeviceproperties[2]);
						        obj.setDevicecode(mobiledeviceproperties[0]);
						        System.out.println(mobiledeviceproperties[0]);
				        	}
				         	
				        	if(queryfield.equals("brandName")){
				        		 data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
				        		obj.setBrandname(data[0]);
				        	}
                       
				        	if(queryfield.equals("refcurrentoriginal"))
				  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);if(filter.containsKey("authorName")){String authorName = article.getAuthor();obj.setArticleAuthor(filter.get("authorName"));String authorId = article.getAuthorId();obj.setAuthorId(AuthorMap1.get(filter.get("authorName")));}List<String> tags1 = article.getTags();if(tags1!=null) {obj.setArticleTag(tags1);}if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
				        	

				            Random random = new Random();	
				            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
				            Integer max = (int)Double.parseDouble(data[1]);
				            Integer randomNumber1 = random.nextInt(max) + 1;
				            
				            if(queryfield.equals("audience_segment"))	
				            {
				            obj.setCount(data[1]); 	
				            obj.setExternalWorldCount(randomNumber.toString());	
				            obj.setVisitorCount(randomNumber1.toString());
				            obj.setAverageTime("0.0");	

					        obj.setSection(sectionname);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        pubreport.add(obj);
				            }
				            
				            else if(queryfield.equals("agegroup")==true) {
				            	
				            	if(data[0].contains("medium")==false){
				            		if(filtermetric == null || filtermetric.isEmpty() ||  filtermetric.equals("pageviews"))
						            	obj.setCount(data[1]);
						            
						                if(filtermetric != null && !filtermetric.isEmpty() && filtermetric.equals("engagementTime") )
						                obj.setEngagementTime(data[1]);
						           
						                if(filtermetric != null && !filtermetric.isEmpty()  && filtermetric.equals("visitorCount") )
						                obj.setVisitorCount(data[1]);

							         obj.setSection(sectionname);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        pubreport.add(obj);
				            	}
				            }
				            
                             else if(queryfield.equals("refcurrentoriginal")==true) {
				            	
                    	        obj.setCount(data[1]);
                    	        Random randomv1 = new Random();	
    				            Integer randomNumberv1 = random.nextInt(1000 + 1 - 500) + 500;
    				            Integer maxv1 = (int)Double.parseDouble(data[1]);
    				            Integer randomNumber2 = random.nextInt(maxv1) + 1;
    				            Integer randomNumber3 = random.nextInt(maxv1) + 1;
    				            Integer value = (Integer)(randomNumber2/randomNumber3) + 1;
    				            obj.setAverageTime(value.toString());
    				            obj.setEngagementTime(randomNumber2.toString());
    				            obj.setVisitorCount(randomNumber3.toString());
                    	        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						       
						        pubreport.add(obj);
			            	}
				            else{
				            	if(filtermetric == null || filtermetric.isEmpty() ||  filtermetric.equals("pageviews"))
					            	obj.setCount(data[1]);
					            
					                if(filtermetric != null && !filtermetric.isEmpty() && filtermetric.equals("engagementTime") )
					                obj.setEngagementTime(data[1]);
					           
					                if(filtermetric != null && !filtermetric.isEmpty()  && filtermetric.equals("visitorCount") )
					                obj.setVisitorCount(data[1]);

					         obj.setSection(sectionname);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        pubreport.add(obj);
				            
				            }
				       
				        }
				        catch(Exception e){
				        	continue;
				        	
				        }
				        
				        
				        }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
		  
		  
		  
				/*  
				  public List<PublisherReport> getQueryFieldChannelSectionFilter(String queryfield,String startdate, String enddate, String channel_name, String sectionname, Map<String,String>filter)
						    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
						  {
						    
					        int size = filter.size();
					        String queryfilterbuilder = "";
					        String formattedString = "";
					        int j =0;
					        for (Map.Entry<String, String> entry : filter.entrySet())
					        {
					        	if (j==0){
					                formattedString = addCommaString(entry.getValue());
					        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
					        	
					        	}
					            else{
					            formattedString = addCommaString(entry.getValue());	
					            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
					       
					            }
					            j++;
					         
					        }
					  
					        String query = "";
					  
					        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					        query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
						    
					        if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
					        query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
						    
					        if(filter != null && !filter.isEmpty() && filter.equals("visitorCount"))
						    query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
					        
					        
					        System.out.println(query);
					        CSVResult csvResult = getCsvResult(false, query);
						    List<String> headers = csvResult.getHeaders();
						    List<String> lines = csvResult.getLines();
						    List<PublisherReport> pubreport = new ArrayList();
						    
						    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
						        lines = processList(lines);
						    
						    
						    
						    
						    //System.out.println(headers);
						    //System.out.println(lines);
						    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
						    {
						      for (int i = 0; i < lines.size(); i++)
						      {
						        try{
						    	  
						    	  PublisherReport obj = new PublisherReport();
						        
						        String[] data = ((String)lines.get(i)).split(",");
						     //   String demographicproperties = demographicmap.get(data[0]);
						            if(queryfield.equals("gender"))
						        	obj.setGender(data[0]);
						        
						            if(queryfield.equals("device"))
						        	obj.setDevice_type(data[0]);
						        	
						        	if(queryfield.equals("city")){
						        	    try{
						        		String locationproperties = citycodeMap.get(data[0]);
								        data[0]=data[0].replace("_"," ").replace("-"," ");
								        data[0] = capitalizeString(data[0]);
								        obj.setCity(data[0]);
								        System.out.println(data[0]);
								        obj.setLocationcode(locationproperties);
						        	    }
						        	    catch(Exception e)
						        	    {
						        	    	continue;
						        	    }
						        	    
						        	    }
						        	if(queryfield.equals("audience_segment"))
						             {
						        		String audienceSegment = audienceSegmentMap.get(data[0]);
						        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
						        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
						        		obj.setAudience_segment(audienceSegment);
						        		obj.setAudienceSegmentCode(audienceSegmentCode);
						        		}
						        		else
						        	    obj.setAudience_segment(data[0]);
						        		
						             }
						        	
						        	if(queryfield.equals("reforiginal"))
							             obj.setReferrerSource(data[0]);
						            	
						        	if(queryfield.equals("agegroup"))
						        	{
						        		 data[0]=data[0].replace("_","-");
						        		 data[0]=data[0]+ " Years";
						        		 if(data[0].contains("medium")==false)
						        		 obj.setAge(data[0]);
						        	}
						            	
						            	
						        	if(queryfield.equals("incomelevel"))
							          obj.setIncomelevel(data[0]);
						     
						        	
						        	if(queryfield.equals("system_os")){
						        		String osproperties = oscodeMap.get(data[0]);
								        data[0]=data[0].replace("_"," ").replace("-", " ");
								        data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
								        String [] osParts = oscodeMap1.get(osproperties).split(",");
								        obj.setOs(osParts[0]);
								        obj.setOSversion(osParts[1]);
								        obj.setOscode(osproperties);
						        	}
						         	
						        	if(queryfield.equals("modelName")){
						        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
							        	
								        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
								        System.out.println(mobiledeviceproperties[2]);
								        obj.setDevicecode(mobiledeviceproperties[0]);
								        System.out.println(mobiledeviceproperties[0]);
						        	}
						         	
						        	if(queryfield.equals("brandName")){
						        		 data[0]= AggregationModule.capitalizeFirstLetter(data[0]);
						        		obj.setBrandname(data[0]);
						        	}
                                 
						        	if(queryfield.equals("refcurrentoriginal"))
						  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
						        	

						            Random random = new Random();	
						            Integer randomNumber = random.nextInt(1000 + 1 - 500) + 500;
						            Integer max = (int)Double.parseDouble(data[1]);
						            Integer randomNumber1 = random.nextInt(max) + 1;
						            
						            if(queryfield.equals("audience_segment"))	
						            {
						            obj.setCount(data[1]); 	
						            obj.setExternalWorldCount(randomNumber.toString());	
						            obj.setVisitorCount(randomNumber1.toString());
						            obj.setAverageTime("0.0");	

							        obj.setSection(sectionname);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        pubreport.add(obj);
						            }
						            
						            else if(queryfield.equals("agegroup")==true) {
						            	
						            	if(data[0].contains("medium")==false){
						            		if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								            	obj.setCount(data[1]);
								            
								                if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								                obj.setEngagementTime(data[1]);
								           
								                if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								                obj.setVisitorCount(data[1]);

									         obj.setSection(sectionname);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        pubreport.add(obj);
						            	}
						            }
						            
						            
						            else{
						            	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							            	obj.setCount(data[1]);
							            
							                if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							                obj.setEngagementTime(data[1]);
							           
							                if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							                obj.setVisitorCount(data[1]);

							         obj.setSection(sectionname);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        pubreport.add(obj);
						            
						            }
						       
						        }
						        catch(Exception e){
						        	continue;
						        	
						        }
						        
						        
						        }
						      //System.out.println(headers);
						      //System.out.println(lines);
						    }
						    return pubreport;
						  }
				  */

				
				  public List<PublisherReport> getQueryFieldChannelSectionGroupBy(String queryfield,String startdate, String enddate, String channel_name, String sectionname,List<String> groupby, String  filter)
						    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
						  {
						    
					      
					    
				      String querygroupbybuilder = convert(groupby);
				      List<PublisherReport> pubreport = new ArrayList();
				      String query = "";
				      
				      int  l=0;
				      
				      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				    	 	query = "Select count(*),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
				    	    
				    	    
				    	    if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime"))
				    	     	query = "Select SUM(engagementTime),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
				    	        
				    	    
				    	    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount"))
				    	     	query = "Select "+queryfield+","+querygroupbybuilder+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder+"";
				    	        

				    	    if(querygroupbybuilder.equals("hour")){
				    	    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				    	    	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
				    	   
				    	    	
				    	    }
				    	  
				    	    if(querygroupbybuilder.equals("minute")){
				    	    	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				    	    	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
				    	   		    
				    	    } 	
				    	    
				    	    if(querygroupbybuilder.equals("second")){
				    	   // 	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				    	    //	query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
				    	   		    
				    	    } 	
				    	    
				    	    if(querygroupbybuilder.equals("hour")){
				    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
				    	    	query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
				    	   
				    	    	
				    	    }
				    	  
				    	    if(querygroupbybuilder.equals("minute")){
				    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
				    	    	query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
				    	   		    
				    	    } 	
				    	    
				    	    if(querygroupbybuilder.equals("second")){
				    	    //	if(filter != null && !filter.isEmpty() &&  filter.equals("engagementTime"))
				    	    //	query = "Select SUM(engagementTime),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
				    	   		    
				    	    } 	
				    	    
				    	   
				    	    if(querygroupbybuilder.equals("hour")){
				    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
				    	    	query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1h')";
				    	   
				    	    	
				    	    }
				    	  
				    	    if(querygroupbybuilder.equals("minute")){
				    	    	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
				    	    	query = "Select "+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1m')";
				    	   		    
				    	    } 	
				    	    
				    	    if(querygroupbybuilder.equals("second")){
				    	 //   	if(filter != null && !filter.isEmpty() &&  filter.equals("visitorCount"))
				    	    //	query = "Select "
				    	    	//		+ ","+queryfield+",COUNT(DISTINCT(cookie_id))  FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+"date_histogram(field='request_time','interval'='1s')";
				    	   		    
				    	    } 	
				    	    
				    	    
				    	    
				    	    
				    	    
				    	    
				    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("totalViews"))
				    	    {
				    	    	
				    	    	query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
				    	    }
				    	    
				    	    	
				    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("totalViews"))
				    	    {
				    	   	   if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				    	    	query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
				    	    }
				    	    	
				    	    if(querygroupbybuilder.equals("second") && queryfield.equals("totalViews"))
				    	    {
				    	    //	if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				    	      //  query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
				    	    }
				    	    
				    	    	
				    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("uniqueVisitors") )
				    	    {
				    	    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + 
				    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
				    	    	
				    	    }
				    	 		    
				    			    	
				    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("uniqueVisitors") )
				    	    {
				    	    	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + 
				    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
				    	    	
				    	    }
				    	    	
				    	    
				    	    if(querygroupbybuilder.equals("second") && queryfield.equals("uniqueVisitors"))
				    	    {
				    	    //	query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + 
				    			//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
				    	    	
				    	    }
				    	    
				    	    
				    	    if(querygroupbybuilder.equals("hour") && queryfield.equals("engagementTime"))
				    	    {
				    	    	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + 
				    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
				    	    	
				    	    }
				    	 		    
				    			    	
				    	    if(querygroupbybuilder.equals("minute") && queryfield.equals("engagementTime")  )
				    	    {
				    	    	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + 
				    				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
				    	    	
				    	    }
				    	    	
				    	    
				    	    if(querygroupbybuilder.equals("second") && queryfield.equals("engagementTime") )
				    	    {
				    	   // 	query = "SELECT SUM(engagementTime) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and  channel_name = '" + 
				    			//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1s')";
				    	    	
				    	    }
						    	
						    
				         	
				         	System.out.println(query);
				         	CSVResult csvResult = getCsvResult(false, query);
						    List<String> headers = csvResult.getHeaders();
						    List<String> lines = csvResult.getLines();
						   
						    if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							    lines = processList1(lines);
						    
						   
						    if(queryfield.equals("audience_segment") && groupby.get(0).equals("subcategory")== true){
							    
							    
							    List<List<String>> data = new ArrayList<List<String>>();
							    for (int i = 0; i < lines.size(); i++) {
						            List<String> objects = new ArrayList<String>();
						            String [] parts = lines.get(i).split(",");
						            for(int j =0; j< parts.length; j++)
						              objects.add(parts[j]);
						           
						            data.add(objects);
						        }
							    
							    
							    ResultSet obj = ListtoResultSet.getResultSet(headers, data);
							    
							    
							//    JSONArray json = Convertor.convertResultSetIntoJSON(obj);
							 //   String s = json.toString();
							    pubreport= NestedJSON.getNestedJSONObject(obj, queryfield, groupby,filter); 
							 //   System.out.println(nestedJson);
							    return pubreport;
							    
							    }
						    
						    
						    
						    
						    //System.out.println(headers);
						    //System.out.println(lines);
						    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
						    {
						      for (int i = 0; i < lines.size(); i++)
						      {
						        try{
						    	  
						    	PublisherReport obj = new PublisherReport();
						        
						        String[] data = ((String)lines.get(i)).split(",");
						     //   String demographicproperties = demographicmap.get(data[0]);
						        
						            if(queryfield.equals("gender"))
						        	obj.setGender(data[0]);
						        
						            if(queryfield.equals("device"))
						        	obj.setDevice_type(data[0]);
						        	
						        	if(queryfield.equals("city")){
						        		try{
						        		String locationproperties = citycodeMap.get(data[0]);
								        data[0]=data[0].replace("_"," ").replace("-"," ");
								        data[0] = capitalizeString(data[0]);
								        obj.setCity(data[0]);
								        System.out.println(data[0]);
								        obj.setLocationcode(locationproperties);
						        		}
						        		catch(Exception e)
						        		{
						        			continue;
						        		}
						        		}
						        	
						        	if(queryfield.equals("audience_segment"))
						        	{
						        		
						        		String audienceSegment = audienceSegmentMap.get(data[0]);
						        		String audienceSegmentCode = audienceSegmentMap2.get(data[0]);
						        		if(audienceSegment!=null && !audienceSegment.isEmpty()){
						        		obj.setAudience_segment(audienceSegment);
						        		obj.setAudienceSegmentCode(audienceSegmentCode);
						        		}
						        		else
						        	    obj.setAudience_segment(data[0]);
						        		
						        		
						        	}
						        	
						        	if(queryfield.equals("reforiginal"))
							             obj.setReferrerSource(data[0]);
						            	
						        	if(queryfield.equals("agegroup"))
						        	{
						        		 data[0]=data[0].replace("_","-");
						        		 data[0]=data[0]+ " Years";
						        		 if(data[0].contains("medium")==false)
						        		 obj.setAge(data[0]);
						        	}
						            	
						        	  if(queryfield.equals("state"))
						            	{
						            	
						            	data[0]=data[0].replace("_", " ");
						            	data[0] = capitalizeString(data[0]);
						            	obj.setState(data[0]);
						            	}
						            
						            
						            if(queryfield.equals("country"))
						        	  {
						        	
						            	data[0]=data[0].replace("_", " ");
						            	data[0] = capitalizeString(data[0]);
						            	obj.setCountry(data[0]);
						             	}
						        	
						        	
										        	
						        	if(queryfield.equals("incomelevel"))
							          obj.setIncomelevel(data[0]);
						     
						        	
						        	if(queryfield.equals("system_os")){
						        		String osproperties = oscodeMap.get(data[0]);
								        data[0]=data[0].replace("_"," ").replace("-", " ");
								        data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
								        String [] osParts = oscodeMap1.get(osproperties).split(",");
								        obj.setOs(osParts[0]);
								        obj.setOSversion(osParts[1]);
								        obj.setOscode(osproperties);
						        	}
						         	
						        	if(queryfield.equals("modelName")){
						        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
							        	
								        obj.setMobile_device_model_name(mobiledeviceproperties[2]);
								        System.out.println(mobiledeviceproperties[2]);
								        obj.setDevicecode(mobiledeviceproperties[0]);
								        System.out.println(mobiledeviceproperties[0]);
						        	}
						         	
						        	if(queryfield.equals("brandName")){
						        		 data[0]= AggregationModule2.capitalizeFirstLetter(data[0]);
						        		obj.setBrandname(data[0]);
						        	}

						        	if(queryfield.equals("refcurrentoriginal"))
						  	          {String articleparts[] = data[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data[0]); Article article = GetMiddlewareData.getArticleMetaData(data[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}}
						        	
						        	
						        	
						        	
						        	//   obj.setCode(code);
					            for(int k = 0; k < groupby.size(); k++)
					            {
					            	
					            	if(groupby.get(k).equals(queryfield)==false)
					            	{
					            	try{
					            	if(groupby.get(k).equals("device"))
					            	obj.setDevice_type(data[k+1]);
					            	
					            	if(groupby.get(k).equals("city"))
					            	{
					            		try{
					            		String locationproperties = citycodeMap.get(data[k+1]);
					    		        data[k+1]=data[k+1].replace("_"," ").replace("-"," ");
					    		        data[k+1] = capitalizeString(data[k+1]);
					    		        obj.setCity(data[k+1]);
					    		        System.out.println(data[k+1]);
					    		        obj.setLocationcode(locationproperties);
					            		}
					            		catch(Exception e)
					            		{
					            			continue;
					            		}
					            	}
					            	
					            	if(groupby.get(k).equals("audience_segment"))
					            	{
					            		
					            		String audienceSegment = audienceSegmentMap.get(data[k+1]);
					            		String audienceSegmentCode = audienceSegmentMap2.get(data[k+1]);
					            		if(audienceSegment!=null && !audienceSegment.isEmpty()){
					            		obj.setAudience_segment(audienceSegment);
					            		obj.setAudienceSegmentCode(audienceSegmentCode);
					            		}
					            		else
					            	    obj.setAudience_segment(data[k+1]);
					            		
					            	}
					            	
					            	
					            	 if(groupby.get(k).equals("state"))
						             	{
						             	
						             	data[k+1]=data[k+1].replace("_", " ");
						             	data[k+1] = capitalizeString(data[k+1]);
						             	obj.setState(data[k+1]);
						             	}
						             
						             
						             if(groupby.get(k).equals("country"))
						         	  {
						         	
						             	data[k+1]=data[k+1].replace("_", " ");
						             	data[k+1] = capitalizeString(data[k+1]);
						             	obj.setCountry(data[k+1]);
						              	}
					            	
					            	
					            	if(groupby.get(k).equals("gender"))
							             obj.setGender(data[k+1]);
					            	
					            	
					            	if(groupby.get(k).equals("date"))
							             obj.setDate(data[k+1]);
						            	
					            	if(groupby.get(k).equals("hour"))
							             obj.setDate(data[k+1]);
					            	
					            	if(groupby.get(k).equals("minute"))
							             obj.setDate(data[k+1]);
					            	
					            	if(groupby.get(k).equals("subcategory"))
					            	 {
					            		String audienceSegment = audienceSegmentMap.get(data[k+1]);
					            		String audienceSegmentCode = audienceSegmentMap2.get(data[k+1]);
					            		if(audienceSegment!=null && !audienceSegment.isEmpty()){
					            		obj.setSubcategory(audienceSegment);
					            		obj.setSubcategorycode(audienceSegmentCode);
					            		}
					            		else
					            	    obj.setSubcategory(data[k+1]);
							             }
					            	
					            	if(groupby.get(k).equals("refcurrentoriginal"))
							             obj.setReferrerSource(data[k+1]);
						            	
					            	if(groupby.get(k).equals("agegroup"))
					            	{
						        		 data[k+1]=data[k+1].replace("_","-");
						        		 data[k+1]=data[k+1]+ " Years";
						        		 if(data[k+1].contains("medium")==false)
						        		 obj.setAge(data[k+1]);
						        	}
						            	
						            	
					            	if(groupby.get(k).equals("incomelevel"))
							          obj.setIncomelevel(data[k+1]);
						            	
				                    l++;
					            	}
					            	catch(Exception e){
					            	continue;
					            	}
					            	}
					            }
						        
					            
					            	            
					            if(l!=0){
					            	 
			            	        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					            	obj.setCount(data[l+1]);
					             
						            if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					                obj.setVisitorCount(data[l+1]);
					         

						            if(filter != null && !filter.isEmpty()  && filter.equals("engagementTime") )
					                obj.setEngagementTime(data[l+1]);
						        
					            }   
						        obj.setSection(sectionname);
						        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						        pubreport.add(obj);
						        l=0;
						        }
						        catch(Exception e)
						        {
						        	continue;
						        }
						        
						        }
						      //System.out.println(headers);
						      //System.out.println(lines);
						    }
						    return pubreport;
						  }
				  
				  	  
  
  
  public List<PublisherReport> getAgegroupChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),agegroup from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY agegroup";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
    //    String demographicproperties = demographicmap.get(data[0]);
        obj.setAge(data[0]);
  //      obj.setCode(code);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getISPChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),ISP from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY ISP";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
        obj.setISP(data[0]);
        obj.setCount(data[1]);
        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
        pubreport.add(obj);
         }
        }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getOrgChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
    CSVResult csvResult1 = getCsvResult(false, query1);
    List<String> headers1 = csvResult1.getHeaders();
    List<String> lines1 = csvResult1.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines1.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data1 = ((String)lines1.get(i)).split(",");
        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
        {
          obj.setOrganisation(data1[0]);
          obj.setCount(data1[1]);
          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
          pubreport.add(obj);
        }
      }
      //System.out.println(headers1);
      //System.out.println(lines1);
    }
    return pubreport;
  }
  
  
  
  public PublisherReport getUserdetailsChannel(String startdate, String enddate, String channel_name,Map<String,String> filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	  
	  int size = filter.size();
      String queryfilterbuilder = "";
      String formattedString = "";
      String query = "";
      int j =0;
      for (Map.Entry<String, String> entry : filter.entrySet())
      {
      	if (j==0){
              formattedString = addCommaString(entry.getValue());
      		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
      	
      	}
          else{
          formattedString = addCommaString(entry.getValue());	
          queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
     
          }
          j++;
       
      }

		  
	        String query1 = "Select Distinct(gender) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult1 = getCsvResult(false, query1);
		    List<String> headers1 = csvResult1.getHeaders();
		    List<String> lines1 = csvResult1.getLines();
		   
		    String query2 = "Select Distinct(agegroup) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult2 = getCsvResult(false, query2);
		    List<String> headers2 = csvResult2.getHeaders();
		    List<String> lines2 = csvResult2.getLines();
		    
		    
		    String query3 = "Select Distinct(incomelevel) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult3 = getCsvResult(false, query3);
		    List<String> headers3 = csvResult3.getHeaders();
		    List<String> lines3 = csvResult3.getLines();
		    
		    
		    
		    
		    String query5 = "Select Distinct(device) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult5 = getCsvResult(false, query5);
		    List<String> headers5 = csvResult5.getHeaders();
		    List<String> lines5 = csvResult5.getLines();
		    
		    

		    String query6 = "Select Distinct(modelName) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult6 = getCsvResult(false, query6);
		    List<String> headers6 = csvResult6.getHeaders();
		    List<String> lines6 = csvResult6.getLines();
		    
		    String query7 = "Select Distinct(system_os) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult7 = getCsvResult(false, query7);
		    List<String> headers7 = csvResult7.getHeaders();
		    List<String> lines7 = csvResult7.getLines();
		    
		   
		    
		    /*
		    String query8 = "Select contributionFactor,audience_segment from audiencesegmentcomputeindex1 where "+queryfilterbuilder;
		    CSVResult csvResult8 = getCsvResult(false, query8);
		    List<String> headers8 = csvResult8.getHeaders();
		    List<String> lines8 = csvResult8.getLines();
		    */
		    
		    
		    
		    
		    String query9 = "Select count(*) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult9 = getCsvResult(false, query9);
		    List<String> headers9 = csvResult9.getHeaders();
		    List<String> lines9 = csvResult9.getLines();
		    
		    String query10 = "Select SUM(engagementTime) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult10 = getCsvResult(false, query10);
		    List<String> headers10 = csvResult10.getHeaders();
		    List<String> lines10 = csvResult10.getLines();
		    
		    String query11 = "Select distinct(session_id) from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
		    CSVResult csvResult11 = getCsvResult(false, query11);
		    List<String> headers11 = csvResult11.getHeaders();
		    List<String> lines11 = csvResult11.getLines();
		    
		    String gender  = lines1.get(0);
		    String agegroup = lines2.get(0);
		    String incomelevel = lines3.get(0);
		    String device = lines5.get(0);
		    String os = lines7.get(0);
		    String modelName = lines6.get(0);
		    String engagementTime = lines10.get(0);
		    int sessioncount = lines11.size();
		    Integer sessioncount1 = sessioncount; 
		    String pageviews = lines9.get(0);
		    
		    
		    PublisherReport pubreport = new PublisherReport();
		    agegroup=agegroup.replace("_","-");
		    agegroup=agegroup+ " Years";
   		    if(agegroup.contains("medium")==true)
   		    agegroup="25-34 Years";
		    pubreport.setAge(agegroup);
		    pubreport.setGender(gender);
		    pubreport.setIncomelevel(incomelevel);
		    pubreport.setDevice_type(device);
		    String mobiledata = devicecodeMap.get(modelName.toLowerCase());
		    String[] mobiledeviceproperties;
		    if(mobiledata!=null && mobiledata.isEmpty()==false){
		    mobiledeviceproperties = mobiledata.split(",");
        	
		    pubreport.setMobile_device_model_name(mobiledeviceproperties[2]);
		    }
		    else{
		    	 pubreport.setDevice_type("Desktop");
		    	
		    }
		    os = os.replace("_"," ");
		    pubreport.setOs(os);
		    pubreport.setEngagementTime(engagementTime);
		    pubreport.setTotalvisits(pageviews);
		    pubreport.setSessionCount(sessioncount1.toString());
		    
		    return pubreport;
		  }
		  
  
  
  public List<PublisherReport> getUserdetailsCityChannel(String startdate, String enddate, String channel_name,Map<String,String> filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	  List<PublisherReport> pubreport = new ArrayList();
	  int size = filter.size();
      String queryfilterbuilder = "";
      String formattedString = "";
      String query = "";
      int j =0;
      for (Map.Entry<String, String> entry : filter.entrySet())
      {
      	if (j==0){
              formattedString = addCommaString(entry.getValue());
      		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
      	
      	}
          else{
          formattedString = addCommaString(entry.getValue());	
          queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
     
          }
          j++;
       
      }
	  
	  
	    String query4 = "Select count(*),city from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'"+" group by city";
	    CSVResult csvResult4 = getCsvResult(false, query4);
	    List<String> headers4 = csvResult4.getHeaders();
	    List<String> lines4 = csvResult4.getLines();
	    if ((lines4 != null) && (!lines4.isEmpty()) && (!((String)lines4.get(0)).isEmpty()))
	    {
	      for (int i = 0; i < lines4.size(); i++)
	      {
	        PublisherReport obj = new PublisherReport();
	        
	        String[] data1 = ((String)lines4.get(i)).split(",");
	        String locationproperties = citycodeMap.get(data1[0]);
	        data1[0]=data1[0].replace("_"," ").replace("-"," ");
	        data1[0]=capitalizeString(data1[0]);
	          obj.setCity(data1[0]);
	          obj.setCount(data1[1]);
	          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
	          pubreport.add(obj);
	       
	      }
	      //System.out.println(headers1);
	      //System.out.println(lines1);
	    }
  
          return pubreport;
  
		  }
  
  
  public List<PublisherReport> getUserdetailsISPChannel(String startdate, String enddate, String channel_name,Map<String,String> filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	  List<PublisherReport> pubreport = new ArrayList();
	  int size = filter.size();
    String queryfilterbuilder = "";
    String formattedString = "";
    String query = "";
    int j =0;
    for (Map.Entry<String, String> entry : filter.entrySet())
    {
    	if (j==0){
            formattedString = addCommaString(entry.getValue());
    		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
    	
    	}
        else{
        formattedString = addCommaString(entry.getValue());	
        queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
   
        }
        j++;
     
    }
	  
	  
    String query7 = "Select count(*),ISP from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'"+" group by ISP";
    CSVResult csvResult7 = getCsvResult(false, query7);
    List<String> headers7 = csvResult7.getHeaders();
    List<String> lines7 = csvResult7.getLines();
    
	    if ((lines7 != null) && (!lines7.isEmpty()) && (!((String)lines7.get(0)).isEmpty()))
	    {
	      for (int i = 0; i < lines7.size(); i++)
	      {
	        PublisherReport obj = new PublisherReport();
	        
	        String[] data1 = ((String)lines7.get(i)).split(",");
	       
	          obj.setISP(data1[0]);
	          obj.setCount(data1[1]);
	          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
	          pubreport.add(obj);
	       
	      }
	      //System.out.println(headers1);
	      //System.out.println(lines1);
	    }

        return pubreport;

		  }

  
  public List<PublisherReport> getUserdetailsSegmentChannel(String startdate, String enddate, String channel_name,Map<String,String> filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
	    
	  List<PublisherReport> pubreport = new ArrayList();
	  int size = filter.size();
    String queryfilterbuilder = "";
    String formattedString = "";
    String query = "";
    int j =0;
    for (Map.Entry<String, String> entry : filter.entrySet())
    {
    	if (j==0){
            formattedString = addCommaString(entry.getValue());
    		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
    	
    	}
        else{
        formattedString = addCommaString(entry.getValue());	
        queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
   
        }
        j++;
     
    }
	  
	    String query8 = "Select count(*),audience_segment,subcategory from enhanceduserdatabeta1 where "+queryfilterbuilder+"and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'"+" group by audience_segment,subcategory";
	    CSVResult csvResult8 = getCsvResult(false, query8);
	    List<String> headers8 = csvResult8.getHeaders();
	    List<String> lines8 = csvResult8.getLines();

	    if ((lines8 != null) && (!lines8.isEmpty()) && (!((String)lines8.get(0)).isEmpty()))
	    {
	      for (int i = 0; i < lines8.size(); i++)
	      {
	        PublisherReport obj = new PublisherReport();
	        
	        String[] data1 = ((String)lines8.get(i)).split(",");
	       
	          obj.setAudience_segment(audienceSegmentMap.get(data1[0]));
	          obj.setSubcategory(audienceSegmentMap.get(data1[1]));
	          obj.setCount(data1[2]);
	          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
	          pubreport.add(obj);
	       
	      }
	      //System.out.println(headers1);
	      //System.out.println(lines1);
	    }

        return pubreport;  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  }
  
  
  
  
  
  
  public List<PublisherReport> countBrandNameChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    String query = "SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName";
		    //System.out.println(query);
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
		        obj.setBrandname(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		        } 
		       }
		  //    //System.out.println(headers);
		  //    //System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countBrowserChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where channel_name ='" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setBrowser(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countOSChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os";
		    System.out.println(query);
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        String osproperties = oscodeMap.get(data[0]);
		        data[0]=data[0].replace("_"," ").replace("-", " ");
		        obj.setOs(data[0]);
		        obj.setOscode(osproperties);
		        System.out.println(data[0]);
		        obj.setCount(data[1]);
		        System.out.println(osproperties);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		    }
		    
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countModelChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");

		        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
		        String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
		        	
		        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
		        System.out.println(mobiledeviceproperties[1]);
		        obj.setDevicecode(mobiledeviceproperties[0]);
		        System.out.println(mobiledeviceproperties[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		        
		        }
		    }
		  
		    System.out.println(pubreport.toString());
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countCityChannelArticle(String startdate, String enddate, String channel_name, String articlename, String filter)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city order by count desc";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    Integer accumulatedCount = 0;
		    
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		      
		        String[] data = ((String)lines.get(i)).split(",");
		        if(i<10){
		        String locationproperties = citycodeMap.get(data[0]);
		        data[0]=data[0].replace("_"," ").replace("-"," ");
		        obj.setCity(data[0]);
		        System.out.println(data[0]);
		        obj.setLocationcode(locationproperties);
		        System.out.println(locationproperties);
		        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
					obj.setCount(data[1]);
					if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
					obj.setEngagementTime(data[1]);
					if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
					obj.setVisitorCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		       }
		       else{
		    	   
		    	  accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
		    	  
		    	  if(i == (lines.size()-1)){
		    		 obj.setCity("Others"); 
		    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				     String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				     if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(accumulatedCount.toString());
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(accumulatedCount.toString());
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(accumulatedCount.toString());
		    		 pubreport.add(obj);
		    		 System.out.println("Others");
                     System.out.println(accumulatedCount.toString());
		    	  }
		       }
		       }
		    }
		   
		    System.out.println(pubreport.toString());
		    return pubreport;
		  }
		  
		  
		  public List<PublisherReport> countStateChannelArticle(String startdate, String enddate, String channel_name, String articlename, String filter)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,state FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by state order by count desc";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    Integer accumulatedCount = 0;
				    
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				      
				        String[] data = ((String)lines.get(i)).split(",");
				        if(i<10){
				        	data[0]=data[0].replace("_", " ");
			            	data[0] = capitalizeString(data[0]);
			            	obj.setState(data[0]);
				        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data[1]);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data[1]);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				        pubreport.add(obj);
				       }
				       else{
				    	   
				    	  accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
				    	  
				    	  if(i == (lines.size()-1)){
				    		 obj.setState("Others"); 
				    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						     String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
						     if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
									obj.setCount(accumulatedCount.toString());
									if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
									obj.setEngagementTime(accumulatedCount.toString());
									if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
									obj.setVisitorCount(accumulatedCount.toString());
				    		 pubreport.add(obj);
				    		 System.out.println("Others");
		                     System.out.println(accumulatedCount.toString());
				    	  }
				       }
				       }
				    }
				   
				    System.out.println(pubreport.toString());
				    return pubreport;
				  }
		  
		  
		  
		  public List<PublisherReport> countCountryChannelArticle(String startdate, String enddate, String channel_name, String articlename, String filter)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,country FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by country order by count desc";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    Integer accumulatedCount = 0;
				    
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				      
				        String[] data = ((String)lines.get(i)).split(",");
				        if(i<10){
				        	data[0]=data[0].replace("_", " ");
			            	data[0] = capitalizeString(data[0]);
			            	obj.setCountry(data[0]);
				        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data[1]);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data[1]);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				        pubreport.add(obj);
				       }
				       else{
				    	   
				    	  accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
				    	  
				    	  if(i == (lines.size()-1)){
				    		 obj.setCountry("Others"); 
				    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						     String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
						     if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
									obj.setCount(accumulatedCount.toString());
									if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
									obj.setEngagementTime(accumulatedCount.toString());
									if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
									obj.setVisitorCount(accumulatedCount.toString());
				    		 pubreport.add(obj);
				    		 System.out.println("Others");
		                     System.out.println(accumulatedCount.toString());
				    	  }
				       }
				       }
				    }
				   
				    System.out.println(pubreport.toString());
				    return pubreport;
				  }
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  public List<PublisherReport> countfingerprintChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
			  
			  
		//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
			  
		    
			  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + 
				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
			  
			//	 CSVResult csvResult00 = getCsvResult(false, query00);
				// List<String> headers00 = csvResult00.getHeaders();
		//		 List<String> lines00 = csvResult00.getLines();
			//	 List<PublisherReport> pubreport00 = new ArrayList();  
				
				 
			//	System.out.println(headers00);
			//	System.out.println(lines00);  
				  
				//  for (int i = 0; i < lines00.size(); i++)
			    //  {
			       
			     //   String[] data = ((String)lines00.get(i)).split(",");
			  //      //System.out.println(data[0]);
			     
				  
				  
				  
				//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
			    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
			      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
			      CSVResult csvResult = getCsvResult(false, query);
			      List<String> headers = csvResult.getHeaders();
			      List<String> lines = csvResult.getLines();
			      List<PublisherReport> pubreport = new ArrayList();
			      System.out.println(headers);
			      System.out.println(lines);
			      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
			      for (int i = 0; i < lines.size(); i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			        String[] data = ((String)lines.get(i)).split(",");
			       // obj.setDate(data[0]);
			        obj.setReach(data[0]);
			        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
			        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
			        pubreport.add(obj);
			      }
			    }  
			    
		    return pubreport;
		  }
		  
	
		  public List<PublisherReport> countbenchmarkfingerprintChannelArticle(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					  String time = startdate;
                      DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                      Date date1 = df.parse(time);
                     
                      String time1 = enddate;
                      Date date2 = df.parse(time1);
                            
                     
                     
                      int days = getDifferenceDays(date2, date1)-2;
                      Calendar cal = Calendar.getInstance();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, days);
                      Date benchmarkStartDate1 = cal.getTime();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, -1);
                      Date benchmarkEndDate1 = cal.getTime();


                      String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                      String benchmarkEndDate  = df.format(benchmarkEndDate1);	
						  
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					       // obj.setDate(data[0]);
					        obj.setReach(data[0]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
		  
		  
		  
		  
		  public List<PublisherReport> countfingerprintChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        obj.setDate(data[0]);
					        obj.setReach(data[1]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
		  
		 
		  
		  public List<PublisherReport> countbenchmarkfingerprintChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
			  String time = startdate;
              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
              Date date1 = df.parse(time);
             
              String time1 = enddate;
              Date date2 = df.parse(time1);
                    
             
             
              int days = getDifferenceDays(date2, date1)-2;
              Calendar cal = Calendar.getInstance();
              cal.setTime(date1);
              cal.add(Calendar.DAY_OF_YEAR, days);
              Date benchmarkStartDate1 = cal.getTime();
              cal.setTime(date1);
              cal.add(Calendar.DAY_OF_YEAR, -1);
              Date benchmarkEndDate1 = cal.getTime();


              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
					  
              
              String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + benchmarkStartDate  + "'" + " and " + "'" + benchmarkEndDate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        obj.setDate(data[0]);
					        obj.setReach(data[1]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  public List<PublisherReport> countfingerprintChannelArticleHourwise(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        obj.setDate(data[0]);
					        obj.setReach(data[1]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
		  
		  
		  public List<PublisherReport> countbenchmarkfingerprintChannelArticleHourwise(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
					  String time = startdate;
                      DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                      Date date1 = df.parse(time);
                     
                      String time1 = enddate;
                      Date date2 = df.parse(time1);
                            
                     
                     
                      int days = getDifferenceDays(date2, date1)-2;
                      Calendar cal = Calendar.getInstance();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, days);
                      Date benchmarkStartDate1 = cal.getTime();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, -1);
                      Date benchmarkEndDate1 = cal.getTime();


                      String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                      String benchmarkEndDate  = df.format(benchmarkEndDate1);	
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        obj.setDate(data[0]);
					        obj.setReach(data[1]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  public List<PublisherReport> countbenchmarktotalvisitorsChannelArticle(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					  String time = startdate;
                      DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                      Date date1 = df.parse(time);
                     
                      String time1 = enddate;
                      Date date2 = df.parse(time1);
                            
                     
                     
                      int days = getDifferenceDays(date2, date1)-2;
                      Calendar cal = Calendar.getInstance();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, days);
                      Date benchmarkStartDate1 = cal.getTime();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, -1);
                      Date benchmarkEndDate1 = cal.getTime();


                      String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                      String benchmarkEndDate  = df.format(benchmarkEndDate1);	
						  
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" +benchmarkEndDate + "'";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					       // obj.setDate(data[0]);
					        obj.setTotalvisits(data[0]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
				  
			
		  
		  public List<PublisherReport> counttotalvisitorsChannelArticle(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					       // obj.setDate(data[0]);
					        obj.setTotalvisits(data[0]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
				  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
				  
				  public List<PublisherReport> counttotalvisitorsChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setTotalvisits(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
		  
				  public List<PublisherReport> countbenchmarktotalvisitorsChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
							  
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" +  benchmarkEndDate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setTotalvisits(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  
				  
				  
		  
				  public List<PublisherReport> EngagementTimeChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT SUM(engagementTime) as eT,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setEngagementTime(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
				  
				  
				  
				  public List<PublisherReport> benchmarkEngagementTimeChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
					  
					  String time = startdate;
					  DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					  Date date1 = df.parse(time);
					  
					  String time1 = enddate;
					  Date date2 = df.parse(time1);
					         
					  
					  
					  int days = getDifferenceDays(date1, date2);
					  Calendar cal = Calendar.getInstance();
					  cal.add(Calendar.DATE, days);
					  Date benchmarkStartDate = cal.getTime();
					  Date benchmarkEndDate = date1;
					
					  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT SUM(engagementTime) as eT,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setEngagementTime(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  public List<PublisherReport> EngagementTimeChannelArticle(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT SUM(engagementTime) as eT FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setEngagementTime(data[0]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
				  
				  public List<PublisherReport> benchmarkEngagementTimeChannelArticle(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
							  String time = startdate;
							  DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							  Date date1 = df.parse(time);
							  
							  String time1 = enddate;
							  Date date2 = df.parse(time1);
							         
							  
							  
							  int days = getDifferenceDays(date1, date2);
							  Calendar cal = Calendar.getInstance();
							  cal.add(Calendar.DATE, days);
							  Date benchmarkStartDate = cal.getTime();
							  Date benchmarkEndDate = date1; 
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT SUM(engagementTime) as eT FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setEngagementTime(data[0]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
		  
				  public List<PublisherReport> counttotalvisitorsChannelArticleHourwise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							  String time = startdate;
							  DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							  Date date1 = df.parse(time);
							  
							  String time1 = enddate;
							  Date date2 = df.parse(time1);
							         
							  
							  
							  int days = getDifferenceDays(date1, date2);
							  Calendar cal = Calendar.getInstance();
							  cal.add(Calendar.DATE, days);
							  Date benchmarkStartDate = cal.getTime();
							  Date benchmarkEndDate = date1;
							
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setTotalvisits(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
		  
				  
				  public List<PublisherReport> countbenchmarktotalvisitorsChannelArticleHourwise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
							  
							  
							  
							  
							  
							  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate+ "'" + " and " + "'" + benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setTotalvisits(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
		  
		  
				  public List<PublisherReport> EngagementTimeChannelArticleHourwise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT SUM(engagementTime) as eT,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setEngagementTime(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
		  
				  

				  public List<PublisherReport> benchmarkEngagementTimeChannelArticleHourwise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							  
							  String time = startdate;
							  DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							  Date date1 = df.parse(time);
							  
							  String time1 = enddate;
							  Date date2 = df.parse(time1);
							         
							  
							  
							  int days = getDifferenceDays(date1, date2);
							  Calendar cal = Calendar.getInstance();
							  cal.add(Calendar.DATE, days);
							  Date benchmarkStartDate = cal.getTime();
							  Date benchmarkEndDate = date1;
							
							  
							  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT SUM(engagementTime) as eT,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate  + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setEngagementTime(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
		  
		  
		  
		  
				  
		  public List<PublisherReport> countAudiencesegmentChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		      List<PublisherReport> pubreport = new ArrayList(); 
			  
			  String querya1 = "SELECT COUNT(DISTINCT(cookie_id)) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";   
			  
			    //Divide count in different limits 
			
			  
			  List<String> Query = new ArrayList();
			  


			    System.out.println(querya1);
			    
			    final long startTime2 = System.currentTimeMillis();
				
			    
			    CSVResult csvResult1 = null;
				try {
					csvResult1 = AggregationModule2.getCsvResult(false, querya1);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    
			    final long endTime2 = System.currentTimeMillis();
				
			    List<String> headers = csvResult1.getHeaders();
			    List<String> lines = csvResult1.getLines();
			    
			    
			    String count = lines.get(0);
			    Double countv1 = Double.parseDouble(count);
			    Double n = 0.0;
			    if(countv1 >= 250000)
			       n=10.0;
			    
			    if(countv1 >= 100000 && countv1 <= 250000 )
			       n=10.0;
			    
			    if(countv1 <= 100000 && countv1 > 100)
		           n=10.0;	    
			   
			    if(countv1 <= 100)
			    	n=1.0;
			    
			    if(countv1 == 0)
			    {
			    	
			    	return pubreport;
			    	
			    }
			    
			    Double total_length = countv1 - 0;
			    Double subrange_length = total_length/n;	
			    
			    Double current_start = 0.0;
			    for (int i = 0; i < n; ++i) {
			      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
			      Double startlimit = current_start;
			      Double finallimit = current_start + subrange_length;
			      Double index = startlimit +1;
			      if(countv1 == 1)
			    	  index=0.0;
			      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
				  System.out.println(query);
			  //    Query.add(query);
			      current_start += subrange_length;
			      Query.add(query);
			      
			    }
			    
			    
			    	
			    
			  
			  ExecutorService executorService = Executors.newFixedThreadPool(2000);
		        
		       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
		    
		       for(int i=0 ; i < Query.size(); i++ ){
		       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
		    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
		       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
		       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
		       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
		        
		       // returns a list of Futures holding their status and results when all complete
		       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
		   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
		       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
		       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
		       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
		       }
		       
		       
		       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
		        
		       System.out.println(maps.size() +" Responses recieved.\n");
		        
		       for(Future<FastMap<String,Double>> task : maps)
		       {
		    	   try{
		           if(task!=null)
		    	   System.out.println(task.get().toString());
		    	   }
		    	   catch(Exception e)
		    	   {
		    		   e.printStackTrace();
		    		   continue;
		    	   }
		    	    
		    	   
		    	   }
		        
		       /* shutdown your thread pool, else your application will keep running */
		       executorService.shutdown();
			  
			
			  //  //System.out.println(headers1);
			 //   //System.out.println(lines1);
			    
			    
		       
		       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
		       
		       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
		       
		       Double count1 = 0.0;
		       
		       Double count2 = 0.0;
		       
		       String key ="";
		       String key1 = "";
		       Double value = 0.0;
		       Double vlaue1 = 0.0;
		       
			    for (int i = 0; i < maps.size(); i++)
			    {
			    
			    	if(maps!=null && maps.get(i)!=null){
			        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
			    	
			       if(map.size() > 0){
			       
			       if(map.containsKey("audience_segment")==true){
			       for (Map.Entry<String, Double> entry : map.entrySet())
			    	 {
			    	  key = entry.getKey();
			    	  key = key.trim();
			    	  value=  entry.getValue();
			    	if(key.equals("audience_segment")==false) { 
			    	if(audiencemap.containsKey(key)==false)
			    	audiencemap.put(key,value);
			    	else
			    	{
			         count1 = audiencemap.get(key);
			         if(count1!=null)
			         audiencemap.put(key,count1+value);	
			    	}
			      }
			    }
			  }   

			       if(map.containsKey("subcategory")==true){
			       for (Map.Entry<String, Double> entry : map.entrySet())
			    	 {
			    	   key = entry.getKey();
			    	   key = key.trim();
			    	   value=  entry.getValue();
			    	if(key.equals("subcategory")==false) {    
			    	if(subcatmap.containsKey(key)==false)
			    	subcatmap.put(key,value);
			    	else
			    	{
			         count1 = subcatmap.get(key);
			         if(count1!=null)
			         subcatmap.put(key,count1+value);	
			    	}
			    }  
			    	
			   }
			      
			     	       }
			           
			       } 
			    
			    	} 	
			   }    
			    
			    String subcategory = null;
			   
			    if(audiencemap.size()>0){
			   
			    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
			    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
			    

			        PublisherReport obj = new PublisherReport();
			        
			   //     String[] data = ((String)lines.get(i)).split(",");
			        
			     //   if(data[0].trim().toLowerCase().contains("festivals"))
			      //  obj.setAudience_segment("");
			      //  else
			        obj.setAudience_segment( entry.getKey());	
			        obj.setCount(String.valueOf(entry.getValue()));
			      
			        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
			        {
			         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
			        	 
			        	    
			        	 
			        	 PublisherReport obj1 = new PublisherReport();
			            
			           
			            if (entry1.getKey().contains(entry.getKey()))
			            {
			              String substring = "_" + entry.getKey() + "_";
			              subcategory = entry1.getKey().replace(substring, "");
			           //   if(data[0].trim().toLowerCase().contains("festivals"))
			           //   obj1.setAudience_segment("");
			           //   else
			        
			              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
			              obj1.setAudience_segment(subcategory);
			              obj1.setCount(String.valueOf(entry1.getValue()));
			              obj.getAudience_segment_data().add(obj1);
			            }
			          }
			          pubreport.add(obj);
			        }
			      
			    }
			    }
			    return pubreport;
		  }
		  
		  public List<PublisherReport> gettimeofdayChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setTime_of_day(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		     
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countPinCodeChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        String[] data1 = data[0].split("_");
		        String locationproperties  = citycodeMap.get(data1[0]);
		        obj.setPostalcode(data[0]);
		        obj.setCount(data[1]);
		        obj.setLocationcode(locationproperties);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countLatLongChannelArticle(String startdate, String enddate, String channel_name, String articlename, String filter)
		    throws CsvExtractorException, Exception
		  {
		    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        String[] data1 = data[0].split("_");
		        String locationproperties  = citycodeMap.get(data1[0]);
		        String[] dashcount = data[0].split("_");
		        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_'))
		        {
		          if (!dashcount[2].isEmpty())
		          {
		            obj.setLatitude_longitude(data[0]);
		            
		            if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
		            obj.setCount(data[1]);
		               
		            if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
		            obj.setEngagementTime(data[1]);
		        
                     if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
		             obj.setVisitorCount(data[1]);
		            
                     obj.setLocationcode(locationproperties);
		            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		            String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		          }
		          pubreport.add(obj);
		        }
		      }
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> gettimeofdayQuarterChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='4h')";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setTime_of_day(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		     
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> gettimeofdayDailyChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1d')";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setTime_of_day(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      System.out.println(headers);
		      System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getdayQuarterdataChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    System.out.println(headers);
		      System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        if (data[0].equals("quarter1")) {
		          data[0] = "quarter1 (00 - 04 AM)";
		        }
		        if (data[0].equals("quarter2")) {
		          data[0] = "quarter2 (04 - 08 AM)";
		        }
		        if (data[0].equals("quarter3")) {
		          data[0] = "quarter3 (08 - 12 AM)";
		        }
		        if (data[0].equals("quarter4")) {
		          data[0] = "quarter4 (12 - 16 PM)";
		        }
		        if (data[0].equals("quarter5")) {
		          data[0] = "quarter5 (16 - 20 PM)";
		        }
		        if (data[0].equals("quarter6")) {
		          data[0] = "quarter6 (20 - 24 PM)";
		        }
		        obj.setTime_of_day(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      System.out.println(headers);
		      System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getGenderChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),gender from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY gender";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		    System.out.println(headers);
		    System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setGender(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      System.out.println(headers);
		      System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getAgegroupChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),agegroup from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY agegroup";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setAge(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		    
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getISPChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),ISP from enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY ISP";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
		        obj.setISP(data[0]);
		        obj.setCount(data[1]);
		        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		        String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		        pubreport.add(obj);
		         }
		        }
		     // System.out.println(headers);
		     // System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getOrgChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
		    CSVResult csvResult1 = getCsvResult(false, query1);
		    List<String> headers1 = csvResult1.getHeaders();
		    List<String> lines1 = csvResult1.getLines();
		    System.out.println(headers1);
		      System.out.println(lines1);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines1.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data1 = ((String)lines1.get(i)).split(",");
		        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
		        {
		          obj.setOrganisation(data1[0]);
		          obj.setCount(data1[1]);
		          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
		          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
		          pubreport.add(obj);
		        }
		      }
		    //  System.out.println(headers1);
		    //  System.out.println(lines1);
		    }
		    return pubreport;
		  }
		  
		  
		  public List<PublisherReport> getChannelSectionArticleList(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				      
				        
				          String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
				          obj.setCount(data1[1]);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          obj.setSection(sectionname);
				          pubreport.add(obj);
				        
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  
		  
		  public List<PublisherReport> getChannelArticleReferrerList(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        if ((data1[0].trim().toLowerCase().contains("facebook") || (data1[0].trim().toLowerCase().contains("google"))))
				        {
				          //if(data1[0].equals()) 
				         
				          obj.setReferrerSource(data1[0]);
				          obj.setCount(data1[1]);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				          pubreport.add(obj);
				        }
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  
		  public List<PublisherReport> getChannelArticleReferrerList1(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				//    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				 //   {
				    
				    String data0="";
				    String data1="";
				    
				    for (int i = 0; i < 5; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				      //  String[] data1 = ((String)lines1.get(i)).split(",");
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="http://m.facebook.com";
				          data1 = "15.0";
				          }
				          

				          if(i == 1){
				          data0="http://www.facebook.com";
				          data1 = "5.0";
				          }
				          
				          
				          if(i == 2){
					          data0="http://l.facebook.com";
					          data1 = "3.0";
					          }
					    
				          
				          if(i == 3){
					          data0="http://www.google.co.pk";
					          data1 = "3.0";
					          }
					          
				          if(i==4){
				        	  data0="http://www.google.co.in";
				              data1 = "2.0";
				          }
				              
				           obj.setReferrerSource(data0);
				          obj.setCount(data1);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				   }
				    return pubreport;
		  
		  
		  
		  
		  
				      }  
		  
		  
		 
		  public List<PublisherReport> getDeviceTypeChannelArticle(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				//    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				 //   {
				    String data0="";
				    String data1="";
				    
				    for (int i = 0; i < 3; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="Mobile";
				          data1 = "18.0";
				          }
				          

				          if(i == 1){
				          data0="Tablet";
				          data1 = "5.0";
				          }
				          
				          
				          if(i == 2){
					          data0="Desktop";
					          data1 = "5.0";
					      }
					    
				        
				          obj.setDevice_type(data0);
				          obj.setCount(data1);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				   }
				    return pubreport;
		  
		  
		  
		  
		  
				      }  
		  
		  
		
		  public List<PublisherReport> getIncomeChannelArticle(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				//    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				 //   {
				   
				    String data0="";
				    String data1="";
				    
				    for (int i = 0; i < 3; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				       // String[] data1 = ((String)lines1.get(i)).split(",");
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="Medium";
				          data1 = "15.0";
				          }
				          

				          if(i == 1){
				          data0="High";
				          data1 = "6.0";
				          }
				          
				          
				          if(i == 2){
					          data0="Low";
					          data1 = "7.0";
					      }
					    
				        
				          obj.setIncomelevel(data0);
				          obj.setCount(data1);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				   }
				    return pubreport;
		  
		  
		  
		  
		  
				      }  
		  
		  
		  public List<PublisherReport> getArticleMetaData(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				//    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				 //   {
				        PublisherReport obj = new PublisherReport();
				     
				     
					    
				        
				          obj.setArticleAuthor("admin");
				          obj.setArticleTags("filmfare,shahid kapoor,deepika padukone,bollywood");
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				  
				    return pubreport;
		  
		  
		  
		  
		  
				      }  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  public List<PublisherReport> getChannelArticleReferredPostsList(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),clickurloriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY clickurloriginal";
				    
				    
				    Site site = GetMiddlewareData.getSiteDetails(channel_name);
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				          String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
				          obj.setCount(data1[1]);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          String articleparts1[] = articlename.split("/"); String articleTitle1 = articleparts1[articleparts1.length-1]; obj.setArticleTitle(articleTitle1);obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  public List<PublisherReport> getChannelArticleReferredPostsListInternal(String startdate, String enddate, String channel_name, String articlename, String filter, String typefilter)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),clickurloriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY clickurloriginal";
				    
				    
				    Site site = GetMiddlewareData.getSiteDetails(channel_name);
				    String siteurl = site.getSiteurl();
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				    	try{  
				    	  
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        
				        if(typefilter == null || typefilter.isEmpty() || typefilter.equals("Internal")){
				        if(data1[0].contains(siteurl)) { 
				        String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
				        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
			            	 obj.setCount(data1[1]);
			               
			                 if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
			                 obj.setEngagementTime(data1[1]);
			        

			                 if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
			                 obj.setVisitorCount(data1[1]);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          String articleparts1[] = articlename.split("/"); String articleTitle1 = articleparts1[articleparts1.length-1];obj.setArticle(articlename);
				          pubreport.add(obj);
				          }
				        }
				        
				        if(typefilter.equals("External")){
					        if(data1[0].contains(siteurl)== false) { 
					        String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
					        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				            	 obj.setCount(data1[1]);
				               
				                 if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
				                 obj.setEngagementTime(data1[1]);
				        

				                 if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
				                 obj.setVisitorCount(data1[1]);
					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					          String articleparts1[] = articlename.split("/"); String articleTitle1 = articleparts1[articleparts1.length-1];obj.setArticle(articlename);
					          pubreport.add(obj);
					          }
					        }
				       
				        if(typefilter.equals("image")){
					        if(data1[0].contains("jpg") || data1[0].contains("png") ||  data1[0].contains("gif")  ) { 
					        String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
					        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				            	 obj.setCount(data1[1]);
				               
				                 if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
				                 obj.setEngagementTime(data1[1]);
				        

				                 if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
				                 obj.setVisitorCount(data1[1]);
					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					          String articleparts1[] = articlename.split("/"); String articleTitle1 = articleparts1[articleparts1.length-1];obj.setArticle(articlename);
					          pubreport.add(obj);
					          }
					        }
				        
				        if(typefilter.equals("video")){
					        if(data1[0].contains("mp4") || data1[0].contains("avi") ||  data1[0].contains("swf") ) { 
					        String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
					        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
				            	 obj.setCount(data1[1]);
				               
				                 if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
				                 obj.setEngagementTime(data1[1]);
				        

				                 if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
				                 obj.setVisitorCount(data1[1]);
					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					          String articleparts1[] = articlename.split("/"); String articleTitle1 = articleparts1[articleparts1.length-1];obj.setArticle(articlename);
					          pubreport.add(obj);
					          }
					        }
				        
				        
				        
				        }
				    	catch(Exception e){
				    		
				    		continue;
				    	}
				        
				        
				        
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  
				  
		  
		  public List<PublisherReport> getChannelSectionArticleCount(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
				        {
				          String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
				          obj.setCount(data1[1]);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          obj.setSection(sectionname);
				          pubreport.add(obj);
				        }
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  public List<PublisherReport> countBrandNameChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    String query = "SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName";
				    //System.out.println(query);
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
				        obj.setBrandname(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				        } 
				       }
				  //    //System.out.println(headers);
				  //    //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countBrowserChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where channel_name ='" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setBrowser(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countOSChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os";
				    System.out.println(query);
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setOs(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countModelChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");

				        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
				        obj.setMobile_device_model_name(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				        
				        }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countCityChannelSection(String startdate, String enddate, String channel_name, String sectionname, String filter)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city order by count desc";
				    System.out.println(query);
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    Integer accumulatedCount = 0;
				    
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(i < 10){
				        	String locationproperties = citycodeMap.get(data[0]);
					        data[0]=data[0].replace("_"," ").replace("-"," ");
					        data[0]=capitalizeString(data[0]);
					        obj.setCity(data[0]);
					        System.out.println(data[0]);
					        obj.setLocationcode(locationproperties);
				        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data[1]);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data[1]);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				        }
				        else{
					    	   
				        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
					    	  if(i == (lines.size()-1)){
					    		 obj.setCity("Others"); 
					    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					    		 obj.setSection(sectionname);
					    		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
										obj.setCount(accumulatedCount.toString());
										if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
										obj.setEngagementTime(accumulatedCount.toString());
										if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
										obj.setVisitorCount(accumulatedCount.toString());
					    		 pubreport.add(obj);
					    	  }
					       }
				        
				      
				      
				      
				      }
				    }
				    return pubreport;
				  }
				  
				  
				  public List<PublisherReport> countStateChannelSection(String startdate, String enddate, String channel_name, String sectionname, String filter)
						    throws CsvExtractorException, Exception
						  {
						    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
						    String query = "SELECT COUNT(*)as count,state FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by state order by count desc";
						    System.out.println(query);
						    CSVResult csvResult = getCsvResult(false, query);
						    List<String> headers = csvResult.getHeaders();
						    List<String> lines = csvResult.getLines();
						    System.out.println(headers);
						    System.out.println(lines);
						    Integer accumulatedCount = 0;
						    
						    List<PublisherReport> pubreport = new ArrayList();
						    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
						      for (int i = 0; i < lines.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data = ((String)lines.get(i)).split(",");
						        if(i < 10){
						        	data[0]=data[0].replace("_", " ");
					            	data[0] = capitalizeString(data[0]);
					            	obj.setState(data[0]);
						        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
									obj.setCount(data[1]);
									if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
									obj.setEngagementTime(data[1]);
									if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
									obj.setVisitorCount(data[1]);
						        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						        obj.setSection(sectionname);
						        pubreport.add(obj);
						        }
						        else{
							    	   
						        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
							    	  if(i == (lines.size()-1)){
							    		 obj.setState("Others"); 
							    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							    		 obj.setSection(sectionname);
							    		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
												obj.setCount(accumulatedCount.toString());
												if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
												obj.setEngagementTime(accumulatedCount.toString());
												if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
												obj.setVisitorCount(accumulatedCount.toString());
							    		 pubreport.add(obj);
							    	  }
							       }
						        
						      
						      
						      
						      }
						    }
						    return pubreport;
						  }
				  
				  public List<PublisherReport> countCountryChannelSection(String startdate, String enddate, String channel_name, String sectionname, String filter)
						    throws CsvExtractorException, Exception
						  {
						    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
						    String query = "SELECT COUNT(*)as count,country FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by country order by count desc";
						    System.out.println(query);
						    CSVResult csvResult = getCsvResult(false, query);
						    List<String> headers = csvResult.getHeaders();
						    List<String> lines = csvResult.getLines();
						    System.out.println(headers);
						    System.out.println(lines);
						    Integer accumulatedCount = 0;
						    
						    List<PublisherReport> pubreport = new ArrayList();
						    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
						      for (int i = 0; i < lines.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data = ((String)lines.get(i)).split(",");
						        if(i < 10){
						        	data[0]=data[0].replace("_", " ");
					            	data[0] = capitalizeString(data[0]);
					            	obj.setCountry(data[0]);
						        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
									obj.setCount(data[1]);
									if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
									obj.setEngagementTime(data[1]);
									if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
									obj.setVisitorCount(data[1]);
						        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						        obj.setSection(sectionname);
						        pubreport.add(obj);
						        }
						        else{
							    	   
						        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
							    	  if(i == (lines.size()-1)){
							    		 obj.setCountry("Others"); 
							    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							    		 obj.setSection(sectionname);
							    		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
												obj.setCount(accumulatedCount.toString());
												if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
												obj.setEngagementTime(accumulatedCount.toString());
												if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
												obj.setVisitorCount(accumulatedCount.toString());
							    		 pubreport.add(obj);
							    	  }
							       }
						        
						      
						      
						      
						      }
						    }
						    return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countfingerprintChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					       // obj.setDate(data[0]);
					        obj.setReach(data[0]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        obj.setSection(sectionname);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
				  
				  
				  
				  
				  
				  
				  public List<PublisherReport> countbenchmarkfingerprintChannelSection(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setReach(data[0]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
						  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
			
				  public List<PublisherReport> countfingerprintChannelDatewise(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				
				  
				  public List<PublisherReport> countBenchmarkfingerprintChannelDatewise(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
					  String time = startdate;
                      DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                      Date date1 = df.parse(time);
                     
                      String time1 = enddate;
                      Date date2 = df.parse(time1);
                            
                     
                     
                      int days = getDifferenceDays(date2, date1)-2;
                      Calendar cal = Calendar.getInstance();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, days);
                      Date benchmarkStartDate1 = cal.getTime();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, -1);
                      Date benchmarkEndDate1 = cal.getTime();


                      String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                      String benchmarkEndDate  = df.format(benchmarkEndDate1);	
                      
                      
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  public List<PublisherReport> countfingerprintChannelDateHourwise(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  public List<PublisherReport> countbenchmarkfingerprintChannelDateHourwise(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate  + "'" + " and " + "'" + benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  			  
				  
				  
				  
				  
				  public List<PublisherReport> countfingerprintChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  
				  
				  
				  public List<PublisherReport> countbenchmarkfingerprintChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							 
					  
					  String time = startdate;
                      DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                      Date date1 = df.parse(time);
                     
                      String time1 = enddate;
                      Date date2 = df.parse(time1);
                            
                     
                     
                      int days = getDifferenceDays(date2, date1)-2;
                      Calendar cal = Calendar.getInstance();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, days);
                      Date benchmarkStartDate1 = cal.getTime();
                      cal.setTime(date1);
                      cal.add(Calendar.DAY_OF_YEAR, -1);
                      Date benchmarkEndDate1 = cal.getTime();


                      String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                      String benchmarkEndDate  = df.format(benchmarkEndDate1);	
					  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  public List<PublisherReport> countfingerprintChannelSectionDateHourwise(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> countbenchmarkfingerprintChannelSectionDateHourwise(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" +  benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  public List<PublisherReport> counttotalvisitorsChannelSection(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setTotalvisits(data[0]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
						  
					
				  
				  public List<PublisherReport> countbenchmarktotalvisitorsChannelSection(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setTotalvisits(data[0]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
						  
						  public List<PublisherReport> counttotalvisitorsChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
				  
						  
						  public List<PublisherReport> counbenchmarktotalvisitorsChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
							  
							  String time = startdate;
							  DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							  Date date1 = df.parse(time);
							  
							  String time1 = enddate;
							  Date date2 = df.parse(time1);
							         
							  
							  
							  int days = getDifferenceDays(date1, date2);
							  Calendar cal = Calendar.getInstance();
							  cal.add(Calendar.DATE, days);
							  Date benchmarkStartDate = cal.getTime();
							  Date benchmarkEndDate = date1;
							  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" +benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate  + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  public List<PublisherReport> countbenchmarktotalvisitorsChannelSectionDateHourwise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									  String time = startdate;
		                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		                              Date date1 = df.parse(time);
		                             
		                              String time1 = enddate;
		                              Date date2 = df.parse(time1);
		                                    
		                             
		                             
		                              int days = getDifferenceDays(date2, date1)-2;
		                              Calendar cal = Calendar.getInstance();
		                              cal.setTime(date1);
		                              cal.add(Calendar.DAY_OF_YEAR, days);
		                              Date benchmarkStartDate1 = cal.getTime();
		                              cal.setTime(date1);
		                              cal.add(Calendar.DAY_OF_YEAR, -1);
		                              Date benchmarkEndDate1 = cal.getTime();


		                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
		                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
									  
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" +  benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  
						  
						  
						  
						  
						  
						  
						  
						  public List<PublisherReport> counttotalvisitorsChannelSectionDateHourwise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  public List<PublisherReport> engagementTimeChannelSection(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime) as eT FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									       // obj.setDate(data[0]);
									        obj.setEngagementTime(data[0]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }		  
						  
						  public List<PublisherReport> benchmarkengagementTimeChannelSection(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									  String time = startdate;
									  DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
									  Date date1 = df.parse(time);
									  
									  String time1 = enddate;
									  Date date2 = df.parse(time1);
									         
									  
									  
									  int days = getDifferenceDays(date1, date2);
									  Calendar cal = Calendar.getInstance();
									  cal.add(Calendar.DATE, days);
									  Date benchmarkStartDate = cal.getTime();
									  Date benchmarkEndDate = date1;
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime) as eT FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" +benchmarkEndDate + "'";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									       // obj.setDate(data[0]);
									        obj.setEngagementTime(data[0]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }		  
						  
						  
						  
						  
						  
						
						  public List<PublisherReport> engagementTimeChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime) as eT,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setEngagementTime(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  
						  
						  public List<PublisherReport> benchmarkengagementTimeChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
							  String time = startdate;
							  DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							  Date date1 = df.parse(time);
							  
							  String time1 = enddate;
							  Date date2 = df.parse(time1);
							         
							  
							  
							  int days = getDifferenceDays(date1, date2);
							  Calendar cal = Calendar.getInstance();
							  cal.add(Calendar.DATE, days);
							  Date benchmarkStartDate = cal.getTime();
							  Date benchmarkEndDate = date1;
							  
							  
							  
							  
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime) as eT,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate 
									      + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setEngagementTime(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
						  
						  
						  
						  
						  
						  
						  
						  
						  
				   
						  public List<PublisherReport> engagementTimeChannelSectionDateHourwise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime) as eT,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setEngagementTime(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				   
						  public List<PublisherReport> benchmarkengagementTimeChannelSectionDateHourwise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									
									  String time = startdate;
                                      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                      Date date1 = df.parse(time);
                                     
                                      String time1 = enddate;
                                      Date date2 = df.parse(time1);
                                            
                                     
                                     
                                      int days = getDifferenceDays(date1, date2);
                                      Calendar cal = Calendar.getInstance();
                                      cal.add(Calendar.DATE, days);
                                      Date benchmarkStartDate = cal.getTime();
                                      Date benchmarkEndDate = date1;

										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime) as eT,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + benchmarkStartDate  + "'" + " and " + "'" + benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setEngagementTime(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
						  
						  
						  
						  
						  public List<PublisherReport> counttotalvisitorsChannelDatewise(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									       
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  
						  public List<PublisherReport> countBenchmarktotalvisitorsChannelDatewise(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
							  
							  
							  
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate+ "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									       
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  public List<PublisherReport> engagementTimeChannel(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime)as eT FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									      //  obj.setDate(data[0]);
									        obj.setEngagementTime(data[0]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									       
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  public List<PublisherReport> benchmarkengagementTimeChannel(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
									  String time = startdate;
		                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		                              Date date1 = df.parse(time);
		                             
		                              String time1 = enddate;
		                              Date date2 = df.parse(time1);
		                                    
		                             
		                             
		                              int days = getDifferenceDays(date2, date1)-2;
		                              Calendar cal = Calendar.getInstance();
		                              cal.setTime(date1);
		                              cal.add(Calendar.DAY_OF_YEAR, days);
		                              Date benchmarkStartDate1 = cal.getTime();
		                              cal.setTime(date1);
		                              cal.add(Calendar.DAY_OF_YEAR, -1);
		                              Date benchmarkEndDate1 = cal.getTime();


		                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
		                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime)as eT FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									      //  obj.setDate(data[0]);
									        obj.setEngagementTime(data[0]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									       
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
						  
						  
						  
						  
						  
						  
						  
						  public List<PublisherReport> engagementTimeChannelDatewise(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime)as eT,date FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setEngagementTime(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									       
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  
						  
						  public List<PublisherReport> benchmarkengagementTimeChannelDatewise(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
							  
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime)as eT,date FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" +  benchmarkStartDate+ "'" + " and " + "'" + benchmarkEndDate + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setEngagementTime(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									       
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  public List<PublisherReport> engagementTimeChannelDateHourwise(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime)as eT,date FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setEngagementTime(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									       
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  
						  
						  public List<PublisherReport> benchmarkengagementTimeChannelDateHourwise(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
									  String time = startdate;
		                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		                              Date date1 = df.parse(time);
		                             
		                              String time1 = enddate;
		                              Date date2 = df.parse(time1);
		                                    
		                             
		                             
		                              int days = getDifferenceDays(date2, date1)-2;
		                              Calendar cal = Calendar.getInstance();
		                              cal.setTime(date1);
		                              cal.add(Calendar.DAY_OF_YEAR, days);
		                              Date benchmarkStartDate1 = cal.getTime();
		                              cal.setTime(date1);
		                              cal.add(Calendar.DAY_OF_YEAR, -1);
		                              Date benchmarkEndDate1 = cal.getTime();

		                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
		                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT SUM(engagementTime)as eT,date FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setEngagementTime(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									       
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						   
						  
						  
						  
						  
						  
						  
						  
						  public List<PublisherReport> counttotalvisitorsChannelSectionDateHourlywise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									//  String query00 = "SELECT date_histogram(field=request_time,interval=1h)cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits,gender FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY gender, date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
				  
				  
						  public List<PublisherReport> counttotalvisitorsChannelSectionDateHourlyMinutewise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									//  String query00 = "SELECT date_histogram(field=request_time,interval=1h)cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
						  public List<PublisherReport> counttotalvisitorsChannelDateHourlywise(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									//  String query00 = "SELECT date_histogram(field=request_time,interval=1h)cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
				  
						  
						  public List<PublisherReport> countbenchmarktotalvisitorsChannelDateHourlywise(String startdate, String enddate, String channel_name)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									//  String query00 = "SELECT date_histogram(field=request_time,interval=1h)cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();


                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits FROM enhanceduserdatabeta1 where channel_name = '" + 
									      channel_name + "' and date between " + "'" + benchmarkStartDate + "'" + " and " + "'" + benchmarkEndDate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
				  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
						  
				  
						  public List<PublisherReport> counttotalvisitorsChannelDateHourlyMinutewise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									//  String query00 = "SELECT date_histogram(field=request_time,interval=1h)cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
									//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits FROM enhanceduserdatabeta1 where refcurrentoriginal channel_name = '" + 
									      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
				  
				  
				  
				  
				  
				  
						  
				  public List<PublisherReport> countAudiencesegmentChannelSection(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
				      List<PublisherReport> pubreport = new ArrayList(); 
					  
					  String querya1 = "SELECT COUNT(DISTINCT(cookie_id)) FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";   
					  
					    //Divide count in different limits 
					
					  
					  List<String> Query = new ArrayList();
					  


					    System.out.println(querya1);
					    
					    final long startTime2 = System.currentTimeMillis();
						
					    
					    CSVResult csvResult1 = null;
						try {
							csvResult1 = AggregationModule2.getCsvResult(false, querya1);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					    
					    final long endTime2 = System.currentTimeMillis();
						
					    List<String> headers = csvResult1.getHeaders();
					    List<String> lines = csvResult1.getLines();
					    
					    
					    String count = lines.get(0);
					    Double countv1 = Double.parseDouble(count);
					    Double n = 0.0;
					    if(countv1 >= 250000)
					       n=10.0;
					    
					    if(countv1 >= 100000 && countv1 <= 250000 )
					       n=10.0;
					    
					    if(countv1 <= 100000 && countv1 > 100)
				           n=10.0;	    
					   
					    if(countv1 <= 100)
					    	n=1.0;
					    
					    if(countv1 == 0)
					    {
					    	
					    	return pubreport;
					    	
					    }
					    
					    Double total_length = countv1 - 0;
					    Double subrange_length = total_length/n;	
					    
					    Double current_start = 0.0;
					    for (int i = 0; i < n; ++i) {
					      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
					      Double startlimit = current_start;
					      Double finallimit = current_start + subrange_length;
					      Double index = startlimit +1;
					      if(countv1 == 1)
					    	  index=0.0;
					      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdatabeta1 where refcurrentoriginal= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
						  System.out.println(query);
					  //    Query.add(query);
					      current_start += subrange_length;
					      Query.add(query);
					      
					    }
					    
					    
					    	
					    
					  
					  ExecutorService executorService = Executors.newFixedThreadPool(2000);
				        
				       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
				    
				       for(int i=0 ; i < Query.size(); i++ ){
				       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
				    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
				        
				       // returns a list of Futures holding their status and results when all complete
				       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
				   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
				       }
				       
				       
				       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
				        
				       System.out.println(maps.size() +" Responses recieved.\n");
				        
				       for(Future<FastMap<String,Double>> task : maps)
				       {
				    	   try{
				           if(task!=null)
				    	   System.out.println(task.get().toString());
				    	   }
				    	   catch(Exception e)
				    	   {
				    		   e.printStackTrace();
				    		   continue;
				    	   }
				    	    
				    	   
				    	   }
				        
				       /* shutdown your thread pool, else your application will keep running */
				       executorService.shutdown();
					  
					
					  //  //System.out.println(headers1);
					 //   //System.out.println(lines1);
					    
					    
				       
				       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
				       
				       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
				       
				       Double count1 = 0.0;
				       
				       Double count2 = 0.0;
				       
				       String key ="";
				       String key1 = "";
				       Double value = 0.0;
				       Double vlaue1 = 0.0;
				       
					    for (int i = 0; i < maps.size(); i++)
					    {
					    
					    	if(maps!=null && maps.get(i)!=null){
					        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
					    	
					       if(map.size() > 0){
					       
					       if(map.containsKey("audience_segment")==true){
					       for (Map.Entry<String, Double> entry : map.entrySet())
					    	 {
					    	  key = entry.getKey();
					    	  key = key.trim();
					    	  value=  entry.getValue();
					    	if(key.equals("audience_segment")==false) { 
					    	if(audiencemap.containsKey(key)==false)
					    	audiencemap.put(key,value);
					    	else
					    	{
					         count1 = audiencemap.get(key);
					         if(count1!=null)
					         audiencemap.put(key,count1+value);	
					    	}
					      }
					    }
					  }   

					       if(map.containsKey("subcategory")==true){
					       for (Map.Entry<String, Double> entry : map.entrySet())
					    	 {
					    	   key = entry.getKey();
					    	   key = key.trim();
					    	   value=  entry.getValue();
					    	if(key.equals("subcategory")==false) {    
					    	if(subcatmap.containsKey(key)==false)
					    	subcatmap.put(key,value);
					    	else
					    	{
					         count1 = subcatmap.get(key);
					         if(count1!=null)
					         subcatmap.put(key,count1+value);	
					    	}
					    }  
					    	
					   }
					      
					     	       }
					           
					       } 
					    
					    	} 	
					   }    
					    
					    String subcategory = null;
					   
					    if(audiencemap.size()>0){
					   
					    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
					    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
					    

					        PublisherReport obj = new PublisherReport();
					        
					   //     String[] data = ((String)lines.get(i)).split(",");
					        
					     //   if(data[0].trim().toLowerCase().contains("festivals"))
					      //  obj.setAudience_segment("");
					      //  else
					        obj.setAudience_segment( entry.getKey());	
					        obj.setCount(String.valueOf(entry.getValue()));
					      
					        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
					        {
					         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
					        	 
					        	    
					        	 
					        	 PublisherReport obj1 = new PublisherReport();
					            
					           
					            if (entry1.getKey().contains(entry.getKey()))
					            {
					              String substring = "_" + entry.getKey() + "_";
					              subcategory = entry1.getKey().replace(substring, "");
					           //   if(data[0].trim().toLowerCase().contains("festivals"))
					           //   obj1.setAudience_segment("");
					           //   else
					        
					              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
					              obj1.setAudience_segment(subcategory);
					              obj1.setCount(String.valueOf(entry1.getValue()));
					              obj.getAudience_segment_data().add(obj1);
					            }
					          }
					          pubreport.add(obj);
					        }
					      
					    }
					    }
					    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				     
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countPinCodeChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				    	  PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        String[] data1 = data[0].split("_");
					        String locationproperties  = citycodeMap.get(data1[0]);
					        obj.setPostalcode(data[0]);
					        obj.setCount(data[1]);
					        obj.setLocationcode(locationproperties);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            obj.setSection(sectionname);
				            pubreport.add(obj);
				      
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countLatLongChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        String[] dashcount = data[0].split("_");
				        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_'))
				        {
				          if (!dashcount[2].isEmpty())
				          {
				            obj.setLatitude_longitude(data[0]);
				            obj.setCount(data[1]);
				            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            obj.setSection(sectionname);
				          }
				          pubreport.add(obj);
				        }
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayQuarterChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='4h')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				     
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayDailyChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1d')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				      System.out.println(headers);
				      System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getdayQuarterdataChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    System.out.println(headers);
				      System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if (data[0].equals("quarter1")) {
				          data[0] = "quarter1 (00 - 04 AM)";
				        }
				        if (data[0].equals("quarter2")) {
				          data[0] = "quarter2 (04 - 08 AM)";
				        }
				        if (data[0].equals("quarter3")) {
				          data[0] = "quarter3 (08 - 12 AM)";
				        }
				        if (data[0].equals("quarter4")) {
				          data[0] = "quarter4 (12 - 16 PM)";
				        }
				        if (data[0].equals("quarter5")) {
				          data[0] = "quarter5 (16 - 20 PM)";
				        }
				        if (data[0].equals("quarter6")) {
				          data[0] = "quarter6 (20 - 24 PM)";
				        }
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				      System.out.println(headers);
				      System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getGenderChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),gender from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY gender";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    System.out.println(headers);
				    System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setGender(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				      System.out.println(headers);
				      System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getAgegroupChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),agegroup from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY agegroup";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setAge(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				    
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getISPChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),ISP from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY ISP";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
				        obj.setISP(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				         }
				        }
				     // System.out.println(headers);
				     // System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getOrgChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
				        {
				          obj.setOrganisation(data1[0]);
				          obj.setCount(data1[1]);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          obj.setSection(sectionname);
				          pubreport.add(obj);
				        }
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
				  
				  
				
				  
				  
				  
				  public List<PublisherReport> getChannelSectionReferrerList(String startdate, String enddate, String channel_name, String sectionname)
						    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
						  {
						    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where refcurrent like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
						    CSVResult csvResult1 = getCsvResult(false, query1);
						    List<String> headers1 = csvResult1.getHeaders();
						    List<String> lines1 = csvResult1.getLines();
						    System.out.println(headers1);
						      System.out.println(lines1);
						    List<PublisherReport> pubreport = new ArrayList();
						    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
						    {
						      for (int i = 0; i < lines1.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data1 = ((String)lines1.get(i)).split(",");
						        if ((data1[0].trim().toLowerCase().contains("facebook") || (data1[0].trim().toLowerCase().contains("google"))))
						        {
						          obj.setReferrerSource(data1[0]);
						          obj.setCount(data1[1]);
						          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						          obj.setSection(sectionname);
						          pubreport.add(obj);
						        }
						      }
						    //  System.out.println(headers1);
						    //  System.out.println(lines1);
						    }
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> getChannelSectionReferredPostsList(String startdate, String enddate, String channel_name, String sectionname)
						    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
						  {
						    String query1 = "Select count(*),clickedurl from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY clickedurl";
						    CSVResult csvResult1 = getCsvResult(false, query1);
						    List<String> headers1 = csvResult1.getHeaders();
						    List<String> lines1 = csvResult1.getLines();
						    System.out.println(headers1);
						      System.out.println(lines1);
						    List<PublisherReport> pubreport = new ArrayList();
						    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
						    {
						      for (int i = 0; i < lines1.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data1 = ((String)lines1.get(i)).split(",");
						          String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
						          obj.setCount(data1[1]);
						          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						          obj.setSection(sectionname);
						          pubreport.add(obj);
						        
						      }
						    //  System.out.println(headers1);
						    //  System.out.println(lines1);
						    }
						    return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countNewUsersChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname, String filter)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
							//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query00);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							//      System.out.println(headers);
							//      System.out.println(lines);
							      Double count = 0.0;
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							       
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        if (Double.parseDouble(data[1].trim()) < 2.0)
							        {
							        count++;
							        
							        }
							        
							       }
							    }  
							
							      PublisherReport obj = new PublisherReport();
							      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
										obj.setCount(count.toString());
										if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
										obj.setEngagementTime(count.toString());
										if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
										obj.setVisitorCount(count.toString());
							      obj.setVisitorType("New Visitors");
							      obj.setSection(sectionname);
							      pubreport.add(obj);
							      System.out.println("Section:"+sectionname+"Count:"+count);
							      
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> countReturningUsersChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname, String filter)
						    throws CsvExtractorException, Exception
						  {
							  
							  
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					   //   System.out.println(headers);
					   //   System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) >= 2.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(count.toString());
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(count.toString());
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(count.toString());
					      obj.setVisitorType("Returning Visitors");
					      obj.setSection(sectionname);
					      pubreport.add(obj);
					      System.out.println("Section:"+sectionname+"Count:"+count);
					      
				          return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countLoyalUsersChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname,String filter)
						    throws CsvExtractorException, Exception
						  {
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					//      System.out.println(headers);
					 //     System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) > 7.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(count.toString());
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(count.toString());
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(count.toString());
					      obj.setVisitorType("Loyal Visitors");
					      obj.setSection(sectionname);
					      pubreport.add(obj);
					      System.out.println("Section:"+sectionname+"Count:"+count);
					      
				          return pubreport;
							  
						
						  }
				  
				  public List<PublisherReport> countNewUsersChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename, String filter)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
							//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query00);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							 //     System.out.println(headers);
							 //     System.out.println(lines);
							      Double count = 0.0;
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							       
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        if (Double.parseDouble(data[1].trim()) < 2.0)
							        {
							        count++;
							        
							        }
							        
							       }
							    }  
							
							      PublisherReport obj = new PublisherReport();
							      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
										obj.setCount(count.toString());
										if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
										obj.setEngagementTime(count.toString());
										if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
										obj.setVisitorCount(count.toString());
							      obj.setVisitorType("New Visitors");
							      String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
							      pubreport.add(obj);
							      System.out.println("Article:"+articlename+"Count:"+count);
							      
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> countReturningUsersChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename, String filter)
						    throws CsvExtractorException, Exception
						  {
							  
							  
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					//      System.out.println(headers);
					//      System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) >= 2.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(count.toString());
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(count.toString());
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(count.toString());
					      obj.setVisitorType("Returning Visitors");
					      String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					      pubreport.add(obj);
					      System.out.println("Article:"+articlename+"Count:"+count);
					      
				          return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countLoyalUsersChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename,String filter)
						    throws CsvExtractorException, Exception
						  {
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					  //    System.out.println(headers);
					  //    System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) > 7.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(count.toString());
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(count.toString());
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(count.toString());
					      obj.setVisitorType("Loyal Visitors");
					      String articleparts[] = articlename.split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle);obj.setArticle(articlename);
					      pubreport.add(obj);
					      System.out.println("Article:"+articlename+"Count:"+count);
					      
				          return pubreport;
						  }			  
						
				  
				  
				  
				  public List<PublisherReport> countNewUsersChannelDatewise(String startdate, String enddate, String channel_name, String filter)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
							//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query00);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							//      System.out.println(headers);
							 //     System.out.println(lines);
							      Double count = 0.0;
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							       
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        if (Double.parseDouble(data[1].trim()) < 2.0)
							        {
							        count++;
							        
							        }
							        
							       }
							    }  
							
							      PublisherReport obj = new PublisherReport();
							      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
										obj.setCount(count.toString());
										if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
										obj.setEngagementTime(count.toString());
										if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
										obj.setVisitorCount(count.toString());
							      obj.setVisitorType("New Visitors");
							      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							      pubreport.add(obj);
							   
							      
						    return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countNewUsersChannelDatewisegroupby(String startdate, String enddate, String channel_name, String groupby)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT COUNT(*)as count, cookie_id,"+groupby+" FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +" group by cookie_id,"+groupby+" limit 20000000";
							  
							
							  
							  if(groupby.equals("hour")){
						    		query00 =  "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id,date_histogram(field='request_time','interval'='1h') limit 20000000";
						    	}

			                
							  if(groupby.equals("minute")){
				                	query00 =  "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id,date_histogram(field='request_time','interval'='1m') limit 20000000";	
						    	}

						       
							  
							  
							  //	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
							//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
							    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query00);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							//      System.out.println(headers);
							 //     System.out.println(lines);
							      Double count = 0.0;
							      Map<String,Double> dates =new HashMap<String,Double>();
							      String date = "";
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							       
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        if(data.length>2){
							        date = data[1].trim();
							        if(dates.containsKey(date)==false)
							        dates.put(date,0.0);
							        if (Double.parseDouble(data[2].trim()) < 2.0)
							        {
							        count = dates.get(date);
							        dates.put(date,count+1);
							        
							        }
							        
							       }
							      }
							      
							    }  
							
							      for(Map.Entry<String,Double>entry: dates.entrySet()){
							      PublisherReport obj = new PublisherReport();
							      
							      if(groupby.equals("audience_segment"))
							             obj.setAudience_segment(entry.getKey());
						            	
						            	
						            	if(groupby.equals("gender"))
								             obj.setGender(entry.getKey());
						            	
						            	if(groupby.equals("hour"))
								             obj.setDate(entry.getKey());
						            	
						            	if(groupby.equals("minute"))
								             obj.setDate(entry.getKey());
						            	
						            	
						            	if(groupby.equals("gender"))
								             obj.setGender(entry.getKey());
						            	
						            	
						            	if(groupby.equals("refcurrentoriginal"))
								             obj.setGender(entry.getKey());
							            	
						            	if(groupby.equals("date"))
								             obj.setDate(entry.getKey());
							            		            	
						            	if(groupby.equals("subcategory"))
								             obj.setSubcategory(entry.getKey());
						            	
						            	if(groupby.equals("agegroup"))
								             obj.setAge(entry.getKey());
							            	
						            	if(groupby.equals("incomelevel"))
								          obj.setIncomelevel(entry.getKey());
							     
						            	if(groupby.equals("city"))
									    {
						            		String locationproperties = citycodeMap.get(entry.getKey());
						    		        String city =entry.getKey().replace("_"," ").replace("-"," ");
						    		        obj.setCity(city);
						    		        System.out.println(city);
						    		        obj.setLocationcode(locationproperties);
									    }
						            	
						            	obj.setVisitorType("New Visitors");
						            	obj.setCount(entry.getValue().toString());
							      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							      pubreport.add(obj);
							      }
							      
						    return pubreport;
						  }
				  
				  
				  
				  
				  
				  
				  
				  public List<PublisherReport> countReturningUsersChannelDatewisegroupby(String startdate, String enddate, String channel_name,String groupby)
						    throws CsvExtractorException, Exception
						  {
							  
							  
					  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
					    
					  String query00 = "SELECT COUNT(*)as count, cookie_id,"+groupby+" FROM enhanceduserdatabeta1 where channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id,"+groupby+" limit 20000000";
					  
					
					  
					  if(groupby.equals("hour")){
				    		query00 =  "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id,date_histogram(field='request_time','interval'='1h') limit 20000000";
				    	}

	                
					  if(groupby.equals("minute")){
		                	query00 =  "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id,date_histogram(field='request_time','interval'='1m') limit 20000000";	
				    	}

				       
					  
					  
					  //	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					//      System.out.println(headers);
					 //     System.out.println(lines);
					      Double count = 0.0;
					      Map<String,Double> dates =new HashMap<String,Double>();
					      String date = "";
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if(data.length > 2){
					        date = data[1].trim();
					        if(dates.containsKey(date)==false)
					        dates.put(date,0.0);
					        if (Double.parseDouble(data[2].trim()) >= 2.0)
					        {
					        count = dates.get(date);
					        dates.put(date,count+1);
					        
					        }
					       
					        } 
					       }
					    }  
					
					      for(Map.Entry<String,Double>entry: dates.entrySet()){
					      PublisherReport obj = new PublisherReport();
					      if(groupby.equals("audience_segment"))
					             obj.setAudience_segment(entry.getKey());
				            	
				            	
				            	if(groupby.equals("gender"))
						             obj.setGender(entry.getKey());
				            	
				            	if(groupby.equals("hour"))
						             obj.setDate(entry.getKey());
				            	
				            	if(groupby.equals("minute"))
						             obj.setDate(entry.getKey());
				            	
				            	
				            	if(groupby.equals("gender"))
						             obj.setGender(entry.getKey());
				            	
				            	
				            	if(groupby.equals("refcurrentoriginal"))
						             obj.setGender(entry.getKey());
					            	
				            	if(groupby.equals("date"))
						             obj.setDate(entry.getKey());
					            		            	
				            	if(groupby.equals("subcategory"))
						             obj.setSubcategory(entry.getKey());
				            	
				            	if(groupby.equals("agegroup"))
						             obj.setAge(entry.getKey());
					            	
				            	if(groupby.equals("incomelevel"))
						          obj.setIncomelevel(entry.getKey());
					     
				            	if(groupby.equals("city")){
				            	String locationproperties = citycodeMap.get(entry.getKey());
			    		        String city =entry.getKey().replace("_"," ").replace("-"," ");
			    		        obj.setCity(city);
			    		        System.out.println(city);
			    		        obj.setLocationcode(locationproperties);
				            	}
				          
				          obj.setVisitorType("Returning Visitors");
				          obj.setCount(entry.getValue().toString());
					      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					      pubreport.add(obj);
					      }
					      
				    return pubreport;
						  }
				  
				  
				  
				 
				  
				  
				  public List<PublisherReport> countReturningUsersChannelDatewise(String startdate, String enddate, String channel_name, String filter)
						    throws CsvExtractorException, Exception
						  {
							  
							  
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					   //   System.out.println(headers);
					   //   System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) >= 2.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(count.toString());
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(count.toString());
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(count.toString());
					      obj.setVisitorType("Returning Visitors");
					      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					      pubreport.add(obj);
					      
				          return pubreport;
						  }
				  
				  
				  
				  
				  
				  public List<PublisherReport> countLoyalUsersChannelDatewise(String startdate, String enddate, String channel_name, String filter)
						    throws CsvExtractorException, Exception
						  {
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					 //     System.out.println(headers);
					 //     System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) > 7.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
								obj.setCount(count.toString());
								if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
								obj.setEngagementTime(count.toString());
								if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
								obj.setVisitorCount(count.toString());
					      obj.setVisitorType("Loyal Visitors");
					      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					      pubreport.add(obj);
					  
					      
				          return pubreport;
						  }			 
				 
				
				  
				  
				  public List<PublisherReport> countLoyalUsersChannelDatewisegroupby(String startdate, String enddate, String channel_name,String groupby)
						    throws CsvExtractorException, Exception
						  {
					  String query00 = "SELECT COUNT(*)as count, cookie_id,"+groupby+" FROM enhanceduserdatabeta1 where channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id,"+groupby+" limit 20000000";
					  
					
					  
					  if(groupby.equals("hour")){
				    		query00 =  "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id,date_histogram(field='request_time','interval'='1h') limit 20000000";
				    	}

	                
					  if(groupby.equals("minute")){
		                	query00 =  "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id,date_histogram(field='request_time','interval'='1m') limit 20000000";	
				    	}

				       
					  
					  
					  //	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					//      System.out.println(headers);
					 //     System.out.println(lines);
					      Double count = 0.0;
					      Map<String,Double> dates =new HashMap<String,Double>();
					      String date = "";
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if(data.length > 2){
					        date = data[1].trim();
					        if(dates.containsKey(date)==false)
					        dates.put(date,0.0);
					        if (Double.parseDouble(data[2].trim()) > 7.0)
					        {
					        count = dates.get(date);
					        dates.put(date,count+1);
					        
					         }
					        }  
					       }
					    }  
					
					      for(Map.Entry<String,Double>entry: dates.entrySet()){
					      PublisherReport obj = new PublisherReport();
					      if(groupby.equals("audience_segment"))
					             obj.setAudience_segment(entry.getKey());
				            	
				            	
				            	if(groupby.equals("gender"))
						             obj.setGender(entry.getKey());
				            	
				            	if(groupby.equals("hour"))
						             obj.setDate(entry.getKey());
				            	
				            	if(groupby.equals("minute"))
						             obj.setDate(entry.getKey());
				            	
				            	
				            	if(groupby.equals("gender"))
						             obj.setGender(entry.getKey());
				            	
				            	
				            	if(groupby.equals("refcurrentoriginal"))
						             obj.setGender(entry.getKey());
					            	
				            	if(groupby.equals("date"))
						             obj.setDate(entry.getKey());
					            		            	
				            	if(groupby.equals("subcategory"))
						             obj.setSubcategory(entry.getKey());
				            	
				            	if(groupby.equals("agegroup"))
						             obj.setAge(entry.getKey());
					            	
				            	if(groupby.equals("incomelevel"))
						          obj.setIncomelevel(entry.getKey());
					     
				            	if(groupby.equals("city")){
				            		String locationproperties = citycodeMap.get(entry.getKey());
				    		        String city =entry.getKey().replace("_"," ").replace("-"," ");
				    		        obj.setCity(city);
				    		        System.out.println(city);
				    		        obj.setLocationcode(locationproperties);
				            	}
							    
				            	 obj.setVisitorType("Loyal Visitors");	    
				            	 obj.setVisitorCount(count.toString());
					      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					      pubreport.add(obj);
					      }
					      
				          return pubreport;
						  }			 
				 
				  
				  
				  
				  
				  
				  public List<PublisherReport> counttotalvisitorsChannel(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							   //   System.out.println(headers);
							  //    System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setTotalvisits(data[0]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
  
				  
				  public List<PublisherReport> countbenchmarktotalvisitorsChannel(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
							  String time = startdate;
                              DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                              Date date1 = df.parse(time);
                             
                              String time1 = enddate;
                              Date date2 = df.parse(time1);
                                    
                             
                             
                              int days = getDifferenceDays(date2, date1)-2;
                              Calendar cal = Calendar.getInstance();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, days);
                              Date benchmarkStartDate1 = cal.getTime();
                              cal.setTime(date1);
                              cal.add(Calendar.DAY_OF_YEAR, -1);
                              Date benchmarkEndDate1 = cal.getTime();

                              String benchmarkStartDate = df.format(benchmarkStartDate1);	  
                              String benchmarkEndDate  = df.format(benchmarkEndDate1);	
                              
                              
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" +  benchmarkStartDate  + "'" + " and " + "'" + benchmarkEndDate  + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							   //   System.out.println(headers);
							  //    System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setTotalvisits(data[0]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }

				  
				  
				  
				  
				  
				  
				  
				  
				  
				  public List<PublisherReport> countUniqueVisitorsChannel(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							  //    System.out.println(headers);
							   //   System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setReach(data[0]);
							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							        
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }	  
				  
				  
				  
				  public List<PublisherReport> countBrandNameChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    String query = "SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName";
				    //System.out.println(query);
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
				        obj.setBrandname(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				        } 
				       }
				  //    //System.out.println(headers);
				  //    //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countBrowserChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where channel_name ='" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setBrowser(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countOSChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = String.format("SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os", new Object[] { "enhanceduserdatabeta1" });
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				  //  //System.out.println(headers);
				  //  //System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setOs(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countModelChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = String.format("SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName", new Object[] { "enhanceduserdatabeta1" });
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");

				        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
				        obj.setMobile_device_model_name(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				        
				        }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countCityChannelLive(String startdate, String enddate, String channel_name,String filter)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city order by count desc";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    Integer accumulatedCount = 0;
				    
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(i < 10){
				        	 String locationproperties = citycodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-"," ");
						        obj.setCity(data[0]);
						        System.out.println(data[0]);
						        obj.setLocationcode(locationproperties);
						        System.out.println(locationproperties);
				        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
							obj.setCount(data[1]);
							if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
							obj.setEngagementTime(data[1]);
							if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
							obj.setVisitorCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				        }
				        else{
				        	
				        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
				        	 
					    	  if(i == (lines.size()-1)){
					    		 obj.setCity("Others"); 
					    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					    		
					    		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
										obj.setCount(accumulatedCount.toString() );
										if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
										obj.setEngagementTime(accumulatedCount.toString() );
										if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
										obj.setVisitorCount(accumulatedCount.toString() );
					    		 pubreport.add(obj);
					    	  }
				        	
				        }
				      
				      }
				    }
				    return pubreport;
				  }
				  
				  
				  
				  
				  public List<PublisherReport> countStateChannelLive(String startdate, String enddate, String channel_name,String filter)
						    throws CsvExtractorException, Exception
						  {
						    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
						    String query = "SELECT COUNT(*)as count,state FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by state order by count desc";
						    CSVResult csvResult = getCsvResult(false, query);
						    List<String> headers = csvResult.getHeaders();
						    List<String> lines = csvResult.getLines();
						    List<PublisherReport> pubreport = new ArrayList();
						    Integer accumulatedCount = 0;
						    
						    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
						      for (int i = 0; i < lines.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data = ((String)lines.get(i)).split(",");
						        if(i < 10){
						        	data[0]=data[0].replace("_", " ");
					            	data[0] = capitalizeString(data[0]);
					            	obj.setState(data[0]);
						        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
									obj.setCount(data[1]);
									if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
									obj.setEngagementTime(data[1]);
									if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
									obj.setVisitorCount(data[1]);
						        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						        pubreport.add(obj);
						        }
						        else{
						        	
						        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
						        	 
							    	  if(i == (lines.size()-1)){
							    		 obj.setState("Others"); 
							    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							    		
							    		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
												obj.setCount(accumulatedCount.toString() );
												if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
												obj.setEngagementTime(accumulatedCount.toString() );
												if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
												obj.setVisitorCount(accumulatedCount.toString() );
							    		 pubreport.add(obj);
							    	  }
						        	
						        }
						      
						      }
						    }
						    return pubreport;
						  }  
				  
				  
				  public List<PublisherReport> countCountryChannelLive(String startdate, String enddate, String channel_name,String filter)
						    throws CsvExtractorException, Exception
						  {
						    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
						    String query = "SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city order by count desc";
						    CSVResult csvResult = getCsvResult(false, query);
						    List<String> headers = csvResult.getHeaders();
						    List<String> lines = csvResult.getLines();
						    List<PublisherReport> pubreport = new ArrayList();
						    Integer accumulatedCount = 0;
						    
						    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
						      for (int i = 0; i < lines.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data = ((String)lines.get(i)).split(",");
						        if(i < 10){
						        	data[0]=data[0].replace("_", " ");
					            	data[0] = capitalizeString(data[0]);
					            	obj.setCountry(data[0]);
						        if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
									obj.setCount(data[1]);
									if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
									obj.setEngagementTime(data[1]);
									if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
									obj.setVisitorCount(data[1]);
						        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
						        pubreport.add(obj);
						        }
						        else{
						        	
						        	 accumulatedCount = accumulatedCount + (int)Double.parseDouble(data[1]); 
						        	 
							    	  if(i == (lines.size()-1)){
							    		 obj.setCountry("Others"); 
							    		 String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
							    		
							    		 if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
												obj.setCount(accumulatedCount.toString() );
												if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
												obj.setEngagementTime(accumulatedCount.toString() );
												if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
												obj.setVisitorCount(accumulatedCount.toString() );
							    		 pubreport.add(obj);
							    	  }
						        	
						        }
						      
						      }
						    }
						    return pubreport;
						  }  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  
				  public List<PublisherReport> countfingerprintChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
					  
					  
					  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
						      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
						 CSVResult csvResult00 = getCsvResult(false, query00);
						 List<String> headers00 = csvResult00.getHeaders();
						 List<String> lines00 = csvResult00.getLines();
						 List<PublisherReport> pubreport00 = new ArrayList();  
							  
						//  //System.out.println(headers00);
						//  //System.out.println(lines00);  
						  
						  for (int i = 0; i < lines00.size(); i++)
					      {
					       
					        String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					      }
						  
						  
						  
						//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
					      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					    CSVResult csvResult = getCsvResult(false, query);
					    List<String> headers = csvResult.getHeaders();
					    List<String> lines = csvResult.getLines();
					    List<PublisherReport> pubreport = new ArrayList();
					    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        obj.setDate(data[0]);
					        obj.setReach(data[1]);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
					        pubreport.add(obj);
					      }
					    }
					    
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countAudiencesegmentChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				      List<PublisherReport> pubreport = new ArrayList(); 
					  
					  String querya1 = "SELECT COUNT(DISTINCT(cookie_id)) FROM enhanceduserdata where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";   
					  
					    //Divide count in different limits 
					
					  
					  List<String> Query = new ArrayList();
					  


					    System.out.println(querya1);
					    
					    final long startTime2 = System.currentTimeMillis();
						
					    
					    CSVResult csvResult1 = null;
						try {
							csvResult1 = AggregationModule2.getCsvResult(false, querya1);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					    
					    final long endTime2 = System.currentTimeMillis();
						
					    List<String> headers = csvResult1.getHeaders();
					    List<String> lines = csvResult1.getLines();
					    
					    
					    String count = lines.get(0);
					    Double countv1 = Double.parseDouble(count);
					    Double n = 0.0;
					    if(countv1 >= 250000)
					       n=10.0;
					    
					    if(countv1 >= 100000 && countv1 <= 250000 )
					       n=10.0;
					    
					    if(countv1 <= 100000 && countv1 > 100)
				           n=10.0;	    
					   
					    if(countv1 <= 100)
					    	n=1.0;
					    
					    if(countv1 == 0)
					    {
					    	
					    	return pubreport;
					    	
					    }
					    
					    Double total_length = countv1 - 0;
					    Double subrange_length = total_length/n;	
					    
					    Double current_start = 0.0;
					    for (int i = 0; i < n; ++i) {
					      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
					      Double startlimit = current_start;
					      Double finallimit = current_start + subrange_length;
					      Double index = startlimit +1;
					      if(countv1 == 1)
					    	  index=0.0;
					      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdata where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
						  System.out.println(query);
					  //    Query.add(query);
					      current_start += subrange_length;
					      Query.add(query);
					     
					    }
					    
					    
					    	
					    
					  
					  ExecutorService executorService = Executors.newFixedThreadPool(2000);
				        
				       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
				    
				       for(int i=0 ; i < Query.size(); i++ ){
				       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
				    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
				        
				       // returns a list of Futures holding their status and results when all complete
				       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
				   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
				       }
				       
				       
				       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
				        
				       System.out.println(maps.size() +" Responses recieved.\n");
				        
				       for(Future<FastMap<String,Double>> task : maps)
				       {
				    	   try{
				           if(task!=null)
				    	   System.out.println(task.get().toString());
				    	   }
				    	   catch(Exception e)
				    	   {
				    		   e.printStackTrace();
				    		   continue;
				    	   }
				    	    
				    	   
				    	   }
				        
				       /* shutdown your thread pool, else your application will keep running */
				       executorService.shutdown();
					  
					
					  //  //System.out.println(headers1);
					 //   //System.out.println(lines1);
					    
					    
				       
				       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
				       
				       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
				       
				       Double count1 = 0.0;
				       
				       Double count2 = 0.0;
				       
				       String key ="";
				       String key1 = "";
				       Double value = 0.0;
				       Double vlaue1 = 0.0;
				       
					    for (int i = 0; i < maps.size(); i++)
					    {
					    
					    	if(maps!=null && maps.get(i)!=null){
					        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
					    	
					       if(map.size() > 0){
					       
					       if(map.containsKey("audience_segment")==true){
					       for (Map.Entry<String, Double> entry : map.entrySet())
					    	 {
					    	  key = entry.getKey();
					    	  key = key.trim();
					    	  value=  entry.getValue();
					    	if(key.equals("audience_segment")==false) { 
					    	if(audiencemap.containsKey(key)==false)
					    	audiencemap.put(key,value);
					    	else
					    	{
					         count1 = audiencemap.get(key);
					         if(count1!=null)
					         audiencemap.put(key,count1+value);	
					    	}
					      }
					    }
					  }   

					       if(map.containsKey("subcategory")==true){
					       for (Map.Entry<String, Double> entry : map.entrySet())
					    	 {
					    	   key = entry.getKey();
					    	   key = key.trim();
					    	   value=  entry.getValue();
					    	if(key.equals("subcategory")==false) {    
					    	if(subcatmap.containsKey(key)==false)
					    	subcatmap.put(key,value);
					    	else
					    	{
					         count1 = subcatmap.get(key);
					         if(count1!=null)
					         subcatmap.put(key,count1+value);	
					    	}
					    }  
					    	
					   }
					      
					     	       }
					           
					       } 
					    
					    	} 	
					   }    
					    
					    String subcategory = null;
					   
					    if(audiencemap.size()>0){
					   
					    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
					    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
					    

					        PublisherReport obj = new PublisherReport();
					        
					   //     String[] data = ((String)lines.get(i)).split(",");
					        
					     //   if(data[0].trim().toLowerCase().contains("festivals"))
					      //  obj.setAudience_segment("");
					      //  else
					        obj.setAudience_segment( entry.getKey());	
					        obj.setCount(String.valueOf(entry.getValue()));
					      
					        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
					        {
					         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
					        	 
					        	    
					        	 
					        	 PublisherReport obj1 = new PublisherReport();
					            
					           
					            if (entry1.getKey().contains(entry.getKey()))
					            {
					              String substring = "_" + entry.getKey() + "_";
					              subcategory = entry1.getKey().replace(substring, "");
					           //   if(data[0].trim().toLowerCase().contains("festivals"))
					           //   obj1.setAudience_segment("");
					           //   else
					        
					              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
					              obj1.setAudience_segment(subcategory);
					              obj1.setCount(String.valueOf(entry1.getValue()));
					              obj.getAudience_segment_data().add(obj1);
					            }
					          }
					          pubreport.add(obj);
					        }
					      
					    }
					    }
					    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countPinCodeChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    //System.out.println(headers);
				    //System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				    	  PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        String[] data1 = data[0].split("_");
					        String locationproperties  = citycodeMap.get(data1[0]);
					        obj.setPostalcode(data[0]);
					        obj.setCount(data[1]);
					        obj.setLocationcode(locationproperties);
					        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				            pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countLatLongChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    //Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = String.format("SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude", new Object[] { "enhanceduserdatabeta1" });
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    //System.out.println(headers);
				    //System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        String[] dashcount = data[0].split("_");
				        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_'))
				        {
				          if (!dashcount[2].isEmpty())
				          {
				            obj.setLatitude_longitude(data[0]);
				            obj.setCount(data[1]);
				            String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          }
				          pubreport.add(obj);
				        }
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayQuarterChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='4h')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayDailyChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1d')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getdayQuarterdataChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if (data[0].equals("quarter1")) {
				          data[0] = "quarter1 (00 - 04 AM)";
				        }
				        if (data[0].equals("quarter2")) {
				          data[0] = "quarter2 (04 - 08 AM)";
				        }
				        if (data[0].equals("quarter3")) {
				          data[0] = "quarter3 (08 - 12 AM)";
				        }
				        if (data[0].equals("quarter4")) {
				          data[0] = "quarter4 (12 - 16 PM)";
				        }
				        if (data[0].equals("quarter5")) {
				          data[0] = "quarter5 (16 - 20 PM)";
				        }
				        if (data[0].equals("quarter6")) {
				          data[0] = "quarter6 (20 - 24 PM)";
				        }
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getGenderChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),gender from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY gender";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setGender(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getAgegroupChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),agegroup from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY agegroup";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setAge(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getISPChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),ISP from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY ISP";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
				        obj.setISP(data[0]);
				        obj.setCount(data[1]);
				        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				        pubreport.add(obj);
				         }
				        }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getOrgChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
				        {
				          obj.setOrganisation(data1[0]);
				          obj.setCount(data1[1]);
				          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				          pubreport.add(obj);
				        }
				      }
				      //System.out.println(headers1);
				      //System.out.println(lines1);
				    }
				    return pubreport;
				  }
				  
				    
				  				  public List<PublisherReport> countNewUsersChannelLiveDatewise(String startdate, String enddate, String channel_name, String filter)
				  						    throws CsvExtractorException, Exception
				  						  {
				  							  
				  							  
				  						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
				  							  
				  						    
				  							  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  								      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  							  
				  							//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  								// List<String> headers00 = csvResult00.getHeaders();
				  						//		 List<String> lines00 = csvResult00.getLines();
				  							//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  								
				  								 
				  							//	System.out.println(headers00);
				  							//	System.out.println(lines00);  
				  								  
				  								//  for (int i = 0; i < lines00.size(); i++)
				  							    //  {
				  							       
				  							     //   String[] data = ((String)lines00.get(i)).split(",");
				  							  //      //System.out.println(data[0]);
				  							     
				  								  
				  								  
				  								  
				  							//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
				  							    //  channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
				  							      CSVResult csvResult = getCsvResult(false, query00);
				  							      List<String> headers = csvResult.getHeaders();
				  							      List<String> lines = csvResult.getLines();
				  							      List<PublisherReport> pubreport = new ArrayList();
				  							//      System.out.println(headers);
				  							 //     System.out.println(lines);
				  							      Double count = 0.0;
				  							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  							      for (int i = 0; i < lines.size(); i++)
				  							      {
				  							       
				  							        
				  							        String[] data = ((String)lines.get(i)).split(",");
				  							        if (Double.parseDouble(data[1].trim()) < 2.0)
				  							        {
				  							        count++;
				  							        
				  							        }
				  							        
				  							       }
				  							    }  
				  							
				  							      PublisherReport obj = new PublisherReport();
				  							    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
													obj.setCount(count.toString());
													if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
													obj.setEngagementTime(count.toString());
													if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
													obj.setVisitorCount(count.toString());
				  							      obj.setVisitorType("New Visitors");
				  							      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  							      pubreport.add(obj);
				  							   
				  							      
				  						    return pubreport;
				  						  }
				  				  
				  				  
				  				  public List<PublisherReport> countReturningUsersChannelLiveDatewise(String startdate, String enddate, String channel_name,String filter)
				  						    throws CsvExtractorException, Exception
				  						  {
				  							  
				  							  
				  					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  						      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  					  
				  					//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  						// List<String> headers00 = csvResult00.getHeaders();
				  				//		 List<String> lines00 = csvResult00.getLines();
				  					//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  						
				  						 
				  					//	System.out.println(headers00);
				  					//	System.out.println(lines00);  
				  						  
				  						//  for (int i = 0; i < lines00.size(); i++)
				  					    //  {
				  					       
				  					     //   String[] data = ((String)lines00.get(i)).split(",");
				  					  //      //System.out.println(data[0]);
				  					     
				  						  
				  						  
				  						  
				  					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
				  					    //  channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
				  					      CSVResult csvResult = getCsvResult(false, query00);
				  					      List<String> headers = csvResult.getHeaders();
				  					      List<String> lines = csvResult.getLines();
				  					      List<PublisherReport> pubreport = new ArrayList();
				  					   //   System.out.println(headers);
				  					   //   System.out.println(lines);
				  					      Double count = 0.0;
				  					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  					      for (int i = 0; i < lines.size(); i++)
				  					      {
				  					       
				  					        
				  					        String[] data = ((String)lines.get(i)).split(",");
				  					        if (Double.parseDouble(data[1].trim()) >= 2.0)
				  					        {
				  					        count++;
				  					        
				  					        }
				  					        
				  					       }
				  					    }  
				  					
				  					      PublisherReport obj = new PublisherReport();
				  					    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
											obj.setCount(count.toString());
											if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
											obj.setEngagementTime(count.toString());
											if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
											obj.setVisitorCount(count.toString());
				  					      obj.setVisitorType("Returning Visitors");
				  					      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  					      pubreport.add(obj);
				  					      
				  				          return pubreport;
				  						  }
				  				  
				  				  
				  				  
				  				  public List<PublisherReport> countLoyalUsersChannelLiveDatewise(String startdate, String enddate, String channel_name, String filter)
				  						    throws CsvExtractorException, Exception
				  						  {
				  					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  						      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  					  
				  					//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  						// List<String> headers00 = csvResult00.getHeaders();
				  				//		 List<String> lines00 = csvResult00.getLines();
				  					//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  						
				  						 
				  					//	System.out.println(headers00);
				  					//	System.out.println(lines00);  
				  						  
				  						//  for (int i = 0; i < lines00.size(); i++)
				  					    //  {
				  					       
				  					     //   String[] data = ((String)lines00.get(i)).split(",");
				  					  //      //System.out.println(data[0]);
				  					     
				  						  
				  						  
				  						  
				  					//	//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + 
				  					    //  channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
				  					      CSVResult csvResult = getCsvResult(false, query00);
				  					      List<String> headers = csvResult.getHeaders();
				  					      List<String> lines = csvResult.getLines();
				  					      List<PublisherReport> pubreport = new ArrayList();
				  					 //     System.out.println(headers);
				  					 //     System.out.println(lines);
				  					      Double count = 0.0;
				  					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  					      for (int i = 0; i < lines.size(); i++)
				  					      {
				  					       
				  					        
				  					        String[] data = ((String)lines.get(i)).split(",");
				  					        if (Double.parseDouble(data[1].trim()) > 7.0)
				  					        {
				  					        count++;
				  					        
				  					        }
				  					        
				  					       }
				  					    }  
				  					
				  					      PublisherReport obj = new PublisherReport();
				  					    if(filter == null || filter.isEmpty() ||  filter.equals("pageviews"))
											obj.setCount(count.toString());
											if(filter != null && !filter.isEmpty() && filter.equals("engagementTime") )
											obj.setEngagementTime(count.toString());
											if(filter != null && !filter.isEmpty()  && filter.equals("visitorCount") )
											obj.setVisitorCount(count.toString());
				  					      obj.setVisitorType("Loyal Visitors");
				  					      String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  					      pubreport.add(obj);
				  					  
				  					      
				  				          return pubreport;
				  						  }			 
				  				 
				  				
				  				  
				  				  public List<PublisherReport> counttotalvisitorsChannelLive(String startdate, String enddate, String channel_name)
				  						    throws CsvExtractorException, Exception
				  						  {
				  							  
				  							  
				  						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
				  							  
				  						    
				  							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  								      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  							  
				  							//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  								// List<String> headers00 = csvResult00.getHeaders();
				  						//		 List<String> lines00 = csvResult00.getLines();
				  							//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  								
				  								 
				  							//	System.out.println(headers00);
				  							//	System.out.println(lines00);  
				  								  
				  								//  for (int i = 0; i < lines00.size(); i++)
				  							    //  {
				  							       
				  							     //   String[] data = ((String)lines00.get(i)).split(",");
				  							  //      //System.out.println(data[0]);
				  							     
				  								  
				  								  
				  								  
				  								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  							    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where channel_name = '" + 
				  							      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
				  							      CSVResult csvResult = getCsvResult(false, query);
				  							      List<String> headers = csvResult.getHeaders();
				  							      List<String> lines = csvResult.getLines();
				  							      List<PublisherReport> pubreport = new ArrayList();
				  							   //   System.out.println(headers);
				  							  //    System.out.println(lines);
				  							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  							      for (int i = 0; i < lines.size(); i++)
				  							      {
				  							        PublisherReport obj = new PublisherReport();
				  							        
				  							        String[] data = ((String)lines.get(i)).split(",");
				  							       // obj.setDate(data[0]);
				  							        obj.setTotalvisits(data[0]);
				  							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  							        
				  							        pubreport.add(obj);
				  							      }
				  							    }  
				  							    
				  						    return pubreport;
				  						  }
				    
				  				  
				  				  public List<PublisherReport> countUniqueVisitorsChannelLive(String startdate, String enddate, String channel_name)
				  						    throws CsvExtractorException, Exception
				  						  {
				  							  
				  							  
				  						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
				  							  
				  						    
				  							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  								      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  							  
				  							//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  								// List<String> headers00 = csvResult00.getHeaders();
				  						//		 List<String> lines00 = csvResult00.getLines();
				  							//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  								
				  								 
				  							//	System.out.println(headers00);
				  							//	System.out.println(lines00);  
				  								  
				  								//  for (int i = 0; i < lines00.size(); i++)
				  							    //  {
				  							       
				  							     //   String[] data = ((String)lines00.get(i)).split(",");
				  							  //      //System.out.println(data[0]);
				  							     
				  								  
				  								  
				  								  
				  								//Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  							    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
				  							      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
				  							      CSVResult csvResult = getCsvResult(false, query);
				  							      List<String> headers = csvResult.getHeaders();
				  							      List<String> lines = csvResult.getLines();
				  							      List<PublisherReport> pubreport = new ArrayList();
				  							  //    System.out.println(headers);
				  							   //   System.out.println(lines);
				  							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  							      for (int i = 0; i < lines.size(); i++)
				  							      {
				  							        PublisherReport obj = new PublisherReport();
				  							        
				  							        String[] data = ((String)lines.get(i)).split(",");
				  							       // obj.setDate(data[0]);
				  							        obj.setReach(data[0]);
				  							        String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  							        
				  							        pubreport.add(obj);
				  							      }
				  							    }  
				  							    
				  						    return pubreport;
				  						  }	  
								  	  
				  				public List<PublisherReport> getTopPostsbyTotalPageviewschannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					    String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  					    List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      for (int i = 0; i < lines1.size(); i++)
				  					      {
				  					        PublisherReport obj = new PublisherReport();
				  					        
				  					        String[] data1 = ((String)lines1.get(i)).split(",");
				  					      //    String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
				  					         
				  					        String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]); Article article = GetMiddlewareData.getArticleMetaData(data1[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}
				  					        obj.setCount(data1[1]);
				  					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  					          
				  					          pubreport.add(obj);
				  					        
				  					      }
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }
								  	  			  
				  				
				  				public List<PublisherReport> getTopPostsbyUniqueViewschannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					    String query1 = "Select count(distinct(cookies)),refcurrentoriginal from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  					    List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      for (int i = 0; i < lines1.size(); i++)
				  					      {
				  					        PublisherReport obj = new PublisherReport();
				  					        
				  					        String[] data1 = ((String)lines1.get(i)).split(",");
				  					        //  String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]);
				  					          
				  					          String articleparts[] = data1[0].split("/"); String articleTitle = articleparts[articleparts.length-1];articleTitle = articleTitle.replace("-"," ");articleTitle=capitalizeString(articleTitle);obj.setArticleTitle(articleTitle); obj.setPublisher_pages(data1[0]); Article article = GetMiddlewareData.getArticleMetaData(data1[0]);String articleImage = article.getMainimage();obj.setArticleImage(articleImage);String id = article.getId(); obj.setArticleId(id);String authorName = article.getAuthor();obj.setArticleAuthor(authorName);String authorId = article.getAuthorId();obj.setAuthorId(authorId);List<String> tags1 = article.getTags(); obj.setArticleTag(tags1);if(article.getArticletitle() != null && !article.getArticletitle().isEmpty()){ articleTitle = article.getArticletitle();obj.setArticleTitle(articleTitle);}
				  					          obj.setCount(data1[1]);
				  					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  					          
				  					          pubreport.add(obj);
				  					        
				  					      }
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }
								  	  			  
				  				  				
				
				  				public List<PublisherReport> getRefererPostsChannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					 String query1 = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  					    List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      for (int i = 0; i < lines1.size(); i++)
				  					      {
				  					        PublisherReport obj = new PublisherReport();
				  					        
				  					        String[] data1 = ((String)lines1.get(i)).split(",");
				  					        if ((data1[0].trim().toLowerCase().contains("facebook") || (data1[0].trim().toLowerCase().contains("google"))))
				  					        {
				  					          obj.setReferrerSource(data1[0]);
				  					          obj.setCount(data1[1]);
				  					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  					         
				  					          pubreport.add(obj);
				  					        }
				  					      }
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }
				  				
				  				public List<PublisherReport> getNewContentChannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					    String query1 = "Select refcurrentoriginal from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  				
				  					    String query2 = "Select refcurrentoriginal from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time < " + "'" + startdate + "' GROUP BY refcurrentoriginal";
				  					    CSVResult csvResult2 = getCsvResult(false, query2);
				  					    List<String> headers2 = csvResult2.getHeaders();
				  					    List<String> lines2 = csvResult2.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  				     
				  					     Set<String> list2 = new HashSet<String>();
				  					     list2.addAll(lines2);
				  					      
				  					     for(int i=0;i<lines1.size();i++){
				  					    	 
				  					    	 if(list2.contains(lines1.get(i))){
				  					    		 lines1.remove(i);
				  					    		 
				  					     }
				  					    	 
				  					   }
				  					     
				  					     
				  					      
				  					      List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      for (int i = 0; i < lines1.size(); i++)
				  					      {
				  					        PublisherReport obj = new PublisherReport();
				  					        
				  					        String data1 = (String)lines1.get(i);
				  					      
				  					          obj.setPublisher_pages(data1);
				  					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  					         
				  					          pubreport.add(obj);
				  					       
				  					      }
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }	
				  				
				  				public List<PublisherReport> getNewContentCountChannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					    String query1 = "Select refcurrentoriginal from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY refcurrentoriginal";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  				
				  					    String query2 = "Select refcurrentoriginal from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time < " + "'" + startdate + "' GROUP BY refcurrentoriginal";
				  					    CSVResult csvResult2 = getCsvResult(false, query2);
				  					    List<String> headers2 = csvResult2.getHeaders();
				  					    List<String> lines2 = csvResult2.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  				     
				  					     Set<String> list2 = new HashSet<String>();
				  					     list2.addAll(lines2);
				  					      
				  					     for(int i=0;i<lines1.size();i++){
				  					    	 
				  					    	 if(list2.contains(lines1.get(i))){
				  					    		 lines1.remove(i);
				  					    		 
				  					     }
				  					    	 
				  					   }
				  					     
				  					     
				  					      
				  					      List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      
				  					          PublisherReport obj = new PublisherReport();
				  					        
				  					      
				  					          Integer newContent = lines1.size();
				  					          obj.setCount(newContent.toString());
				  					          String[] channels = channel_name.split("_");String channel_name1 = channels[0];channel_name1 = capitalizeString(channel_name1);obj.setChannelName(channel_name1);
				  					         
				  					          pubreport.add(obj);
				  					       
				  					     
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }	
				  				
				  			  
				  				
				  				
				  				
  
  public static CSVResult getCsvResult(boolean flat, String query)
    throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException
  {
    return getCsvResult(flat, query, false, false);
  }
  
  public static CSVResult getCsvResult(boolean flat, String query, boolean includeScore, boolean includeType)
    throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException
  {
    SearchDao searchDao = getSearchDao();
    QueryAction queryAction = searchDao.explain(query);
    Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
    return new CSVResultsExtractor(includeScore, includeType).extractResults(execution, flat, ",");
  }
  
  public static void sumTest()
    throws IOException, SqlParseException, SQLFeatureNotSupportedException
  {}
  
  private static Aggregations query(String query)
    throws SqlParseException, SQLFeatureNotSupportedException
  {
    SqlElasticSearchRequestBuilder select = getSearchRequestBuilder(query);
    return ((SearchResponse)select.get()).getAggregations();
  }
  
  private static SqlElasticSearchRequestBuilder getSearchRequestBuilder(String query)
    throws SqlParseException, SQLFeatureNotSupportedException
  {
    SearchDao searchDao = getSearchDao();
    return (SqlElasticSearchRequestBuilder)searchDao.explain(query).explain();
  }
  
  private static InetSocketTransportAddress getTransportAddress()
  {
    String host = System.getenv("ES_TEST_HOST");
    String port = System.getenv("ES_TEST_PORT");
    if (host == null)
    {
    host = "172.16.101.132";
    	//System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default 'localhost'");
    }
    if (port == null)
    {
      port = "9300";
      //System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
    }
    //System.out.println(String.format("Connection details: host: %s. port:%s.", new Object[] { host, port }));
    return new InetSocketTransportAddress(host, Integer.parseInt(port));
  }
}
