package com.websystique.springmvc.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
import com.personaCache.EnhanceUserDataDaily;
import com.publisherdata.model.CaffeineCache;
import com.websystique.springmvc.controller.ReportRestController;
import com.websystique.springmvc.model.Pair;
import com.websystique.springmvc.model.UserProfile;

import util.EhcacheKeyCodeRepository;

/**
 * Derive Categories Takes Text as Parameter
 * 
 * 
 * 
 */

public class getCookiePersonaDetails {
	private static final long serialVersionUID = 1L;
	public static Map<String, String> citycodeMap;
	public static Map<String, String> citycodeMap2;
	public static Map<String, String> countrymap;
	public static Map<String, List<String>> countrystatemap;
	public static Map<String, List<String>> countrystatecitymap;
	public static Map<String, String> citylatlongMap1;
    public static Map<String, String> AgeMap;
	public static Map<String, String> AgeMap1;
	public static String line = "";
	
    static {	
		
    	 BufferedReader br1 = null;
         try{

			br1 = new BufferedReader(new FileReader("/mnt/data/realtimeTargetingData.txt"));
            line = br1.readLine();
        } catch (Exception e) {

			e.printStackTrace();
		}
    			
		Map<String, String> ageMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/ageMap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> ageMap1 = new HashMap<String, String>();
		Map<String, String> ageMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] ageDetails = line5.split(cvsSplitBy5);
					key2 = ageDetails[0];
					ageMap1.put(key2, ageDetails[1]);
					ageMap2.put(ageDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		AgeMap = Collections.unmodifiableMap(ageMap1);
		AgeMap1 = Collections.unmodifiableMap(ageMap2);
		// System.out.println(deviceMap);
	}

	 // Set up Elasticsearch here for fetching persona corresponding to User Cookie
	public static TransportClient client;

	public static void setUp() throws Exception {
		if (client == null) {
	        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();

	//		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "personaq").build();

	//		client = new TransportClient(settings)
	//   			.addTransportAddress(new InetSocketTransportAddress("192.168.106.118", 9300))
	//	         	.addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300))
    //    			.addTransportAddress(new InetSocketTransportAddress("192.168.106.120", 9300));

	
			client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
			
			
			NodesInfoResponse nodeInfos = (NodesInfoResponse) client.admin().cluster().prepareNodesInfo(new String[0])
					.get();
			String clusterName = nodeInfos.getClusterName().value();
			// System.out.println(String.format("Found cluster... cluster name: %s", new
			// Object[] { clusterName }));

		}
		// System.out.println("Finished the setup process...");
	}

	// More data points in User Persona can be added here (as fetched from enhanced
	// data points index)
	// Most of the data points as per requirement earlier are already supported
	// Fetches Cookie persona from enhanced index and is called by javascript
	// embedded in client website, check code repo for javascripts provided to
	// client
	public static String getCookieData(HttpServletRequest request) {

	
		String document_id = null;
		String city = null;
		String country = null;
		String json = null;
		HashMap<String, Integer> audienceSegmentCountMap = new HashMap<String, Integer>();
		HashMap<String, Integer> cityCountMap = new HashMap<String, Integer>();
		HashMap<String, Integer> countryCountMap = new HashMap<String, Integer>();
		HashMap<String, Integer> mobileCountMap = new HashMap<String, Integer>();
		HashMap<String, Integer> tagMap = new HashMap<String, Integer>();
		HashMap<String, Integer> sectionMap = new HashMap<String, Integer>();
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
		String sectionvalue = null;
		String[] tagparts = null;
		Cookie[] cookies = request.getCookies();
		String referrer = request.getHeader("referer"); 
		String localstorageId = request.getParameter("localstorageid");
		String referrerType = null;
		String persona = null;
        boolean flag = false; 
		
		if (cookies != null) {
			for (int i = 0; i < cookies.length; i++) {
				name = cookies[i].getName();

				if (name.equals("spdsuid")) {
					value = cookies[i].getValue();
					break;
				}

			}
		}
		
		if (value == null || value.isEmpty()) {
		    value = localstorageId;	
		}
		
		UserProfile profilev1 = new UserProfile();
		String [] referrerData = line.split(",");
		
		
	//	value = request.getParameter("cookie");  
		if (value != null && !value.isEmpty()) {
	
		//	value = "66560664_d8ba_40ae_a87f_2a72ea30a680";
			value = value.replace("-", "_");
			//persona = CaffeineCache.cache.getIfPresent(value);
			persona = EhcacheKeyCodeRepository.ehcache.basicCache.get(value);
			// value = "d2ff0ee4-851e-4539-bbe8-90266571578a";
			
			if (persona == null || persona.isEmpty() || persona.matches("^[@#]+")) {

				Set<String> result = new HashSet<String>();
				Set<String> latestFrequentCategories = new LinkedHashSet<String>();
				Set<String> latestFrequentCategories1 = new LinkedHashSet<String>();
				ArrayList<String> mostFrequentCategories = new ArrayList<String>();
				ArrayList<String> mostFrequentCategories1 = new ArrayList<String>();
				ArrayList<String> taglist = new ArrayList<String>();
				ArrayList<String> latestFrequentSection = new ArrayList<String>();
				ArrayList<String> latestFrequentSection1 = new ArrayList<String>();
				ArrayList<String> mostFrequentSection = new ArrayList<String>();
				ArrayList<String> mostFrequentSection1 = new ArrayList<String>();
	               
			   // for (String referrerOptimised: referrerData) {
				//	    if(referrer !=null && referrer.toLowerCase().contains(referrerOptimised.toLowerCase())){
				//	    	flag = true;
				//	    	latestFrequentSection.add(referrerOptimised.toLowerCase());
				//	    }
				//	}		        
			        
			   profilev1.setSection(latestFrequentSection);
			
			  
			} else {

				ArrayList<String> result = new ArrayList<String>();
				ArrayList<String> latestFrequentCategories = new ArrayList<String>();
			    Set<String> latestFrequentCategories1 = new HashSet<String>();
				ArrayList<String> mostFrequentCategories = new ArrayList<String>();
				ArrayList<String> mostFrequentCategories1 = new ArrayList<String>();
				ArrayList<String> taglist = new ArrayList<String>();
				Set<String> redundantChecker = new LinkedHashSet<String>();
				ArrayList<String> latestFrequentSection = new ArrayList<String>();
				ArrayList<String> latestFrequentSection1 = new ArrayList<String>();
				ArrayList<String> mostFrequentSection = new ArrayList<String>();
				ArrayList<String> mostFrequentSection1 = new ArrayList<String>();
				
				if (persona.contains("@@")) {
		        String [] personaParts = persona.split("@@");
		        UserProfile profile = new UserProfile();
               
		        //Dense Features not available for real time signals
		        /*
		        for (String referrerOptimised: referrerData) {
				    if(referrer !=null && referrer.toLowerCase().contains(referrerOptimised.toLowerCase())){
				    	flag = true;
				    	latestFrequentSection.add(referrerOptimised.toLowerCase());
				    }
				}		        
		        */
		        
		        try {
		        
				if (personaParts.length > 0 && personaParts[0] != null && !personaParts[0].equals("##"))
					profile.setCity(personaParts[0]);

				if (personaParts.length > 1 && personaParts[1] != null && !personaParts[1].equals("##"))
					profile.setCountry(personaParts[1]);

				if (personaParts.length > 2 && personaParts[2] != null && !personaParts[2].equals("##"))
					profile.setMobileDevice(personaParts[2]);

				if (personaParts.length > 3 && personaParts[3] != null && !personaParts[3].equals("##")) {
					String [] parts = personaParts[3].split(","); 
					for (String part : parts) {
						latestFrequentCategories1.add(part);
					}
					profile.setInMarketSegments(latestFrequentCategories1);
				}
				if (personaParts.length > 4 && personaParts[4] != null && !personaParts[4].equals("##")) {
					String [] parts = personaParts[4].split(","); 
					for (String part : parts) {
						mostFrequentCategories1.add(part);
					}
					profile.setAffinitySegments(mostFrequentCategories1);
				}
				if (personaParts.length > 5 && personaParts[5] != null && !personaParts[5].equals("##") ) {
					String [] parts = personaParts[5].split(","); 
					for (String part : parts) {
						//if (redundantChecker.contains(part) == false)
						 latestFrequentSection.add(part);
					}
					profile.setSection(latestFrequentSection);
				}
				if (personaParts.length > 6 && personaParts[6] != null && !personaParts[6].equals("##")) {
					profile.setGender(personaParts[6].toLowerCase());
				}
				if (personaParts.length > 7 && personaParts[7] != null && !personaParts[7].equals("##")) {
					profile.setAge(personaParts[7]);
				}
				if (personaParts.length > 8 && personaParts[8] != null && !personaParts[8].equals("##")) {
					profile.setIncomelevel(personaParts[8]);
				}
				if (personaParts.length > 9 && personaParts[9] != null && !personaParts[9].equals("##")) {
					String [] parts = personaParts[9].split(","); 
					for (String part : parts) {
						 result.add(part);
					}
					profile.setTags(result);
				}
				
		        }
		        catch (Exception e) {
		        	e.printStackTrace();
		        }
				json = new Gson().toJson(profile);
				
				//System.out.println("Cookie_id1:" + value + "Persona1:" + json);
				//if(flag == true){
				//Later on - Aggregated record will override real time persona entries	
				//	if(json.equals("{}") == false)
				//	EhcacheKeyCodeRepository.ehcache.basicCache.put(value,json);
				//}
				
				}
				
				return json;
			
			}

		}
		
		
		json = new Gson().toJson(profilev1);
		//if(flag == true){
			//Later on - Aggregated record will override real time persona entries	
			//   if(json.equals("{}")== false)
			//    EhcacheKeyCodeRepository.ehcache.basicCache.put(value,json);
	//	}
		return json;

	}

	public static Entry<String, Integer> getMaxEntry(Map<String, Integer> map) {
		Entry<String, Integer> maxEntry = null;
		Integer max = Collections.max(map.values());

		for (Entry<String, Integer> entry : map.entrySet()) {
			Integer value = entry.getValue();
			if (null != value && max == value) {
				maxEntry = entry;
			}
		}
		return maxEntry;
	}

}
