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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.personaCache.EnhanceUserDataDaily;
import com.personaCache.WorkerThread3;
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
	static final Logger logger = LoggerFactory.getLogger(getCookiePersonaDetails.class);
	
    static {	
		
		
		Map<String, String> ageMap = new HashMap<String, String>();
		String csvFile5 = "I://ageMap.csv";
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

		//	Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "personaq").build();

		//	client = new TransportClient(settings)
		//			.addTransportAddress(new InetSocketTransportAddress("192.168.106.118", 9300))
		//       	.addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300))
        //			.addTransportAddress(new InetSocketTransportAddress("192.168.106.120", 9300));

	
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
		String persona = null;

		if (cookies != null) {
			for (int i = 0; i < cookies.length; i++) {
				name = cookies[i].getName();

				if (name.equals("spdsuid")) {
					value = cookies[i].getValue();
					break;
				}

			}
		}
		UserProfile profilev1 = new UserProfile();

	//	value = request.getParameter("cookie");  
		if (value != null && !value.isEmpty()) {

		//	value = "66560664_d8ba_40ae_a87f_2a72ea30a680";
			value = value.replace("-", "_");
			persona = CaffeineCache.cache.getIfPresent(value);
			// value = "d2ff0ee4-851e-4539-bbe8-90266571578a";
			
			if (persona == null || persona.isEmpty() || persona.matches("^[@#]+")) {
				 logger.debug("Cookie_id : "+value+" : "+" Cache Miss");
				/*
				SearchHit[] results = ElasticSearchAPIs.searchDocumentFingerprintIds(client, "enhanceduserdatabeta1",
						"core2", "cookie_id", value);

				for (SearchHit hit : results) {
					// System.out.println("------------------------------");
					Map<String, Object> result = hit.getSource();

					document_id = (String) result.get("_id");
					mobiledevice = (String) result.get("modelName");
					city = (String) result.get("city");
					country = (String) result.get("country");
					audienceSegment = (String) result.get("subcategory");
					request_time = (String) result.get("request_time");
					tags = (String) result.get("tag");
					sectionvalue = (String) result.get("section");

					if (tags != null)
						tagparts = tags.split(",");

					if (tags != null) {
						for (int k = 0; k < tagparts.length; k++) {
							if (tagparts[k] != null && !tagparts[k].isEmpty()) {
								tags = tagparts[k].trim();

								if (tags != null && !tags.isEmpty()) {
									if (tagMap.containsKey(tags) == false) {
										tagMap.put(tags, 1);
										// System.out.println(result.get("request_time")+"\n");
										// System.out.println("Adding Value First Time"+"\n");
									} else {

										count1 = tagMap.get(tags);
										tagMap.put(tags, count1 + 1);
										value2 = count1 + 1;
										// System.out.println(result.get("request_time")+"\n");
										// System.out.println("Added Count to field count:"+value2+":"+tags+"\n");
									}

								}
							}
						}

					}

					String dateStart = request_time;
					// String dateStop = "01/15/2012 10:31:48";

					// HH converts hour in 24 hours format (0-23), day calculation
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

					Date d1 = null;
					// Date d2 = null;

					Date d2 = new Date();

					try {
						d1 = format.parse(dateStart);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					// d2 = format.parse(date);

					// in milliseconds
					long diff = d2.getTime() - d1.getTime();

					long diffHours = diff / (60 * 60 * 1000) % 24;

					if ((String) result.get("agegroup") != null && !((String) result.get("agegroup")).isEmpty()) {
						if (diffHours > 3) {
							agegroup = (String) result.get("agegroup");

							if (agegroup != null) {

								if (agegroup != null && !agegroup.isEmpty()) {

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

					if ((String) result.get("gender") != null && !((String) result.get("gender")).isEmpty()) {
						if (diffHours > 3) {
							gender = (String) result.get("gender");
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

					if ((String) result.get("incomelevel") != null && !((String) result.get("incomelevel")).isEmpty()) {
						if (diffHours > 3) {

							incomelevel = (String) result.get("incomelevel");

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

					if (audienceSegment != null && !audienceSegment.isEmpty()) {
						audienceSegmentValue = audienceSegment.trim();
					}

					if (audienceSegmentValue != null && !audienceSegmentValue.isEmpty()) {
						if (audienceSegmentCountMap.containsKey(audienceSegmentValue) == false) {
							audienceSegmentCountMap.put(audienceSegmentValue, 1);
							// System.out.println(result.get("request_time")+"\n");
							// System.out.println("Adding Value First Time"+"\n");
						} else {

							count = audienceSegmentCountMap.get(audienceSegmentValue);
							audienceSegmentCountMap.put(audienceSegmentValue, count + 1);
							value1 = count + 1;
							// System.out.println(result.get("request_time")+"\n");
							// System.out.println("Added Count to field count:"+value1+":"+
							// audienceSegmentValue+"\n");

						}

						if (latestaudienceSegment.size() < 6)
							latestaudienceSegment.add(audienceSegmentValue);

					}

					if (sectionvalue != null && !sectionvalue.isEmpty()) {
						if (sectionMap.containsKey(sectionvalue) == false) {
							sectionMap.put(sectionvalue, 1);
							// System.out.println(result.get("request_time")+"\n");
							// System.out.println("Adding Value First Time"+"\n");
						} else {

							count1 = sectionMap.get(sectionvalue);
							sectionMap.put(sectionvalue, count1 + 1);
							value2 = count1 + 1;
							// System.out.println(result.get("request_time")+"\n");
							// System.out.println("Added Count to field count:"+value2+":"+
							// sectionvalue+"\n");

						}

						if (latestsection.size() < 4)
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

				if (audienceSegmentCountMap != null && !audienceSegmentCountMap.isEmpty()) {
					Iterator it = audienceSegmentCountMap.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry pair = (Map.Entry) it.next();
						// System.out.println(pair.getKey() + " = " + pair.getValue());
						if (Integer.parseInt(pair.getValue().toString()) > 1) {

							mostFrequentCategories.add(pair.getKey().toString());

						}

					}
				}

				if (latestaudienceSegment != null && !latestaudienceSegment.isEmpty()) {

					if (mostFrequentCategories != null && !mostFrequentCategories.isEmpty()) {
						for (String segment : latestaudienceSegment) {
							if (mostFrequentCategories.contains(segment))
								latestFrequentCategories.add(segment);
						}

					}

				}

				if (sectionMap != null && !sectionMap.isEmpty()) {
					Iterator it = sectionMap.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry pair = (Map.Entry) it.next();
						// System.out.println(pair.getKey() + " = " + pair.getValue());
						if (Integer.parseInt(pair.getValue().toString()) > 1) {

							mostFrequentSection.add(pair.getKey().toString());

						}

					}
				}

				if (latestsection != null && !latestsection.isEmpty()) {

					if (mostFrequentSection != null && !mostFrequentSection.isEmpty()) {
						for (String section1 : latestsection) {
							if (mostFrequentSection.contains(section1))
								latestFrequentSection.add(section1.toLowerCase());
						}

					}

				}

				for (String s1 : latestFrequentCategories) {
					// System.out.println(s1);
					// latestFrequentCategories1.add(audienceSegmentMap2.get(s1));
					latestFrequentCategories1.add(s1.toLowerCase().replace("_", " ").replaceAll("\\.", " "));
				}

				for (String s2 : mostFrequentCategories) {
					// System.out.println(s2);
					// mostFrequentCategories1.add(audienceSegmentMap2.get(s2));
					mostFrequentCategories1.add(s2.toLowerCase().replace("_", " ").replaceAll("\\.", " "));
				}

				// Create a Heap

				PriorityQueue<Pair> queue = new PriorityQueue<Pair>(1000, new Comparator<Pair>() {

					public int compare(Pair a, Pair b) {
						return a.count - b.count;
					}
				});

				if (tagMap != null && !tagMap.isEmpty()) {
					for (Map.Entry<String, Integer> entry : tagMap.entrySet()) {
						Pair p = new Pair(entry.getKey().toLowerCase(), entry.getValue());
						queue.offer(p);
						if (queue.size() > 15) {
							queue.poll();
						}
					}

					while (queue.size() > 0) {
						result.add(queue.poll().topic);
					}

				}

				if (gendermap != null && !gendermap.isEmpty())
					gender = getMaxEntry(gendermap).getKey();

				if (agemap != null && !agemap.isEmpty())
					agegroup = getMaxEntry(agemap).getKey();

				if (incomemap != null && !incomemap.isEmpty())
					incomelevel = getMaxEntry(incomemap).getKey();

				UserProfile profile = new UserProfile();

				if (city != null)
					profile.setCity(city.toLowerCase());

				if (country != null)
					profile.setCountry(country.toLowerCase());

				if (mobiledevice != null)
					profile.setMobileDevice(mobiledevice.toLowerCase());

				if (latestFrequentCategories1 != null)
					profile.setInMarketSegments(latestFrequentCategories1);

				if (mostFrequentCategories1 != null)
					profile.setAffinitySegments(mostFrequentCategories1);

				if (latestFrequentSection != null)
					profile.setSection(latestFrequentSection);

				if (gender != null)
					profile.setGender(gender.toLowerCase());

				if (agegroup != null)
					profile.setAge(AgeMap1.get(agegroup));

				if (incomelevel != null)
					profile.setIncomelevel(incomelevel.toLowerCase());

				if (result != null)
					profile.setTags(result);

	 			json = new Gson().toJson(profile);
*/
			} else {

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
				
		        String [] personaParts = persona.split("@@");
		        UserProfile profile = new UserProfile();

				if (personaParts[0] != null && !personaParts[0].equals("##"))
					profile.setCity(personaParts[0]);

				if (personaParts[1] != null && !personaParts[1].equals("##"))
					profile.setCountry(personaParts[1]);

				if (personaParts[2] != null && !personaParts[2].equals("##"))
					profile.setMobileDevice(personaParts[2]);

				if (personaParts[3] != null && !personaParts[3].equals("##")) {
					String [] parts = personaParts[3].split(","); 
					for (String part : parts) {
						latestFrequentCategories1.add(part);
					}
					profile.setInMarketSegments(latestFrequentCategories1);
				}
				if (personaParts[4] != null && !personaParts[4].equals("##")) {
					String [] parts = personaParts[4].split(","); 
					for (String part : parts) {
						mostFrequentCategories1.add(part);
					}
					profile.setAffinitySegments(mostFrequentCategories1);
				}
				if (personaParts[5] != null && !personaParts[5].equals("##") ) {
					String [] parts = personaParts[5].split(","); 
					for (String part : parts) {
						latestFrequentSection.add(part);
					}
					profile.setSection(latestFrequentSection);
				}
				if (personaParts[6] != null && !personaParts[6].equals("##")) {
					profile.setGender(personaParts[6].toLowerCase());
				}
				if (personaParts[7] != null && !personaParts[7].equals("##")) {
					profile.setAge(personaParts[7]);
				}
				if (personaParts[8] != null && !personaParts[8].equals("##")) {
					profile.setIncomelevel(personaParts[8]);
				}
				if (personaParts[9] != null && !personaParts[9].equals("##")) {
					String [] parts = personaParts[9].split(","); 
					for (String part : parts) {
						 result.add(part);
					}
					profile.setTags(result);
				}
				json = new Gson().toJson(profile);
				//System.out.println("Cookie_id1:" + value + "Persona1:" + json);
				logger.debug("Cookie_id : "+value+" : "+" Cache hit");
				return json;
			
			}

		}
		
		json = new Gson().toJson(profilev1);
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
