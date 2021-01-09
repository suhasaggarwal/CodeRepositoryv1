package com.personaCache;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import javax.servlet.http.HttpServletRequest;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import com.google.gson.Gson;
import com.websystique.springmvc.model.Pair;
import com.websystique.springmvc.model.UserProfile;
import com.websystique.springmvc.service.ElasticSearchAPIs;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;

public class WorkerThread3 implements Callable<String> {

	Client client;
	String channelName = null;
	String cookie_id = null;
	String filename = null;
	FileWriter writer = null;
	FileWriter batchwriter = null;
	String referrerTypeDerivation = null;
	String affinitySegments = null;
	String sections = null;
	String tagsOptimised = null;
	String batchfilename = null;
	AtomicInteger progressTracker;
    Integer batchTracker = 0;  
	Integer LastBatchCounter = 0;
    
	public WorkerThread3(Client client, String cookie,  String referrerType, String affinitySegments, String sections, String tags, FileWriter writer, FileWriter batchwriter,String batchfilename,AtomicInteger progressTracker, int LastBatchCounter) {

		this.client = client;
		this.cookie_id = cookie;
		this.writer = writer;
		this.batchwriter = batchwriter;
		this.referrerTypeDerivation = referrerType;
		this.affinitySegments = affinitySegments;
		this.sections = sections;
		this.tagsOptimised = tags;
		this.batchfilename = batchfilename;
		this.progressTracker = progressTracker;
		this.LastBatchCounter = LastBatchCounter;
		
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

	public String call() {
		progressTracker.getAndIncrement();
		batchTracker = progressTracker.get();
		System.out.println("Cookies processed: " + progressTracker.toString());
		// Handle the hit...
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
		Map<String, Integer> channelMap = new HashMap<String, Integer>();
		Set<String> latestaudienceSegment = new LinkedHashSet<String>();
		Set<String> latestsection = new LinkedHashSet<String>();
		Set<String> channel_name = new LinkedHashSet<String>();
		Set<String> referrerTypeSet = new HashSet<String>();
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
		Integer count2 = 0;
		Integer value3 = 0;
		Integer value4 = 0;
		String agegroup = null;
		String gender = null;
		String incomelevel = null;
		String tags = null;
		String section = null;
		String sectionvalue = null;
		String[] tagparts = null;
		String fieldSeparator = "@@";
		String channelname = null;
		String channelValue = null;
		String referrerType = null;
		String sourceUrl = null;
		String affinitySegmentstobeOptimised = null;
		// value = "d2ff0ee4-851e-4539-bbe8-90266571578a";
		SearchHit[] results = ElasticSearchAPIs.searchDocumentFingerprintIds(client, "enhanceduserdatabeta1", "core2",
				"cookie_id", cookie_id);
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
			channelname = (String) result.get("channel_name");
            referrerType = (String) result.get("referrerType");
            sourceUrl  = (String) result.get("sourceUrl");
			
            if (referrerTypeDerivation != null && !referrerTypeDerivation.isEmpty()){
              String[] referrerTypeData = referrerTypeDerivation.split(",");
            
            if (referrerType != null && !referrerType.isEmpty()) {
            for (String referrer : referrerTypeData) {
            	if (referrerType.toLowerCase().contains(referrer.toLowerCase())){
            		referrerTypeSet.add(referrer.toLowerCase());
            	}
			 }
            }
           			
            if(sourceUrl != null && !sourceUrl.isEmpty()) {
            	 for (String referrer : referrerTypeData) {
                 	if (sourceUrl.toLowerCase().contains(referrer.toLowerCase())){
                 		referrerTypeSet.add(referrer.toLowerCase());
                 	}
     			}
             }
            }
            	
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

			if (channelname != null && !channelname.isEmpty()) {
				if (channelMap.containsKey(channelname) == false) {
					channelMap.put(channelname, 1);
					// System.out.println(result.get("request_time")+"\n");
					// System.out.println("Adding Value First Time"+"\n");
				} else {

					count2 = channelMap.get(channelname);
					channelMap.put(channelname, count2 + 1);
					value3 = count2 + 1;
					// System.out.println(result.get("request_time")+"\n");
					// System.out.println("Added Count to field count:"+value2+":"+
					// sectionvalue+"\n");

				}

			}

			channelValue = Collections
					.max(channelMap.entrySet(), (entry1, entry2) -> entry1.getValue() - entry2.getValue()).getKey();

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
				
				if (affinitySegments != null && !affinitySegments.isEmpty()) {
				    String [] affinitySegmentsData = affinitySegments.split(",");
				
				for (String affinitysegment: affinitySegmentsData) {
				    if(pair.getKey().toString().toLowerCase().contains(affinitysegment.toLowerCase())){
						 mostFrequentCategories.add(pair.getKey().toString());
					 }
				}
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

				if (sections != null && !sections.isEmpty()) {
				    String [] sectionsData = sections.split(",");
				
				for (String sectionOptimised: sectionsData) {
				    if(pair.getKey().toString().toLowerCase().contains(sectionOptimised.toLowerCase())){
				    	latestFrequentSection.add(pair.getKey().toString().toLowerCase());
				    }
				}
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

		if (channelValue != null && !channelValue.isEmpty()) {
			latestFrequentSection.add(channelValue.trim().toLowerCase());
		}

		for (String referrerData : referrerTypeSet) {
			latestFrequentSection.add(referrerData.trim().toLowerCase());
		}
		
		
		for (String s1 : latestFrequentCategories) {
			latestFrequentCategories1.add(EnhanceUserDataDaily.audienceSegmentMap2.get(s1).trim());
			// latestFrequentCategories1.add(s1.toLowerCase().replace("_", "
			// ").replaceAll("\\.", " "));
		}

		for (String s2 : mostFrequentCategories) {
			mostFrequentCategories1.add(EnhanceUserDataDaily.audienceSegmentMap2.get(s2).trim());
			// mostFrequentCategories1.add(s2.toLowerCase().replace("_", "
			// ").replaceAll("\\.", " "));
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
				if (queue.size() > 30) {
					queue.poll();
				}
				if (tagsOptimised != null && !tagsOptimised.isEmpty()) {
				    String [] tagsData = tagsOptimised.split(",");
				
				    for (String tagsOptimisedData: tagsData) {
				    if(entry.getKey().toLowerCase().toString().toLowerCase().contains(tagsOptimisedData.toLowerCase())){
				    	result.add(entry.getKey().toString().toLowerCase());
				    }
				}
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
			profile.setAge(EnhanceUserDataDaily.AgeMap1.get(agegroup));

		if (incomelevel != null)
			profile.setIncomelevel(incomelevel.toLowerCase());

		if (result != null)
			profile.setTags(result);

		json = new Gson().toJson(profile);
	//	System.out.println("Cookie_id:" + cookie_id + "Persona:" + json);

		StringBuilder sbf = new StringBuilder("");
		if (city != null && !city.trim().isEmpty()) {
			sbf.append(city.toLowerCase());
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (country != null && !country.trim().isEmpty()) {
			sbf.append(country.toLowerCase());
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (mobiledevice != null && !mobiledevice.trim().isEmpty()) {
			sbf.append(mobiledevice.toLowerCase());
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (latestFrequentCategories1 != null && !latestFrequentCategories1.isEmpty()) {
			sbf.append(String.join(",", latestFrequentCategories1));
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (mostFrequentCategories1 != null && !mostFrequentCategories1.isEmpty()) {
			sbf.append(String.join(",", mostFrequentCategories1));
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (latestFrequentSection != null && !latestFrequentSection.isEmpty()) {
			sbf.append(String.join(",", latestFrequentSection));
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (gender != null && !gender.isEmpty()) {
			sbf.append(gender.toLowerCase());
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (EnhanceUserDataDaily.AgeMap1.get(agegroup) != null
				&& !EnhanceUserDataDaily.AgeMap1.get(agegroup).isEmpty()) {
			sbf.append(EnhanceUserDataDaily.AgeMap1.get(agegroup));
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (incomelevel != null && !incomelevel.isEmpty()) {
			sbf.append(incomelevel.toLowerCase());
		} else {
			sbf.append("##");
		}
		sbf.append("@@");
		if (result != null && !result.isEmpty()) {
			sbf.append(String.join(",", result));
		} else {
			sbf.append("##");
		}
	//	System.out.println("Persona:" + sbf.toString());
		try {
			EnhanceUserDataDaily.PersonaQueue.put(cookie_id + ":@" + sbf.toString());
			System.out.println("Counter Value:"); 
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if(batchTracker % 1000 == 0 || batchTracker.equals(LastBatchCounter))
		System.out.println("Counter Value from Thread :"+ Thread.currentThread().getId() + ":" + new Integer(batchTracker).toString());
	    
		if(batchTracker % 1000 == 0 || batchTracker.equals(LastBatchCounter)) {
	    	FileWriter batchwriter = null;
			try {
				batchwriter = new FileWriter(batchfilename+(batchTracker/1000),true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 	
	    
        if (batchTracker % 1000 == 0) {
		for(int i=0; i<1000; i++) {
	    	try {
				batchwriter.write(EnhanceUserDataDaily.PersonaQueue.take()+"\n");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
			   continue;
			}
	    	
	    }
        } else {
        	
        	for(int i=0; i<LastBatchCounter%1000-1; i++) {
    	    	try {
    				batchwriter.write(EnhanceUserDataDaily.PersonaQueue.take()+"\n");
    			} catch (Exception e1) {
    				// TODO Auto-generated catch block
    			   continue;
    			}
    	    	
    	    }   
        
        }
			
	    try {
			batchwriter.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
	    /*
	    
	    HttpUrl.Builder urlBuilder = HttpUrl.parse("http://localhost:8080/publisherv1/loadNativeCache?cacheFile="+batchfilename+(batchTracker/1000)).newBuilder();
	    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
	    System.out.println(timestamp + "Ingestion Url:" + "http://localhost:8080/publisherv1/loadNativeCache?cacheFile="+batchfilename+(batchTracker/1000));
	    String url = urlBuilder.build().toString();

	    Request request = new Request.Builder()
		                     .url(url)
		                     .build();
		OkHttpClient client = new OkHttpClient();
		
		client.newCall(request).enqueue(new Callback() {
		    @Override
		    public void onFailure(Call call, IOException e) {
		        e.printStackTrace();
		    }

		    @Override
		    public void onResponse(Call call, final Response response) throws IOException {
		        if (!response.isSuccessful()) {
		            throw new IOException("Unexpected code " + response);
		        } else {
		    }
		  }
		});
	    
	    */
	    
	    }
	    
		
	
		try {			
	     	writer.write(cookie_id + ":@" + sbf.toString() + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
		return "";

		/*
		 * Set<String> result = new HashSet<String>(); Set<String>
		 * latestFrequentCategories = new LinkedHashSet<String>(); Set<String>
		 * latestFrequentCategories1 = new LinkedHashSet<String>(); ArrayList<String>
		 * mostFrequentCategories = new ArrayList<String>(); ArrayList<String>
		 * mostFrequentCategories1 = new ArrayList<String>(); ArrayList<String> taglist
		 * = new ArrayList<String>(); Set<String> latestFrequentSection = new
		 * LinkedHashSet<String>(); Set<String> latestFrequentSection1 = new
		 * LinkedHashSet<String>(); ArrayList<String> mostFrequentSection = new
		 * ArrayList<String>(); ArrayList<String> mostFrequentSection1 = new
		 * ArrayList<String>();
		 * 
		 * String cookiePersona = ehcache.get(cookie_id); String [] personaParts =
		 * cookiePersona.split("@@"); UserProfile profile = new UserProfile();
		 * 
		 * if (personaParts[0] != null) profile.setCity(personaParts[0])\;
		 * 
		 * if (personaParts[1] != null) profile.setCountry(personaParts[1]);
		 * 
		 * if (personaParts[2] != null) profile.setMobileDevice(personaParts[2]);
		 * 
		 * if (personaParts[3] != null) { String [] parts = personaParts[3].split(",");
		 * for (String part : parts) { latestFrequentCategories1.add(part); }
		 * profile.setInMarketSegments(latestFrequentCategories1); } if (personaParts[4]
		 * != null) { String [] parts = personaParts[4].split(","); for (String part :
		 * parts) { mostFrequentCategories1.add(part); }
		 * profile.setAffinitySegments(mostFrequentCategories1); } if (personaParts[5]
		 * != null) { String [] parts = personaParts[5].split(","); for (String part :
		 * parts) { latestFrequentSection.add(part); }
		 * profile.setSection(latestFrequentSection); } if (personaParts[6] != null) {
		 * profile.setGender(gender.toLowerCase()); } if (personaParts[7] != null) {
		 * profile.setAge(EnhanceUserDataDaily.AgeMap1.get(agegroup)); } if
		 * (personaParts[8] != null) {
		 * profile.setIncomelevel(incomelevel.toLowerCase()); } if (personaParts[9] !=
		 * null) { String [] parts = personaParts[9].split(","); for (String part :
		 * parts) { result.add(part); } profile.setTags(result); } json = new
		 * Gson().toJson(profile); System.out.println("Cookie_id1:" + cookie_id +
		 * "Persona1:" + json); return "";
		 * 
		 */

	}

}
