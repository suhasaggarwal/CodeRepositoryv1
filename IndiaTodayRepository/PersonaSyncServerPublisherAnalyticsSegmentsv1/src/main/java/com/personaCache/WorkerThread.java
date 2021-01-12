package com.personaCache;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
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
import java.util.regex.Pattern;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import com.google.common.base.Charsets;
import com.google.common.io.CharSink;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.websystique.springmvc.model.Pair;
import com.websystique.springmvc.model.UserProfile;
import com.websystique.springmvc.service.ElasticSearchAPIs;

public class WorkerThread implements Callable<String> {

	Client client;
	String channelName = null;
	String cookie_id = null;
	String filename = null;
	FileWriter writer = null;
	String referrerTypeDerivation = null;
	String affinitySegments = null;
	String sections = null;
	String tagsOptimised = null;
   
	
	public WorkerThread(Client client, String cookie, String referrerType, String affinitySegments, String sections,
			String tags, FileWriter writer) {

		this.client = client;
		this.cookie_id = cookie;
		this.writer = writer;
		this.referrerTypeDerivation = referrerType;
		this.affinitySegments = affinitySegments;
		this.sections = sections;
		this.tagsOptimised = tags;
		
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
		// Handle the hit...
		
		try {
		
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
		HashMap<String, Integer> sectionengagementTime = new HashMap<String, Integer>();
		HashMap<String, Integer> topicengagementTime = new HashMap<String, Integer>();
		HashMap<String, Integer> segmentengagementTime = new HashMap<String, Integer>();
		HashMap<String, Integer> sessionmap = new HashMap<String, Integer>();
		HashMap<String, Integer> quartermap = new HashMap<String, Integer>();
		Map<String, Integer> gendermap = new HashMap<String, Integer>();
		Map<String, Integer> agemap = new HashMap<String, Integer>();
		Map<String, Integer> incomemap = new HashMap<String, Integer>();
		Map<String, Integer> channelMap = new HashMap<String, Integer>();
		Set<String> latestaudienceSegment = new LinkedHashSet<String>();
		Set<String> latestsection = new LinkedHashSet<String>();
		Set<String> sessionset = new LinkedHashSet<String>();
		Set<String> channel_name = new LinkedHashSet<String>();
		Set<String> dates = new LinkedHashSet<String>();
		Set<String> referrerTypeSet = new HashSet<String>();
		List<Integer> segmentengagementtimes = new ArrayList<Integer>();
		List<Integer> topicengagementtimes = new ArrayList<Integer>();
		List<Integer> sectionengagementtimes = new ArrayList<Integer>();
		List<String> logs = new ArrayList<String>();
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
		Integer totalnumberofsessions = 0;
		Integer timesincelastsession = 0;
		Integer totalengagementtime = 0;
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
		String sessionhash = null;
		Integer engagementTime = 0;
		String date = null;
		String quarter = null;
		String mostactivequarter = null;
		String topic = null;
		String loyaltyType = null;
		String visitor_counter = null;
		Integer visitor_id = null;
		SearchHit[] results = ElasticSearchAPIs.searchDocumentFingerprintIds(client, "enhanceduserdatabeta1", "core2",
				"cookie_id", cookie_id);
		for (SearchHit hit : results) {
			// System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
            if (result.get("_id") != null)
			document_id = (String) result.get("_id");
			if (result.get("modelName") != null)
            mobiledevice = (String) result.get("modelName");
			if (result.get("city") != null)
			city = (String) result.get("city");
			if (result.get("country") != null)
			country = (String) result.get("country");
			if (result.get("subcategory") != null)
			audienceSegment = (String) result.get("subcategory");
			if (result.get("request_time") != null)
			request_time = (String) result.get("request_time");
			if (result.get("tag") != null)
			tags = (String) result.get("tag");
			if (result.get("section") != null)
			sectionvalue = (String) result.get("section");
			if (result.get("channel_name") != null)
			channelname = (String) result.get("channel_name");
			if (result.get("referrerType") != null)
			referrerType = (String) result.get("referrerType");
			if (result.get("sourceUrl") != null)
			sourceUrl = (String) result.get("sourceUrl");
			if (result.get("engagementTime") != null)
			engagementTime = (Integer) result.get("engagementTime");
			if (result.get("session_id") != null)
			sessionhash = (String) result.get("session_id");
			if (result.get("date") != null)
			date = (String) result.get("date");
            if (result.get("QuarterValue") != null)
			quarter = (String) result.get("QuarterValue");
            if (result.get("visitor_id") != null)
    		visitor_counter = (String) result.get("visitor_id");
            if (visitor_counter != null && visitor_counter.length() > 4) {
            	visitor_counter = visitor_counter.substring(0, 4);
            }
            if (visitor_counter != null && !visitor_counter.isEmpty() && !visitor_counter.equals("NaN") && visitor_counter.chars().allMatch(x -> Character.isDigit(x))) {
            	visitor_id = Integer.parseInt(visitor_counter);
            }
            
			if (referrerTypeDerivation != null && !referrerTypeDerivation.isEmpty()) {
				String[] referrerTypeData = referrerTypeDerivation.split(",");

				if (referrerType != null && !referrerType.isEmpty()) {
					for (String referrer : referrerTypeData) {
						if (referrerType.toLowerCase().contains(referrer.toLowerCase())) {
							referrerTypeSet.add(referrer.toLowerCase());
						}
					}
				}

				if (sourceUrl != null && !sourceUrl.isEmpty()) {
					for (String referrer : referrerTypeData) {
						if (sourceUrl.toLowerCase().contains(referrer.toLowerCase())) {
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

			if (tags != null) {
				for (int k = 0; k < tagparts.length; k++) {
					if (tagparts[k] != null && !tagparts[k].isEmpty()) {
						tags = tagparts[k].trim();
                        tags = tags.toLowerCase();
						if (tags != null && !tags.isEmpty() && engagementTime != null ) {
							if (topicengagementTime.containsKey(tags) == false) {
								topicengagementTime.put(tags, engagementTime);
								// System.out.println(result.get("request_time")+"\n");
								// System.out.println("Adding Value First Time"+"\n");
							} else {

								count1 = topicengagementTime.get(tags);
								topicengagementTime.put(tags, count1 + engagementTime);
								value2 = count1 + engagementTime;
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

			if (sessionhash != null && !sessionhash.isEmpty()) {
				sessionset.add(sessionhash);
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

			
			if (quarter != null && !quarter.isEmpty()) {
				if (quartermap.containsKey(quarter) == false) {
					quartermap.put(quarter, 1);
					// System.out.println(result.get("request_time")+"\n");
					// System.out.println("Adding Value First Time"+"\n");
				} else {

					count = quartermap.get(quarter);
					quartermap.put(quarter, count + 1);
					value1 = count + 1;
					// System.out.println(result.get("request_time")+"\n");
					// System.out.println("Added Count to field count:"+value1+":"+
					// audienceSegmentValue+"\n");

				}

			}				
			
			
			
			if (audienceSegmentValue != null && !audienceSegmentValue.isEmpty() && engagementTime!=null) {
				if (segmentengagementTime.containsKey(audienceSegmentValue) == false) {
					segmentengagementTime.put(audienceSegmentValue, engagementTime);
					// System.out.println(result.get("request_time")+"\n");
					// System.out.println("Adding Value First Time"+"\n");
				} else {

					count = segmentengagementTime.get(audienceSegmentValue);
					segmentengagementTime.put(audienceSegmentValue, count + engagementTime);
					value1 = count + engagementTime;
					// System.out.println(result.get("request_time")+"\n");
					// System.out.println("Added Count to field count:"+value1+":"+
					// audienceSegmentValue+"\n");

				}

				

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

			if (sectionvalue != null && !sectionvalue.isEmpty() && engagementTime != null) {
				    sectionvalue = sectionvalue.toLowerCase();
				if (sectionengagementTime.containsKey(sectionvalue) == false) {
					sectionengagementTime.put(sectionvalue, engagementTime);
					// System.out.println(result.get("request_time")+"\n");
					// System.out.println("Adding Value First Time"+"\n");
				} else {

					count1 = sectionengagementTime.get(sectionvalue);
					sectionengagementTime.put(sectionvalue, count1 + engagementTime);
					value2 = count1 + engagementTime;
					// System.out.println(result.get("request_time")+"\n");
					// System.out.println("Added Count to field count:"+value2+":"+
					// sectionvalue+"\n");

				}

		

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

			dates.add(date);

			if (engagementTime != null)
			totalengagementtime += engagementTime;

		}

		Set<String> result = new HashSet<String>();
		Set<String> redundancychecker = new HashSet<String>();
		Set<String> latestFrequentCategories = new LinkedHashSet<String>();
		Set<String> latestFrequentCategories1 = new LinkedHashSet<String>();
		ArrayList<String> mostFrequentCategories = new ArrayList<String>();
		ArrayList<String> mostFrequentCategories1 = new ArrayList<String>();
		ArrayList<String> taglist = new ArrayList<String>();
		ArrayList<String> latestFrequentSection = new ArrayList<String>();
		ArrayList<String> latestFrequentSection1 = new ArrayList<String>();
		ArrayList<String> mostFrequentSection = new ArrayList<String>();
		ArrayList<String> mostFrequentSection1 = new ArrayList<String>();
		
		totalnumberofsessions = sessionset.size();
		Integer tracker = 0;
		String date1 = null;
		String mostrecentDate = null;
		Date d1 = null;
        Date d2 = null;
		Long diff = null;
		Long diffHours = null;

		for (String consecutivedate : dates) {
			//logs.add("Date:" + consecutivedate);
			if (tracker == 0) {
				date1 = consecutivedate;
				//logs.add("Date1:" + date1);
			}
			if (tracker == 1) {
				mostrecentDate = consecutivedate;
				//logs.add("Date2:" + mostrecentDate);
				break;
			}
			tracker++;
		}

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

		if (quartermap != null && !quartermap.isEmpty())
		    mostactivequarter = Collections.max(quartermap.entrySet(), (entry1, entry2) -> entry1.getValue() - entry2.getValue()).getKey();

		/*
		try {
			if (date1 != null) {
			d1 = format.parse(date1);
		//	System.out.println("Date1:" + d1.toLocaleString());
			}
			
			if (mostrecentDate != null) {
			d2 = format.parse(mostrecentDate);
		//	System.out.println("Date2:"+ d2.toLocaleString());
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// in milliseconds
		if (d1 != null &&  d2 !=null)
		    diff = d2.getTime() - d1.getTime();

		if (diff != null) {
		    diffHours = diff / (60 * 60 * 1000) % 24;
            //logs.add("Difference in Hours:" + diffHours);
		}
		*/
		
		Integer daysdiff = null;
		
		// create date instances
		if (date1 != null && mostrecentDate != null) {
		LocalDate localDate1 = LocalDate.parse(date1);
		LocalDate localDate2 = LocalDate.parse(mostrecentDate);
		daysdiff = Period.between(localDate2, localDate1).getDays();
		}
		
		if (audienceSegmentCountMap != null && !audienceSegmentCountMap.isEmpty()) {
			Iterator it = audienceSegmentCountMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				// System.out.println(pair.getKey() + " = " + pair.getValue());
				if (Integer.parseInt(pair.getValue().toString()) > 1) {

					mostFrequentCategories.add(pair.getKey().toString());

				}

				if (affinitySegments != null && !affinitySegments.isEmpty()) {
					String[] affinitySegmentsData = affinitySegments.split(",");

					for (String affinitysegment : affinitySegmentsData) {
						if (pair.getKey().toString().toLowerCase().contains(affinitysegment.toLowerCase())) {
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
					String[] sectionsData = sections.split(",");

					for (String sectionOptimised : sectionsData) {
						if (pair.getKey().toString().toLowerCase().contains(sectionOptimised.toLowerCase())) {
							latestFrequentSection.add(pair.getKey().toString().toLowerCase());
							 if (sectionengagementTime.get(pair.getKey().toString().toLowerCase()) != null) {
								 //logs.add("Section Engagement Time List:" + pair.getKey().toString().toLowerCase() + ":" + sectionengagementTime.get(pair.getKey().toString().toLowerCase()));	
								 sectionengagementtimes.add(sectionengagementTime.get(pair.getKey().toString().toLowerCase()));
						     } 
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
					    if (sectionengagementTime.get(section1.toLowerCase()) != null) {
					    	//logs.add("Section Engagement Time List:" + section1.toLowerCase() + ":" + sectionengagementTime.get(section1.toLowerCase()));	
					    	sectionengagementtimes.add(sectionengagementTime.get(section1.toLowerCase()));
				
					    }   
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
			latestFrequentCategories1.add(s1.trim());
			// latestFrequentCategories1.add(s1.toLowerCase().replace("_", "
			// ").replaceAll("\\.", " "));
		}

		for (String s2 : mostFrequentCategories) {
			if (redundancychecker.contains(s2) == false) {
			mostFrequentCategories1.add(s2.trim());
			if (segmentengagementTime.get(s2) != null) {
				//logs.add("Segment Engagement Time List:" + s2 + ":" + segmentengagementTime.get(s2));	
				segmentengagementtimes.add(segmentengagementTime.get(s2));
			}
			redundancychecker.add(s2.trim());
		 }
		}

		redundancychecker.clear();

		// Create a Heap

		PriorityQueue<Pair> queue = new PriorityQueue<Pair>(1000, new Comparator<Pair>() {

			public int compare(Pair a, Pair b) {
				return a.count - b.count;
			}
		});
		
		if(mostactivequarter !=null && !mostactivequarter.isEmpty()) {
			//logs.add("Most Active Quarter:" + mostactivequarter);	
			latestFrequentSection.add(mostactivequarter);
		}
		
		if (tagMap != null && !tagMap.isEmpty()) {
			for (Map.Entry<String, Integer> entry : tagMap.entrySet()) {
				Pair p = new Pair(entry.getKey().toLowerCase(), entry.getValue());
				queue.offer(p);
				if (queue.size() > 30) {
					queue.poll();
				}
				if (tagsOptimised != null && !tagsOptimised.isEmpty()) {
					String[] tagsData = tagsOptimised.split(",");

					for (String optimumTags : tagsData) {
						if (entry.getKey().toLowerCase().toString().toLowerCase()
								.contains(optimumTags.toLowerCase()) && redundancychecker.contains(optimumTags) == false) {
							result.add(entry.getKey().toString().toLowerCase());
							
							if (topicengagementTime.get(entry.getKey().toString().toLowerCase()) != null) {
								//logs.add("Topic Engagement Time List:" + entry.getKey().toString().toLowerCase() + ":" + topicengagementTime.get(entry.getKey().toString().toLowerCase()).toString());	
								topicengagementtimes.add(topicengagementTime.get(entry.getKey().toString().toLowerCase()));
							}
						   
							redundancychecker.add(optimumTags.toLowerCase());
						}
					}
				}

			}

			while (queue.size() > 0) {
				topic = queue.poll().topic;
				result.add(topic.toLowerCase());
				if (topicengagementTime.get(topic.toLowerCase()) != null && redundancychecker.contains(topic.toLowerCase()) == false) {
					//logs.add("Topic Engagement Time List:" + topic.toLowerCase() + ":" + topicengagementTime.get(topic.toLowerCase()).toString());	
					topicengagementtimes.add(topicengagementTime.get(topic.toLowerCase()));
				}
				redundancychecker.add(topic.toLowerCase()); 
		   }

		}
		
		for (int j=0; j < topicengagementtimes.size(); j++) {
		     if (topicengagementtimes.get(j) != null) {
				 //logs.add("Topic Engagement Time:" + topicengagementtimes.get(j).toString());	
				 latestFrequentSection.add(topicengagementtimes.get(j).toString());
			 } 
		}
		
		
		for (int i=0; i<30; i++) {
			 if (i >= topicengagementtimes.size()) {
				 //logs.add("Padding Topic Engagement Time");
				 latestFrequentSection.add("0");
			 }	 			 			
		  }
		
		for (int j=0; j < segmentengagementtimes.size(); j++) {
		     if (segmentengagementtimes.get(j) != null) {
				 //logs.add("Segment Engagement Time:" + segmentengagementtimes.get(j).toString());	
				 latestFrequentSection.add(segmentengagementtimes.get(j).toString());
			 } 
		}
	
		for (int i=0; i<30; i++) {
			 if (i >= segmentengagementtimes.size()) {
				 //logs.add("Padding Segment Engagement Time");
				 latestFrequentSection.add("0");
			 }	 			 			
		  }
		

		
		for (int j=0; j < sectionengagementtimes.size(); j++) {
		     if (sectionengagementtimes.get(j) != null) {
				 //logs.add("Section Engagement Time:" + sectionengagementtimes.get(j).toString());	
				 latestFrequentSection.add(sectionengagementtimes.get(j).toString());
			 } 
		}
		
		for (int i=0; i<15; i++) {
			 if (i >= sectionengagementtimes.size()) {
				 //logs.add("Padding Section Engagement Time");
				 latestFrequentSection.add("0");
			 }		 			
		}
		
		if (visitor_id != null && visitor_id < 2 )
		   loyaltyType = "new";
		
		if (visitor_id != null && visitor_id >= 2 && visitor_id < 7)
		   loyaltyType = "returning";
			
		if (visitor_id != null && visitor_id >= 7)
		   loyaltyType = "loyal";
		
		if (gendermap != null && !gendermap.isEmpty())
			gender = getMaxEntry(gendermap).getKey();

		if (agemap != null && !agemap.isEmpty())
			agegroup = getMaxEntry(agemap).getKey();

		if (incomemap != null && !incomemap.isEmpty())
			incomelevel = getMaxEntry(incomemap).getKey();

		UserProfile profile = new UserProfile();

		if (totalengagementtime != null) {
		    //logs.add("Total Engagement Time:"+totalengagementtime.toString());
			latestFrequentSection.add(totalengagementtime.toString());
		}else {
			//logs.add("Total Engagement Time:"+"0");
			latestFrequentSection.add("0");
		}
		
		if (totalnumberofsessions != null) {
			//logs.add("Total Number of sessions:"+totalnumberofsessions.toString());
		    latestFrequentSection.add(totalnumberofsessions.toString());
		}else {
			//logs.add("Total Number of sessions:"+"0");
			latestFrequentSection.add("0");
		}
		
		if (daysdiff != null) {
			//logs.add("Time Since Last Session:"+daysdiff.toString());
			latestFrequentSection.add(daysdiff.toString());
		}else {
			//logs.add("Time Since Last Session:"+"0");
			latestFrequentSection.add("0");
		}
		
		
		if (loyaltyType != null) {
			//logs.add("Loyalty Nature:"+ loyaltyType);
			latestFrequentSection.add(loyaltyType);
		}else {
			//logs.add("Data not available");
			latestFrequentSection.add("0");
		}
		
		if (visitor_id != null) {
			//logs.add("Visit Counter:"+ visitor_id.toString());
			latestFrequentSection.add(visitor_id.toString());
		}else {
			//logs.add("Data not available");
			latestFrequentSection.add("0");
		}
		
		
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
		//logs.add("Cookie_id:" + cookie_id + "Persona:" + json);
		//EnhanceUserDataDaily.writetoFile(logs);
		
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
		System.out.println("Persona:" + sbf.toString());
		try {
			writer.write(cookie_id + ":@" + sbf.toString() + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return "";

	}

}
