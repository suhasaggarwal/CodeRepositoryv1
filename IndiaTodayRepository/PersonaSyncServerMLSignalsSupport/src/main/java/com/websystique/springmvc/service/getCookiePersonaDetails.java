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
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.gson.Gson;
import com.publisherdata.model.CaffeineCache;
import com.websystique.springmvc.controller.ReportRestController;
import com.websystique.springmvc.model.AffinityPair;
import com.websystique.springmvc.model.SectionPair;
import com.websystique.springmvc.model.TopicPair;
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
		try {

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

			// Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name",
			// "personaq").build();

			// client = new TransportClient(settings)
			// .addTransportAddress(new InetSocketTransportAddress("192.168.106.118", 9300))
			// .addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300))
			// .addTransportAddress(new InetSocketTransportAddress("192.168.106.120",
			// 9300));

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
		String tunePersonaSize = request.getParameter("tunepersonasize");
		if (tunePersonaSize == null || tunePersonaSize.isEmpty()) {
			tunePersonaSize  = "30";
		}
		Integer optimumPersonaSize = Integer.parseInt(tunePersonaSize);
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
		String[] referrerData = line.split(",");

		// value = request.getParameter("cookie");
		if (value != null && !value.isEmpty()) {

			// value = "66560664_d8ba_40ae_a87f_2a72ea30a680";
			try {

				value = value.replace("-", "_");
				// persona = CaffeineCache.cache.getIfPresent(value);
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
					// if(referrer !=null &&
					// referrer.toLowerCase().contains(referrerOptimised.toLowerCase())){
					// flag = true;
					// latestFrequentSection.add(referrerOptimised.toLowerCase());
					// }
					// }

					profilev1.setSection(latestFrequentSection);

				} else {

					List<String> topicList = new ArrayList<String>();
					Set<String> latestFrequentCategories = new LinkedHashSet<String>();
					ArrayList<String> latestFrequentCategories1 = new ArrayList<String>();
					ArrayList<String> mostFrequentCategories = new ArrayList<String>();
					ArrayList<String> mostFrequentCategories1 = new ArrayList<String>();
					ArrayList<String> taglist = new ArrayList<String>();
					Set<String> redundantChecker = new LinkedHashSet<String>();
					ArrayList<String> latestFrequentSection = new ArrayList<String>();
					ArrayList<String> latestFrequentSection1 = new ArrayList<String>();
					ArrayList<String> mostFrequentSection = new ArrayList<String>();
					ArrayList<String> mostFrequentSection1 = new ArrayList<String>();
					ArrayList<String> genderList = new ArrayList<String>();
					ArrayList<String> ageList = new ArrayList<String>();
					ArrayList<String> incomelevelList = new ArrayList<String>();
					ArrayList<String> sectionlist = new ArrayList<String>();
					ArrayList<String> sectionengagementlist = new ArrayList<String>();
					ArrayList<String> segmentengagementlist = new ArrayList<String>();
					ArrayList<String> topicengagementlist = new ArrayList<String>();
					ArrayList<String> affinitysegmentlist = new ArrayList<String>();
					ArrayList<String> sectiondatalist = new ArrayList<String>();
					ArrayList<String> topiclist = new ArrayList<String>();
					ArrayList<String> affinitysegmentetlist = new ArrayList<String>();
					ArrayList<String> sectiondataetlist = new ArrayList<String>();
					ArrayList<String> topicetlist = new ArrayList<String>();
					ArrayList<String> mlsignalslist = new ArrayList<String>();
					ArrayList<String> finalsectionlist = new ArrayList<String>();
					ArrayList<String> topicpaddedlist = new ArrayList<String>();
					ArrayList<String> segmentpaddedlist = new ArrayList<String>();
					ArrayList<String> sectionpaddedlist = new ArrayList<String>();
					ArrayList<String> affinitylistwithoutcodes = new ArrayList<String>();
					ArrayList<String> extradataList = new ArrayList<String>();
					Map affinitylistwithoutcodesmap = new HashMap<String, String>();
					PriorityQueue<AffinityPair> queue = new PriorityQueue<AffinityPair>(100,
							new Comparator<AffinityPair>() {

								public int compare(AffinityPair a, AffinityPair b) {
									return b.count - a.count;
								}
							});

					PriorityQueue<TopicPair> queue1 = new PriorityQueue<TopicPair>(100, new Comparator<TopicPair>() {

						public int compare(TopicPair a, TopicPair b) {
							return b.count - a.count;
						}
					});

					PriorityQueue<SectionPair> queue2 = new PriorityQueue<SectionPair>(100,
							new Comparator<SectionPair>() {

								public int compare(SectionPair a, SectionPair b) {
									return b.count - a.count;
								}
							});

					if (persona.contains("@@")) {
						String[] personaParts = persona.split("@@");
						UserProfile profile = new UserProfile();

						if (personaParts.length > 4 && personaParts[4] != null && !personaParts[4].equals("##")) {
							String[] affinityParts = personaParts[4].split(",");
							String code = null;
							int i = 1;
							for (String part : affinityParts) {
								mostFrequentCategories1.add(part.toLowerCase());
								if (i % 2 == 0 && !part.equals("0")) {
									affinitylistwithoutcodes.add(part);
									affinitylistwithoutcodesmap.put(part, code);
								} else {
									if (!part.equals("0"))
										code = part;
								}

								i++;
							}
						}

						if (personaParts.length > 9 && personaParts[9] != null && !personaParts[9].equals("##")) {
							String[] topicParts = personaParts[9].split(",");
							for (String part : topicParts) {
								if (!part.equals("0"))
									topicList.add(part.toLowerCase());
							}
						}

						if (personaParts.length > 5 && personaParts[5] != null && !personaParts[5].equals("##")) {
							String[] sectionsegments = personaParts[5].split(",");
							Integer marker0 = 30;

							int j = 0;
							boolean remainingdataTracker = false;
							for (int i = 0; i < 30; i++) {
								if (!sectionsegments[i].equals("itweben") && !sectionsegments[i].equals("ajtk")
										&& remainingdataTracker == false)
									sectionlist.add(sectionsegments[i]);

								if (sectionsegments[i].contains("Quarter")) {
									extradataList.add(sectionsegments[i]);
									break;
								}

								if (remainingdataTracker == true) {
									extradataList.add(sectionsegments[i]);
								}

								if (sectionsegments[i].equals("itweben") || sectionsegments[i].equals("ajtk")) {
									remainingdataTracker = true;
									extradataList.add(sectionsegments[i]);
								}

							}

							boolean sectionTracker = false;
							Integer start = sectionlist.size() + extradataList.size();
							if (start <= 15)
								start = 15;
							Integer marker1 = start + personaParts[9].split(",").length;
							Integer marker2 = marker1 + personaParts[4].split(",").length - affinitylistwithoutcodes.size();
							Integer marker3 = marker2 + 15;

							for (int i = start; i < sectionsegments.length; i++) {
								if (i < marker1) {
									if (!sectionsegments[i].equals("0"))
									topicengagementlist.add(sectionsegments[i]);
								}
								if (i >= marker1 && i < marker2) {
									if (!sectionsegments[i].equals("0"))
										segmentengagementlist.add(sectionsegments[i]);
								}
								if (i >= marker2 && i < marker2 + sectionlist.size()) {
									sectionengagementlist.add(sectionsegments[i]);
								}
								if (i >= marker3) {
									mlsignalslist.add(sectionsegments[i]);
								}

							}
						}

						Stream<String> sectionStream = sectionlist.stream();
						Stream<String> sectionEngagementStream = sectionengagementlist.stream();
						Stream<String> segmentStream = affinitylistwithoutcodes.stream();
						Stream<String> segmentEngagementStream = segmentengagementlist.stream();
						Stream<String> topicStream = topicList.stream();
						Stream<String> topicEngagementStream = topicengagementlist.stream();
						String entity = "";
						Integer metric = 0;
						SectionPair spair = null;
						AffinityPair apair = null;
						TopicPair tpair = null;
						if (affinitylistwithoutcodes.size() < optimumPersonaSize) {
							for (int i = 0; i < 30 - affinitylistwithoutcodes.size(); i++) {
								segmentpaddedlist.add("0");
							}
						} else {
							for (int i = 0; i < 30 - optimumPersonaSize; i++) {
								segmentpaddedlist.add("0");
							}
						}

						if (topicList.size() < optimumPersonaSize) {
							for (int i = 0; i < 30 - topicList.size(); i++) {
								topicpaddedlist.add("0");
							}
						} else {
							for (int i = 0; i < 30 - optimumPersonaSize; i++) {
								topicpaddedlist.add("0");
							}
						}

						for (int i = sectionlist.size(); i < 30; i++) {
							sectionpaddedlist.add("0");
						}

						List<SectionPair> sectionpairList = Streams
								.zip(sectionStream, sectionEngagementStream, SectionPair::new)
								.collect(Collectors.toList());

						List<AffinityPair> segmentpairList = Streams
								.zip(segmentStream, segmentEngagementStream, AffinityPair::new)
								.collect(Collectors.toList());

						List<TopicPair> topicpairList = Streams.zip(topicStream, topicEngagementStream, TopicPair::new)
								.collect(Collectors.toList());

						for (SectionPair sp : sectionpairList) {
							queue2.offer(sp);
						}

						for (AffinityPair ap : segmentpairList) {
							queue.offer(ap);
						}

						for (TopicPair tp : topicpairList) {
							queue1.offer(tp);
						}

						while (queue.size() > 0) {
							apair = queue.poll();
							entity = apair.affinitysegment;
							metric = apair.count;
							affinitysegmentlist.add(affinitylistwithoutcodesmap.get(entity).toString());
							affinitysegmentlist.add(entity);
							affinitysegmentetlist.add(metric.toString());
							if (affinitysegmentetlist.size() == optimumPersonaSize) {
								break;
							}
						}

						while (queue1.size() > 0) {
							tpair = queue1.poll();
							entity = tpair.topic;
							metric = tpair.count;
							topiclist.add(entity);
							topicetlist.add(metric.toString());
							if (topicetlist.size() == optimumPersonaSize) {
								break;
							}
						}

						while (queue2.size() > 0) {
							spair = queue2.poll();
							entity = spair.sectionsegment;
							metric = spair.count;
							sectiondatalist.add(entity);
							sectiondataetlist.add(metric.toString());
						}

						sectiondatalist.addAll(extradataList);
						for (int i = sectiondatalist.size(); i < 30; i++) {
							sectiondatalist.add("0");
						}

						sectiondataetlist.addAll(sectionpaddedlist);
						topiclist.addAll(topicpaddedlist);
						topicetlist.addAll(topicpaddedlist);
						affinitysegmentlist.addAll(segmentpaddedlist);
						affinitysegmentetlist.addAll(segmentpaddedlist);

						finalsectionlist = Lists.newArrayList(Iterables.concat(sectiondatalist, topicetlist,
								affinitysegmentetlist, sectiondataetlist, mlsignalslist));
						finalsectionlist.add(value);
						try {

							if (personaParts.length > 0 && personaParts[0] != null && !personaParts[0].equals("##"))
								profile.setCity(personaParts[0].toLowerCase());

							if (personaParts.length > 1 && personaParts[1] != null && !personaParts[1].equals("##"))
								profile.setCountry(personaParts[1].toLowerCase());

							if (personaParts.length > 2 && personaParts[2] != null && !personaParts[2].equals("##"))
								profile.setMobileDevice(personaParts[2].toLowerCase());

							if (personaParts.length > 3 && personaParts[3] != null && !personaParts[3].equals("##")) {
								String[] parts = personaParts[3].split(",");
								for (String part : parts) {
									if (part.contains("Seg") == false) {
										latestFrequentCategories1.add(part.toLowerCase());
									} else {
										latestFrequentCategories1.add(part);
									}
								}
								profile.setInMarketSegments(latestFrequentCategories1);
							}
							if (personaParts.length > 4 && personaParts[4] != null && !personaParts[4].equals("##")) {

								profile.setAffinitySegments(affinitysegmentlist);
							}
							if (personaParts.length > 5 && personaParts[5] != null && !personaParts[5].equals("##")) {
								profile.setSection(finalsectionlist);
							}
							if (personaParts.length > 6 && personaParts[6] != null && !personaParts[6].equals("##")) {
								String[] parts = personaParts[6].split(",");
								for (String part : parts) {
									genderList.add(part.toLowerCase());
								}
								profile.setGender(genderList);
							}
							if (personaParts.length > 7 && personaParts[7] != null && !personaParts[7].equals("##")) {
								String[] parts = personaParts[7].split(",");
								for (String part : parts) {
									ageList.add(part.toLowerCase());
								}
								profile.setAge(ageList);
							}
							if (personaParts.length > 8 && personaParts[8] != null && !personaParts[8].equals("##")) {
								String[] parts = personaParts[8].split(",");
								for (String part : parts) {
									incomelevelList.add(part.toLowerCase());
								}
								profile.setIncomelevel(incomelevelList);
							}
							if (personaParts.length > 9 && personaParts[9] != null && !personaParts[9].equals("##")) {
								profile.setTags(topiclist);
							}

						} catch (Exception e) {
							e.printStackTrace();
						}
						json = new Gson().toJson(profile);

						// System.out.println("Cookie_id1:" + value + "Persona1:" + json);
						// if(flag == true){
						// Later on - Aggregated record will override real time persona entries
						// if(json.equals("{}") == false)
						// EhcacheKeyCodeRepository.ehcache.basicCache.put(value,json);
						// }

					}

					return json;

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		json = new Gson().toJson(profilev1);
		// if(flag == true){
		// Later on - Aggregated record will override real time persona entries
		// if(json.equals("{}")== false)
		// EhcacheKeyCodeRepository.ehcache.basicCache.put(value,json);
		// }
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
