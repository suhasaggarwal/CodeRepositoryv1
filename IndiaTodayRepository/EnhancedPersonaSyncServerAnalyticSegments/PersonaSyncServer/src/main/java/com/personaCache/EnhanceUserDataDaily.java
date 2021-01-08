package com.personaCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class EnhanceUserDataDaily {

	// Enhances existing data points such as geo-based,device-based,Time of the
	// day patterns and update it in corresponding Elasticsearch Document.
	// These enhanced data points are later used to generate publisher based
	// report codes which gives full profile of publishers Audience.

	private static TransportClient client;
	public static Set<String> refurlset = new HashSet<String>(1000000);
//public static Set<String> documentIds = new HashSet<String>(1000000);
//public static Map<String, Boolean> cookieset = new ConcurrentHashMap<String, Boolean>(2000000);

	private static final long serialVersionUID = 1L;
	public static Map<String, String> citycodeMap;
	public static Map<String, String> citycodeMap2;
	public static Map<String, String> countrymap;
	public static Map<String, List<String>> countrystatemap;
	public static Map<String, List<String>> countrystatecitymap;
	public static Map<String, String> citylatlongMap1;

	static {

		String csvFilev1 = "/mnt/data/citylatlong.csv";
		BufferedReader brv1 = null;
		String linev1 = "";
		String cvsSplitByv1 = ",";
		String citykeyv1 = "";
		Map<String, String> citylatlongMap2 = new HashMap<String, String>();
		try {

			brv1 = new BufferedReader(new FileReader(csvFilev1));

			while ((linev1 = brv1.readLine()) != null) {

				try {
					// use comma as separator
					linev1 = linev1.replace(",,", ", , ");
					// System.out.println(line);
					String[] cityDetailsv1 = linev1.split(cvsSplitByv1);
					citykeyv1 = cityDetailsv1[1];
					citylatlongMap2.put(citykeyv1, cityDetailsv1[5] + "," + cityDetailsv1[6]);
					// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);

				} catch (Exception e) {

					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		citylatlongMap1 = Collections.unmodifiableMap(citylatlongMap2);

		// System.out.println(citycodeMap);
	}

	static {
		Map<String, String> countryMap1 = new HashMap<String, String>();
		String csvFile = "/mnt/data/countrycodes.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		String countrykey = "";
		Map<String, String> countryMap2 = new HashMap<String, String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));

			while ((line = br.readLine()) != null) {

				try {
					// use comma as separator
					line = line.replace(",,", ", , ");
					// System.out.println(line);
					String[] countryDetails = line.split(cvsSplitBy);
					countrykey = countryDetails[0];
					countryMap1.put(countrykey, countryDetails[1]);
					// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);

				} catch (Exception e) {

					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		countrymap = Collections.unmodifiableMap(countryMap1);

		// System.out.println(citycodeMap);
	}

	static {
		Map<String, List<String>> countrystateMap1 = new HashMap<String, List<String>>();
		String csvFile1 = "/mnt/data/statecodes.csv";
		BufferedReader br1 = null;
		String line1 = "";
		String cvsSplitBy1 = ",";
		String countrykey1 = "";
		Map<String, String> countrystateMap2 = new HashMap<String, String>();
		List<String> list1 = new ArrayList<String>();
		try {

			br1 = new BufferedReader(new FileReader(csvFile1));

			while ((line1 = br1.readLine()) != null) {

				try {
					// use comma as separator
					line1 = line1.replace(",,", ", , ");
					// System.out.println(line);
					String[] countrystateDetails = line1.split(cvsSplitBy1);
					countrykey1 = countrystateDetails[0];
					if (countrystateMap1.containsKey(countrykey1) == false) {
						List<String> list = new ArrayList<String>();
						list.add(countrystateDetails[1] + ":" + countrystateDetails[2]);
						countrystateMap1.put(countrykey1, list);
						// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
					} else {
						list1 = countrystateMap1.get(countrykey1);
						list1.add(countrystateDetails[1] + ":" + countrystateDetails[2]);
						countrystateMap1.put(countrykey1, list1);

					}
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		countrystatemap = Collections.unmodifiableMap(countrystateMap1);

		// System.out.println(citycodeMap);
	}

	static {
		Map<String, String> cityMap = new HashMap<String, String>();
		String csvFile2 = "/mnt/data/citycode1.csv";
		BufferedReader br2 = null;
		String line2 = "";
		String cvsSplitBy2 = ",";
		String key = "";
		Map<String, String> cityMap1 = new HashMap<String, String>();
		Map<String, List<String>> cityMap2 = new HashMap<String, List<String>>();
		List<String> list2 = new ArrayList<String>();

		try {

			br2 = new BufferedReader(new FileReader(csvFile2));

			while ((line2 = br2.readLine()) != null) {

				try {
					// use comma as separator
					line2 = line2.replace(",,", ", , ");
					// System.out.println(line);
					String[] geoDetails = line2.split(cvsSplitBy2);
					key = geoDetails[6];
					cityMap1.put(key, geoDetails[5]);
					// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
					cityMap.put(geoDetails[5], key);
					if (cityMap2.containsKey(geoDetails[0] + ":" + geoDetails[1]) == false) {
						List<String> list = new ArrayList<String>();
						list.add(geoDetails[2]);
						cityMap2.put(geoDetails[0] + ":" + geoDetails[1], list);
						// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
					} else {
						list2 = cityMap2.get(geoDetails[0] + ":" + geoDetails[1]);
						list2.add(geoDetails[2]);
						cityMap2.put(geoDetails[0] + ":" + geoDetails[1], list2);

					}
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		citycodeMap = Collections.unmodifiableMap(cityMap1);
		citycodeMap2 = Collections.unmodifiableMap(cityMap);
		countrystatecitymap = Collections.unmodifiableMap(cityMap2);
		// System.out.println(citycodeMap);
	}

	public static Map<String, String> oscodeMap;
	static {
		Map<String, String> osMap = new HashMap<String, String>();
		String csvFile = "/mnt/data/oscode2.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		String key = "";
		Map<String, String> osMap1 = new HashMap<String, String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));

			while ((line = br.readLine()) != null) {

				try {
					// use comma as separator
					line = line.replace(",,", ", , ");
					// System.out.println(line);
					String[] osDetails = line.split(cvsSplitBy);
					key = osDetails[0];
					osMap1.put(key, osDetails[1]);
					// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		oscodeMap = Collections.unmodifiableMap(osMap1);
		System.out.println(oscodeMap);
	}

	public static Map<String, String> oscodeMap1;
	static {
		Map<String, String> osMap2 = new HashMap<String, String>();
		String csvFile = "/mnt/data/system_os.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		String key = "";
		Map<String, String> osMap3 = new HashMap<String, String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));

			while ((line = br.readLine()) != null) {

				try {
					// use comma as separator
					line = line.replace(",,", ", , ");
					// System.out.println(line);
					String[] osDetails = line.split(cvsSplitBy);
					key = osDetails[2];
					osMap3.put(key, osDetails[0] + "," + osDetails[1]);
					// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		oscodeMap1 = Collections.unmodifiableMap(osMap3);
		System.out.println(oscodeMap1);
	}

	public static Map<String, String> devicecodeMap;
	static {
		Map<String, String> deviceMap = new HashMap<String, String>();
		String csvFile = "/mnt/data/devicecode2.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		String key = "";
		Map<String, String> deviceMap1 = new HashMap<String, String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));

			while ((line = br.readLine()) != null) {

				try {
					// use comma as separator
					line = line.replace(",,", ", , ");
					// System.out.println(line);
					String[] deviceDetails = line.split(cvsSplitBy);
					key = deviceDetails[0];
					deviceMap1.put(key, deviceDetails[1] + "," + deviceDetails[4] + "," + deviceDetails[8]);
					// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		devicecodeMap = Collections.unmodifiableMap(deviceMap1);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> audienceSegmentMap;
	public static Map<String, String> audienceSegmentMap1;
	public static Map<String, String> audienceSegmentMap2;

	static {
		Map<String, String> audienceMap = new HashMap<String, String>();
		String csvFile = "/mnt/data/subcategorymap1.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		String key = "";
		Map<String, String> audienceMap1 = new HashMap<String, String>();
		Map<String, String> audienceMap2 = new HashMap<String, String>();

		try {

			br = new BufferedReader(new FileReader(csvFile));

			while ((line = br.readLine()) != null) {

				try {
					// use comma as separator
					line = line.replace(",,", ", , ");
					// System.out.println(line);
					String[] segmentDetails = line.split(cvsSplitBy);
					key = segmentDetails[0];
					audienceMap1.put(key, segmentDetails[1]);
					audienceMap.put(segmentDetails[4], key);
					// hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
					audienceMap2.put(key, segmentDetails[4]);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		audienceSegmentMap = Collections.unmodifiableMap(audienceMap1);
		audienceSegmentMap1 = Collections.unmodifiableMap(audienceMap);
		audienceSegmentMap2 = Collections.unmodifiableMap(audienceMap2);
//   System.out.println(deviceMap);
	}

	public static Map<String, String> AuthorMap;
	public static Map<String, String> AuthorMap1;

	static {
		Map<String, String> authorMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/authorMap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> authorMap1 = new HashMap<String, String>();
		Map<String, String> authorMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] authorDetails = line5.split(cvsSplitBy5);
					key2 = authorDetails[0];
					authorMap1.put(key2, authorDetails[1]);
					authorMap2.put(authorDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		AuthorMap = Collections.unmodifiableMap(authorMap1);
		AuthorMap1 = Collections.unmodifiableMap(authorMap2);
//   System.out.println(deviceMap);
	}

	public static Map<String, String> UrlMap;
	public static Map<String, String> UrlMap1;

	static {
		Map<String, String> authorMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/urlMap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> urlMap1 = new HashMap<String, String>();
		Map<String, String> urlMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] urlDetails = line5.split(cvsSplitBy5);
					key2 = urlDetails[0];
					urlMap1.put(key2, urlDetails[1]);
					urlMap2.put(urlDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		UrlMap = Collections.unmodifiableMap(urlMap1);
		UrlMap1 = Collections.unmodifiableMap(urlMap2);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> CountryMap;
	public static Map<String, String> CountryMap1;

	static {
		Map<String, String> countryMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/countryMap2.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> countryMapv1 = new HashMap<String, String>();
		Map<String, String> countryMapv2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] countryDetails = line5.split(cvsSplitBy5);
					key2 = countryDetails[0];
					countryMapv1.put(key2, countryDetails[1]);
					countryMapv2.put(countryDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		CountryMap = Collections.unmodifiableMap(countryMapv1);
		CountryMap1 = Collections.unmodifiableMap(countryMapv2);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> StateMap;
	public static Map<String, String> StateMap1;

	static {
		Map<String, String> stateMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/statesMap5.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> stateMap1 = new HashMap<String, String>();
		Map<String, String> stateMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] stateDetails = line5.split(cvsSplitBy5);
					key2 = stateDetails[1] + "," + stateDetails[2];
					// stateMap1.put(key2,stateDetails[1]);
					stateMap2.put(stateDetails[0], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		// StateMap = Collections.unmodifiableMap(stateMap1);
		StateMap1 = Collections.unmodifiableMap(stateMap2);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> AgeMap;
	public static Map<String, String> AgeMap1;

	static {
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

	public static Map<String, String> GenderMap;
	public static Map<String, String> GenderMap1;

	static {
		Map<String, String> genderMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/genderMap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> genderMap1 = new HashMap<String, String>();
		Map<String, String> genderMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] genderDetails = line5.split(cvsSplitBy5);
					key2 = genderDetails[0];
					genderMap1.put(key2, genderDetails[1]);
					genderMap2.put(genderDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		GenderMap = Collections.unmodifiableMap(genderMap1);
		GenderMap1 = Collections.unmodifiableMap(genderMap2);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> IncomeMap;
	public static Map<String, String> IncomeMap1;

	static {
		Map<String, String> incomeMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/incomeMap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> incomeMap1 = new HashMap<String, String>();
		Map<String, String> incomeMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] incomeDetails = line5.split(cvsSplitBy5);
					key2 = incomeDetails[0];
					incomeMap1.put(key2, incomeDetails[1]);
					incomeMap2.put(incomeDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		IncomeMap = Collections.unmodifiableMap(incomeMap1);
		IncomeMap1 = Collections.unmodifiableMap(incomeMap2);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> tagMap;
	public static Map<String, String> tagMap1;

	static {
		Map<String, String> TagMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/TagMap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> TagMap1 = new HashMap<String, String>();
		Map<String, String> TagMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] TagDetails = line5.split(cvsSplitBy5);
					key2 = TagDetails[0];
					TagMap1.put(key2, TagDetails[1]);
					TagMap2.put(TagDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		tagMap = Collections.unmodifiableMap(TagMap1);
		tagMap1 = Collections.unmodifiableMap(TagMap2);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> tagMap2;
	public static Map<String, String> tagMap3;

	static {
		Map<String, String> TagMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/TagMap1.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> TagMap3 = new HashMap<String, String>();
		Map<String, String> TagMap5 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] TagDetails = line5.split(cvsSplitBy5);
					key2 = TagDetails[0];
					TagMap3.put(key2, TagDetails[1]);
					TagMap5.put(TagDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		tagMap2 = Collections.unmodifiableMap(TagMap3);
		tagMap3 = Collections.unmodifiableMap(TagMap5);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> referrerTypeMap;
	public static Map<String, String> referrerTypeMap1;

	static {
		Map<String, String> ReferrerTypeMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/referrerTypeMap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> ReferrerTypeMap1 = new HashMap<String, String>();
		Map<String, String> ReferrerTypeMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] referrerTypeDetails = line5.split(cvsSplitBy5);
					key2 = referrerTypeDetails[0];
					ReferrerTypeMap1.put(key2, referrerTypeDetails[1]);
					ReferrerTypeMap2.put(referrerTypeDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		referrerTypeMap = Collections.unmodifiableMap(ReferrerTypeMap1);
		referrerTypeMap1 = Collections.unmodifiableMap(ReferrerTypeMap2);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> sectionMap;
	public static Map<String, String> sectionMap1;

	static {
		Map<String, String> SectionMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/sectionmap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> SectionMap1 = new HashMap<String, String>();
		Map<String, String> SectionMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] sectionDetails = line5.split(cvsSplitBy5);
					key2 = sectionDetails[0];
					SectionMap2.put(sectionDetails[1], key2);

					if (sectionDetails[1].toLowerCase().equals("khel-khiladi"))
						sectionDetails[1] = "khel-khiladi";
					if (sectionDetails[1].toLowerCase().equals("filmy-dunia"))
						sectionDetails[1] = "filmy-dunia";
					if (sectionDetails[1].toLowerCase().equals("news-digest"))
						sectionDetails[1] = "news-digest";

					SectionMap1.put(key2, sectionDetails[1]);

				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		sectionMap = Collections.unmodifiableMap(SectionMap1);
		sectionMap1 = Collections.unmodifiableMap(SectionMap2);
		// System.out.println(deviceMap);
	}

	public static Map<String, String> deviceMap;
	public static Map<String, String> deviceMap1;

	static {
		Map<String, String> DeviceMap = new HashMap<String, String>();
		String csvFile5 = "/mnt/data/deviceMap.csv";
		BufferedReader br5 = null;
		String line5 = "";
		String cvsSplitBy5 = ",";
		String key2 = "";
		Map<String, String> DeviceMap1 = new HashMap<String, String>();
		Map<String, String> deviceMap2 = new HashMap<String, String>();

		try {

			br5 = new BufferedReader(new FileReader(csvFile5));

			while ((line5 = br5.readLine()) != null) {

				try {
					// use comma as separator
					line5 = line5.replace(",,", ", , ");
					// System.out.println(line);
					String[] deviceDetails = line5.split(cvsSplitBy5);
					key2 = deviceDetails[0];
					DeviceMap1.put(key2, deviceDetails[1]);
					deviceMap2.put(deviceDetails[1], key2);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

		}

		catch (Exception e) {

			e.printStackTrace();
		}

		deviceMap = Collections.unmodifiableMap(DeviceMap1);
		deviceMap1 = Collections.unmodifiableMap(deviceMap2);
		// System.out.println(deviceMap);
	}

	// Sets up ElasticSearch Instance, ensures instance is a Singleton
	public static void setUp() throws Exception {

		if (client == null) {

			Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "personaq").build();
			client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("192.168.106.118", 9300))
		   		    .addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300))
					.addTransportAddress(new InetSocketTransportAddress("192.168.106.120", 9300));

		//	 Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name",
		//      "elasticsearch").build();
		//	 client = new TransportClient(settings).addTransportAddress(new
		//	 InetSocketTransportAddress("localhost", 9300));

		}
		System.out.println("Finished the setup process...");
		
	}

	// Configuration for ES
	private static InetSocketTransportAddress getTransportAddress() {
		String host = null;
		String port = null;
		if (host == null) {
			host = "192.168.106.118";
			// System.out.println("ES_TEST_HOST enviroment variable does not exist. choose
			// default '172.16.105.231'");
		}
		if (port == null) {
			port = "9300";
			// System.out.println("ES_TEST_PORT enviroment variable does not exist. choose
			// default '9300'");
		}
		System.out.println(String.format("Connection details: host: %s. port:%s.", new Object[] { host, port }));
		return new InetSocketTransportAddress(host, Integer.parseInt(port));
	}

	// Derives Hostname from url
	public static String getHostName(String url) {
		URI uri = null;
		try {
			uri = new URI(url);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		String hostname = uri.getHost();
		if (hostname != null) {
			return hostname.startsWith("www.") ? hostname.substring(4) : hostname;
		}
		return hostname;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// Sets up ElasticSearch Instance
		setUp();

		// Sets up Date range for ES records for which Enhancement is to be done
		// Needs to be configured when doing specific enhancement
		// Ideal time configuration (It should not be run on already enhanced data
		// points)
		String startdate = args[0];

		String enddate = args[1];

		String channel_name = args[2];

		String file_name = args[3];
		
		String referrerType = null;
		
		String affinitySegments = null;
		
		String sections = null;
		
		String tags = null;
		
		if(args.length > 4 && args[4]!=null && !args[4].isEmpty())
		   referrerType = args[4];
		
		if(args.length > 5 && args[5]!=null && !args[5].isEmpty())
	       affinitySegments = args[5];
		
		if(args.length > 6 && args[6]!=null && !args[6].isEmpty())
		   sections = args[6];
		
		if(args.length > 7 && args[7]!=null && !args[7].isEmpty())
		   tags = args[7];
		EnhanceUserDataDailyv1.cachePersonas(startdate, enddate, referrerType, affinitySegments, sections, tags, channel_name, file_name);

	}
}
