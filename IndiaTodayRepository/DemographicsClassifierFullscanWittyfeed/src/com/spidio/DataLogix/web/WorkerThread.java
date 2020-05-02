package com.spidio.DataLogix.web;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.search.SearchHit;

import com.spidio.UserSegmenter.IndexCategoriesData;
import com.spidio.UserSegmenter.IndexCategoriesData2;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class WorkerThread implements Callable<String> {

	SearchHit hit;
	Client client;
	BulkProcessor bulkProcessor;
	String keywords = null;
	String description = null;
	String[] searchkeyword = null;
	String[] searchkeyword1 = null;

	// String [] finalsearchkeyword = null;
	String finalsearchkeyword;
	SearchHit[] matchingsegmentrecords = null;
	String category = null;
	String subcategory = null;
	String finalCategory = null;
	String refurl = null;
	String title = null;
	String audienceSegment = null;
	String[] parts = null;
	String[] parts1 = null;
	String keywordcheck = null;
	String title1 = null;
	String title2 = null;
	String id = null;
	TreeMap<String, Integer> categoryCount = new TreeMap<String, Integer>();
	TreeMap<String, Integer> keywordcategoryCount = new TreeMap<String, Integer>();
	TreeMap<String, Integer> aggregatecategoryCount = new TreeMap<String, Integer>();
	TreeMap<String, Integer> subcategoryMap = new TreeMap<String, Integer>();

	Integer count = 0;
	Integer flag = 0;
	Integer count1 = 0;
	// String id;
	Integer count2 = 0;
	Integer count3 = 0;
	Integer categorycount = 0;
	String agegroup = null;
	String gender = null;
	String incomelevel = null;
	String agegroupcalc = null;
	String gendercalc = null;
	String incomelevelcalc = null;
	String channelName = null;
	Map<String, Integer> gendermap = new HashMap<String, Integer>();
	Map<String, Integer> agemap = new HashMap<String, Integer>();
	Map<String, Integer> incomemap = new HashMap<String, Integer>();
	// Data is copied in elasticsearch every 5 minutes, this module scans
	// elasticsearch records for past 5 minutes and classifies them in to
	// categories and corresponding subcategories
	String genderValue = null;

	String incomeValue = null;

	String ageValue = null;

	Integer flag1 = 0;

	Integer flag2 = 0;

	Integer flag3 = 0;

	String fingerprintId = null;

	Map<String, String> categorysubcategoryMap = new HashMap<String, String>();

	String[] agegroup1 = null;

	String ecommerceCategory = "";

	String tag = "";

	Integer price = 0;

	Long handsetPrice = new Long(0);

	String price_range = "";

	String startdate;
	String enddate;

	Map<String, Object> result0;
	Map<String, Object> result;
	Map<String, Object> result1;
	Map<String, Object> result2;

	public WorkerThread(SearchHit hit, Client client, String startdate, String enddate) {
		this.hit = hit;
		this.client = client;
		this.startdate = startdate;
		this.enddate = enddate;

	}

	// Demography rules
	@Override
	public String call() {
		// Handle the hit...

		String srcIP = "";
		String ES_ID = this.hit.getId();
		String ES_INDEX = this.hit.getIndex();
		try {

			// System.out.println("------------------------------");

			result = hit.getSource();
			fingerprintId = (String) result.get("cookie_id");

			// if (fingerprintId != null && !fingerprintId.equals("")
			// && getDemographicsFullDataorInterval.cookieset.containsKey(fingerprintId) ==
			// false) {
			if (fingerprintId != null && !fingerprintId.equals("")
					&& getDemographicsFullDataorInterval.cookieset.put(fingerprintId, true) == null) {

				// fingerprintId = "728088282.2167482368.1716827031";

				// System.out.println(fingerprintId);
				// String [] fingerprintSegments = fingerprintId.split("\\.");
				// System.out.println(fingerprintSegments[0]+":"+fingerprintSegments[1]+":"+fingerprintSegments[2]+":"+fingerprintSegments[3]);
				// if(fingerprintSegments.length > 1)
				// fingerprintId =
				// fingerprintSegments[0].trim()+"."+fingerprintSegments[1].trim()+"."+fingerprintSegments[2].trim();
				// fingerprintId = fingerprintId.trim();

				// Store/Site specific Categorisation

				SearchHit[] results = IndexCategoriesData2.searchDocumentContent(client, "enhanceduserdatabeta1",
						"core2", "cookie_id", fingerprintId);

				for (SearchHit hit0 : results) {

					try {

						Map<String, Object> result0 = hit0.getSource();

						if (result0 != null) {

							refurl = (String) result0.get("refcurrentoriginal");
							title = (String) result0.get("page_title");
							id = (String) hit0.getId();
							channelName = (String) result0.get("channel_name");
							tag = (String) result0.get("tag");
							category = (String) result0.get("subcategory");
							audienceSegment = (String) result0.get("audience_segment");
							price_range = (String) result0.get("price_range");
						}

						// String gender1 = (String)result0.get("gender");
						// System.out.println("Test index"+gender);

						 System.out.println("Category:" + category + "\n");

						// ecommerceCategory = (String) result0.get("ecommerceCategory");

						ecommerceCategory = audienceSegment + category;

						// price = (Integer)result0.get("price");

						if (tag == null)
							tag = "";

						if (ecommerceCategory == null)
							ecommerceCategory = "";

						if (price == null)
							price = 0;

						if (price_range != null && !price_range.isEmpty())
							handsetPrice = (Long) NumberFormat.getNumberInstance(java.util.Locale.US)
									.parse(price_range);

						if (price_range == null)
							price_range = "";

						if (category != null && category.toLowerCase().isEmpty() == false && audienceSegment != null
								&& audienceSegment.toLowerCase().isEmpty() == false) {
							if (refurl.contains("123greeting") == true) {

								gender = "female";
								genderValue = "female";
							}

							if (category.toLowerCase().contains("beauty")) {
								gender = "female";
								genderValue = "female";

							}

							if (category.toLowerCase().contains("food"))
								gender = "female";

							if (category.toLowerCase().contains("fashion"))
								gender = "female";

							if (category.toLowerCase().contains("jewelry")) {
								gender = "female";
								genderValue = "female";
							}

							if (category.toLowerCase().contains("skirts")) {
								gender = "female";
								genderValue = "female";
							}

							if (category.toLowerCase().contains("parenting")) {
								gender = "female";
								genderValue = "female";
							}

							if (category.toLowerCase().contains("politics")) {
								gender = "male";
								genderValue = "male";
							}

							if (category.toLowerCase().contains("science")) {
								gender = "male";
								genderValue = "male";
							}

							if (category.toLowerCase().contains("health")) {
								gender = "female";
								genderValue = "female";
							}

							if (audienceSegment.toLowerCase().contains("sports")) {
								gender = "male";

								genderValue = "male";
							}

							if (audienceSegment.toLowerCase().contains("technology")) {
								gender = "male";
								genderValue = "male";
							}

							if (category.toLowerCase().contains("bollywood"))
								genderValue = "male";

							if (refurl.contains("silicon"))
								genderValue = "male";

							if (category.toLowerCase().contains("garden")) {
								gender = "female";
								genderValue = "female";
							}

							/*
							 * if (ecommerceCategory.toLowerCase().contains("furnishing")) gender =
							 * "female";
							 * 
							 * 
							 * if (ecommerceCategory.toLowerCase().contains("sports")) gender = "male";
							 */

							if (category.toLowerCase().contains("automotive")) {
								gender = "male";
								genderValue = "male";
							}

							if (category.toLowerCase().contains("shirts")) {
								gender = "male";
								genderValue = "male";
							}

							if (refurl.toLowerCase().contains("men") && !refurl.toLowerCase().contains("women"))
								genderValue = "male";

							if (category.toLowerCase().contains("men") && !category.toLowerCase().contains("women"))
								genderValue = "male";

							if (category.toLowerCase().contains("women"))
								genderValue = "female";

							if (tag.toLowerCase().contains("men") && !tag.toLowerCase().contains("women"))
								genderValue = "male";

							if (tag.toLowerCase().contains("women"))
								genderValue = "female";

							if (tag.toLowerCase().contains("girl"))
								genderValue = "female";

							if (tag.toLowerCase().contains("boy"))
								genderValue = "male";

							if (refurl.toLowerCase().contains("girl"))
								genderValue = "female";

							if (refurl.toLowerCase().contains("boy"))
								genderValue = "male";

							if (refurl.toLowerCase().contains("women"))
								genderValue = "female";

							if (audienceSegment.toLowerCase().contains("business")) {
								gender = "male";
								genderValue = "male";
							}

							if (audienceSegment.toLowerCase().contains("news")) {
								gender = "male";
								genderValue = "male";
							}

							if (gender != null && !gender.isEmpty()) {

								if (gendermap.containsKey(gender) == false) {
									gendermap.put(gender, 1);

								} else {

									count = gendermap.get(gender);

									gendermap.put(gender, count + 1);

								}

							}
						}
					} catch (Exception e) {

						e.printStackTrace();
						continue;

					}

					gender = null;

				}

				for (SearchHit hit1 : results) {

					try {
						Map<String, Object> result0 = hit1.getSource();

						if (result0 != null) {

							refurl = (String) result0.get("refcurrentoriginal");
							title = (String) result0.get("page_title");
							id = (String) hit1.getId();
							channelName = (String) result0.get("channel_name");
							tag = (String) result0.get("tag");
							category = (String) result0.get("subcategory");
							audienceSegment = (String) result0.get("audience_segment");
							price_range = (String) result0.get("price_range");
						}

						if (category != null && category.toLowerCase().isEmpty() == false && audienceSegment != null
								&& audienceSegment.toLowerCase().isEmpty() == false) {

							if (audienceSegment.toLowerCase().contains("business")) {
								incomelevel = "high";
								incomeValue = "high";
							}

							if (category.toLowerCase().contains("game") && category.toLowerCase().contains("video")) {
								incomelevel = "low";
								incomeValue = "low";

							}

							if (refurl.contains("123greetings")) {

								incomelevel = "high";
								incomeValue = "high";

							}

							/*
							 * if(price !=null && price > 35000 ){
							 * 
							 * incomelevel = "high"; incomeValue = "high"; }
							 */
							if (handsetPrice > 40000) {

								incomelevel = "high";
								incomeValue = "high";

							}

							if (handsetPrice != 0 && handsetPrice < 5000) {

								incomelevel = "low";
								incomeValue = "low";

							}

							if (category.toLowerCase().contains("toy")) {
								incomelevel = "low";
								incomeValue = "low";
							}

							/*
							 * if (ecommerceCategory.toLowerCase().contains("mobile") &&
							 * ecommerceCategory.toLowerCase().contains("feature") ){ incomelevel = "low";
							 * incomeValue = "low"; }
							 * 
							 * if (ecommerceCategory.toLowerCase().contains("mobile") && price < 2000 ){
							 * incomelevel = "low"; incomeValue = "low"; }
							 */

							if (tag.toLowerCase().contains("girl")) {

								incomelevel = "low";
								incomeValue = "low";

							}

							if (tag.toLowerCase().contains("boy")) {

								incomelevel = "low";
								incomeValue = "low";

							}

							if (refurl.toLowerCase().contains("girl")) {
								incomelevel = "low";
								incomeValue = "low";
							}

							if (refurl.toLowerCase().contains("boy")) {
								incomelevel = "low";
								incomeValue = "low";
							}

							if (category.toLowerCase().contains("education")) {

								incomelevel = "low";
								incomeValue = "low";

							}

							/*
							 * if (refurl.contains("silicon")) {
							 * 
							 * incomelevel = "high"; incomeValue = "high";
							 * 
							 * }
							 * 
							 */

							if (incomelevel != null && !incomelevel.isEmpty()) {

								if (incomemap.containsKey(incomelevel) == false) {
									incomemap.put(incomelevel, 1);

								} else {

									count = incomemap.get(incomelevel);

									incomemap.put(incomelevel, count + 1);

								}

							}

							// }
						}

					} catch (Exception e) {

						e.printStackTrace();
						continue;

					}

					incomelevel = null;

				}

				for (SearchHit hit2 : results) {

					try {

						Map<String, Object> result0 = hit2.getSource();

						if (result0 != null) {

							refurl = (String) result0.get("refcurrentoriginal");
							title = (String) result0.get("page_title");
							id = (String) hit2.getId();
							channelName = (String) result0.get("channel_name");
							tag = (String) result0.get("tag");
							category = (String) result0.get("subcategory");
							audienceSegment = (String) result0.get("audience_segment");
							price_range = (String) result0.get("price_range");
						}

						// Store/Site specific Categorisation

						if (category != null && category.toLowerCase().isEmpty() == false && audienceSegment != null
								&& audienceSegment.toLowerCase().isEmpty() == false) {

							if (audienceSegment.toLowerCase().contains("business")) {
								agegroup = "high:medium";
							}

							if (refurl.contains("silicon"))
								agegroup = "high:medium";

							if (category.toLowerCase().contains("education"))
								ageValue = "low";

							if (category.toLowerCase().contains("bollywood"))
								ageValue = "medium";

							if (category.toLowerCase().contains("game"))
								ageValue = "low";

							if (refurl.contains("123greetings"))
								agegroup = "medium";

							if (audienceSegment.toLowerCase().contains("news"))
								agegroup = "high:medium";

							if (audienceSegment.toLowerCase().contains("sports"))
								agegroup = "medium";

							if (category.toLowerCase().contains("technology"))
								agegroup = "medium";

							if (category.toLowerCase().contains("science"))
								agegroup = "high:medium";

							if (category.toLowerCase().contains("garden"))
								agegroup = "high:medium";

							if (category.toLowerCase().contains("automotive")) {
								agegroup = "high:medium";
							}

							// if (ecommerceCategory.toLowerCase().contains("academic")){

							// ageValue="low";

							// }

							if (category.toLowerCase().contains("baby") && !category.toLowerCase().contains("toy"))
								ageValue = "medium";

							if (tag.toLowerCase().contains("baby") && !tag.toLowerCase().contains("toy"))
								ageValue = "medium";

							if (category.toLowerCase().contains("toy"))
								ageValue = "low";

							if (tag.toLowerCase().contains("girl"))
								ageValue = "low";

							if (tag.toLowerCase().contains("boy"))
								ageValue = "low";

							if (refurl.toLowerCase().contains("girl"))
								ageValue = "low";

							if (refurl.toLowerCase().contains("boy"))
								ageValue = "low";

							if (agegroup != null) {
								agegroup1 = agegroup.split(":");

								for (int i = 0; i < agegroup1.length; i++) {

									if (agegroup1[i] != null && !agegroup1[i].isEmpty()) {

										if (agemap.containsKey(agegroup1[i]) == false) {
											agemap.put(agegroup1[i], 1);

										} else {

											count = agemap.get(agegroup1[i]);

											agemap.put(agegroup1[i], count + 1);

										}

									}
								}
							}

						}

					} catch (Exception e) {

						e.printStackTrace();
						continue;

					}

					agegroup = null;

				}

				int genderSignal = 0;
				int incomeSignal = 0;
				int ageSignal = 0;

				if (gendermap != null && gendermap.isEmpty() == false) {

					genderSignal = (Collections.max(gendermap.values()));
				}

				if (incomemap != null && incomemap.isEmpty() == false) {

					incomeSignal = (Collections.max(incomemap.values()));
				}

				if (agemap != null && agemap.isEmpty() == false) {
					ageSignal = (Collections.max(agemap.values()));
				}
				// Look for any peak signals
				// For example - in classification of gender to female -
				// look for signal
				// beauty or clothes and Accessories, if count > threshold
				// classify
				// directly

				String genderkey;
				String incomekey;
				String agegroupkey;

				if (gendermap != null && gendermap.isEmpty() == false) {

					for (Entry<String, Integer> entry : gendermap.entrySet()) { // Iterate
																				// through
						// hashmap
						genderkey = getDemographicsFullDataorInterval.getKeys(gendermap, genderSignal);
						if (genderValue == null || genderValue.isEmpty() == true)
							genderValue = genderkey; // Print the key
														// value

					}
				}

				if (incomemap != null && incomemap.isEmpty() == false) {

					for (Entry<String, Integer> entry : incomemap.entrySet()) { // Iterate
																				// through
																				// hashmap
						incomekey = getDemographicsFullDataorInterval.getKeys(incomemap, incomeSignal);

						if (incomeValue == null || incomeValue.isEmpty() == true)
							incomeValue = incomekey;// Print the key
													// value

					}

				}

				if (agemap != null && agemap.isEmpty() == false) {

					for (Entry<String, Integer> entry : agemap.entrySet()) { // Iterate
																				// through
																				// hashmap
						agegroupkey = getDemographicsFullDataorInterval.getKeys(agemap, ageSignal);

						if (ageValue == null || ageValue.isEmpty() == true)
							ageValue = agegroupkey; // Print the key
													// value

					}

				}

				System.out.println("Gender:" + startdate + ":" + enddate + genderValue);
				// System.out.println("Gender:" + genderValue);
				// System.out.println("Agegroup:" + ageValue);
				// System.out.println("IncomeLevel:" + incomeValue);

				String agegroup2[] = { "13_17", "18_24", "18_24" };
				String agegroup3[] = { "25_34", "25_34", "25_34", "18_24", "35_44" };
				String agegroup4[] = { "45_54", "55_64", "45_54", "45_54", "55_64", "65_more" };
				String agegroup5[] = { "45_54", "35_44" };
				// optimisation for demography data - Get Demography data from quantcast for the
				// website (If demography can't be derived using rules)
				// Use age Group which is majority for the website in more quantity below array
				String agegroup6[] = { "18_24", "25_34", "35_44", "25_34", "25_34", "18_24", "Undetermined" };

				if (ageValue != null && ageValue.isEmpty() == false) {
					// ageValue =
					// ageValue.replace("low","13171824").replace("medium","25343544").replace("high","4554556465more");

					if (ageValue.equals("low")) {

						int idx1 = new Random().nextInt(agegroup2.length);
						ageValue = agegroup2[idx1];

					}

					if (ageValue.equals("medium")) {
						int idx2 = new Random().nextInt(agegroup3.length);
						ageValue = agegroup3[idx2];
					}

					if (ageValue.equals("high")) {

						int idx3 = new Random().nextInt(agegroup4.length);

						ageValue = agegroup4[idx3];
					}

					if (ageValue.equals("highmedium")) {
						int idx4 = new Random().nextInt(agegroup5.length);

						ageValue = agegroup5[idx4];

					}

				}

				if (ageValue == null || ageValue.isEmpty()) {

					int idx5 = new Random().nextInt(agegroup6.length);

					ageValue = agegroup6[idx5];

				}
				// optimisation for demography data - Get Demography data from quantcast for the
				// website (If demography can't be derived using rules)
				// Use gender which is majority for the website in more quantity below array
				String gender1[] = { "male", "male", "male", "Undetermined", "Undetermined" };

				if (genderValue == null || genderValue.isEmpty()) {

					int idx6 = new Random().nextInt(gender1.length);

					genderValue = gender1[idx6];

				}
				// optimisation for demography data - Get Demography data from quantcast for the
				// website (If demography can't be derived using rules)
				// Use income level which is majority for the website in more quantity below
				// array
				// More efficient optimisation will be to integrate gender category as derived
				// from word vectors of article and aggregate (it is described in more detail in
				// existing article, it can be experimented with)
				String income1[] = { "low", "medium", "medium", "medium", "high" };

				if (incomeValue == null || incomeValue.isEmpty()) {

					int idx7 = new Random().nextInt(income1.length);

					incomeValue = income1[idx7];

				}

				SearchHit[] results4 = IndexCategoriesData2.searchDocumentContent(client, "enhanceduserdatabeta1",
						"core2", "cookie_id", fingerprintId);

				for (SearchHit hit0 : results4) {

					String id1 = hit0.getId();

					IndexCategoriesData.updateDocumentv1(client, "enhanceduserdatabeta1", "core2", id1, "gender",
							"agegroup", "incomelevel", genderValue, ageValue, incomeValue);

				}

				gendermap.clear();
				incomemap.clear();
				agemap.clear();

				genderValue = null;
				ageValue = null;
				incomeValue = null;

			}
		} catch (NoNodeAvailableException e) {

			try {
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (Exception e) {

			e.printStackTrace();
			// continue;

		}

		finally {
			// bulkProcessor.close();
		}
		return "";
	}

}
