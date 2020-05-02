package com.spidio.UserSegmenter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.Map.Entry;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import util.GetMiddlewareData;

import com.publisherdata.Daos.AggregationModule;
import com.spidio.UserSegmenter.IndexCategoriesData;
import com.spidio.UserSegmenter.IndexCategoriesData2;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class WorkerThread implements Callable<String> {

	SearchHit hit;
	Client client;
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

	Map<String, String> categorysubcategoryMap = new HashMap<String, String>();

	String[] agegroup1 = null;

	String ecommerceCategory = "";

	String tag = "";

	Integer price = 0;

	Long handsetPrice = new Long(0);

	String price_range = "";

	String refurlv1 = "";

	Map<String, Object> result0;
	Map<String, Object> result;
	Map<String, Object> result1;
	Map<String, Object> result2;
	DateFormat df;
	Date startDatev2;
	Calendar calendar;
	String categoryv1 = null;
	String tags = null;
	String author = null;
	String audienceSegmentv1 = null;
	String startdate = null;
	String enddate = null;
	String newTime;
	String publishDate;
	String section;
	AggregationModule mod = null;
	String mappedurl = "";

	public WorkerThread(SearchHit hit, Client client, String channel_name, AggregationModule mod, String startdate,
			String enddate) {
		this.hit = hit;
		this.client = client;
		this.channelName = channel_name;
		this.startdate = startdate;
		this.enddate = enddate;
		this.mod = mod;
	}

	@Override
	public String call() {
		// Handle the hit...

		String srcIP = "";
		String ES_ID = this.hit.getId();
		String ES_INDEX = "enhanceduserdatabeta1";
		try {

			// System.out.println("------------------------------");

			Map<String, Object> result = hit.getSource();
			String refurl = null;
			if (result != null) {
				refurl = (String) result.get("refcurrentoriginal");
				// System.out.println(refurl);
			}

			if (refurl != null && !refurl.equals("") && SpecificFieldEnhancer.refurlset.contains(refurl) == false) {

				SpecificFieldEnhancer.refurlset.add(refurl);

				// fingerprintId = "728088282.2167482368.1716827031";

				// System.out.println(refurl);
				// String [] fingerprintSegments = fingerprintId.split("\\.");
				// System.out.println(fingerprintSegments[0]+":"+fingerprintSegments[1]+":"+fingerprintSegments[2]+":"+fingerprintSegments[3]);
				// if(fingerprintSegments.length > 1)
				// fingerprintId =
				// fingerprintSegments[0].trim()+"."+fingerprintSegments[1].trim()+"."+fingerprintSegments[2].trim();
				// fingerprintId = fingerprintId.trim();

				// Store/Site specific Categorisation

				SearchHit[] results = IndexCategoriesData2.searchDocumentContent(client, "enhanceduserdatabeta1",
						"core2", "refcurrentoriginal", refurl, startdate, enddate);

				Entity product = new Entity();

				product = GetMiddlewareData.getEntityData(channelName, mod, refurl.split("\\?")[0]);

				for (SearchHit hit0 : results) {

					Map<String, Object> resultv1 = hit0.getSource();
					String authorName = null;
					if (resultv1 != null) {
						authorName = (String) resultv1.get("authorName");
						// System.out.println(authorName);
					}
					String documentId = hit0.getId();
					// String url = (String) result.get("refcurrentoriginal");
					// String authorName = (String) result.get("authorName");

					if (authorName == null || authorName.isEmpty()) {
						try {
                            System.out.println("Product:"+product);   
							if (product.getEntity4() != null && !product.getEntity4().isEmpty()) {
								author = product.getEntity4().split(";")[0];
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "authorName", author);
								// System.out.println("author" + ":" + author);
							} else {
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "authorName", "Undetermined");
								author = "Undetermined";
								// System.out.println("author" + ":" + author);

							}
							// Tag Enhancement
							// Does data point processing before indexing in ES (Tags scraped via crawler
							// can contain special characters, this is done so that ES query to fetch entity
							// (AggregationModule.getProductData is used to fetch Entities)
							// does not break and all entities are fetched correctly from index
							if (product.getEntity6() != null && !product.getEntity6().isEmpty()
									&& !product.getEntity6().equals("Undetermined")) {
								tag = product.getEntity6().replace(";", ",").replaceAll("[\\r]+[\\t]+[\\n]+", "")
										.replace("'", "").replace("\\", "").replace("/", "");
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "tag", tag);
								// System.out.println("tag" + ":" + tag);
							} else {
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "authorName", "Undetermined");
								tag = "";
								// System.out.println("tag" + ":" + tag);

							}

							if (product.getEntity5() != null && !product.getEntity5().isEmpty()
									&& !product.getEntity5().equals("Undetermined")) {
								// Publish Date Enhancement
								// Split out timestamp as that is not needed in publish date
								String[] publishdate1 = product.getEntity5().split("T");
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "publishDatev1", publishdate1[0]);
								publishDate = publishdate1[0];
								// System.out.println("publishDate" + ":" + product.getEntity5());

							} else {
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "authorName", "Undetermined");
								publishDate = "2000-01-01";
								// System.out.println("publishDate" + ":" + publishDate);

							}

							if (product.getEntity12() != null && !product.getEntity12().isEmpty()
									&& !product.getEntity12().equals("Undetermined")) {

								section = product.getEntity12();
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "section", product.getEntity12());
								// System.out.println("section" + ":" + product.getEntity12());

							} else {
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "section", "");
								section = "";
								// System.out.println("section" + ":" + "Undetermined");

							}

							if (product.getEntity8() != null && !product.getEntity8().isEmpty()
									&& !product.getEntity8().equals("Undetermined")) {

								/*
								 * String [] categories = product.getEntity5().split(">");
								 * 
								 * String section = categories[2];
								 * 
								 * 
								 * String leafCategory = "";
								 * 
								 * if(categories.length == 3) leafCategory = section;
								 * 
								 * 
								 * 
								 * for(int i=3; i<categories.length;i++){
								 * 
								 * leafCategory = leafCategory+"_"+categories[i]; }
								 * 
								 * 
								 */

								categoryv1 = product.getEntity8().replaceAll("\\s+", "\\.").replaceAll(",", "\\.")
										.replaceAll(";", "\\.").replace("'", "").replace("\\", "").replace("/", "");
								audienceSegmentv1 = product.getEntity10().replaceAll("\\s+", "\\.")
										.replaceAll(",", "\\.").replaceAll(";", "\\.").replace("'", "")
										.replace("\\", "").replace("/", "");

								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "audience_segment", audienceSegmentv1);
								// leafCategory = leafCategory.substring(1,leafCategory.length());
								// System.out.println("audience segment" + ":" + audienceSegmentv1);
								// leafCategory = "_"+section+"_"+leafCategory;
								// System.out.println("subcategory" + ":" + categoryv1);
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "subcategory", categoryv1);
							} else

							{
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "audience_segment", "");
								// leafCategory = leafCategory.substring(1,leafCategory.length());
								audienceSegmentv1 = "";
								// System.out.println("audience segment" + ":" + audienceSegmentv1);
								// leafCategory = "_"+section+"_"+leafCategory;
								categoryv1 = "";
								// System.out.println("subcategory" + ":" + categoryv1);
								// IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2",
								// documentId,
								// "subcategory", "");

							}

							IndexCategoriesData.updateDocumentv1(client, "enhanceduserdatabeta1", "core2", documentId,
									"authorName", "tag", "publishDatev1", "section", "audience_segment", "subcategory",
									author, tag, publishDate, section, audienceSegmentv1, categoryv1);

							System.out.println("RecordProcessed" + "Time" + startdate + ":" + enddate + new Date());

						} catch (NoNodeAvailableException e) {

							Thread.sleep(10000);
						} catch (Exception e) {
							e.printStackTrace();
							continue;
						}

					}

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return "";

	}
}
