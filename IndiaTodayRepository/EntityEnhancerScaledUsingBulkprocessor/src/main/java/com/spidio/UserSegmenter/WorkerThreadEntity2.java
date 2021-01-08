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

public class WorkerThreadEntity2 implements Callable<String> {

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
	String entity8 = null;
	String entity10 = null;
	String newTime;
	String publishDate;
	String section;
	AggregationModule mod = null;
	String mappedurl = "";
	String refurl = "";

	public WorkerThreadEntity2(Client client, String refurl, String entity8, String entity10) {
		this.client = client;
		this.refurl = refurl;
		this.entity8 = entity8;
		this.entity10 = entity10;
	}

	@Override
	public String call() {
		// Handle the hit...

		String srcIP = "";
		Integer tracker = 0;
		String ES_INDEX = "entity";
		try {

			// System.out.println("------------------------------");
			if (refurl != null && !refurl.equals("") && SpecificFieldEnhancer.refurlset.contains(refurl) == false) {
                System.out.println(refurl);
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

				SearchHit[] results = IndexCategoriesData2.searchDocumentContentEntity(client, "entity", "core2",
						"Entity1", refurl);

				for (SearchHit hit0 : results) {

					Map<String, Object> resultv1 = hit0.getSource();
					if (tracker != 0) {
						String documentId = hit0.getId();
						try {

							IndexCategoriesData.doDelete(client, "entity", "core2", documentId);

							System.out.println("RecordProcessed");

						} catch (NoNodeAvailableException e) {

							Thread.sleep(10000);
						} catch (Exception e) {
							e.printStackTrace();
							continue;
						}
					}

					tracker++;
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return "";

	}
}
