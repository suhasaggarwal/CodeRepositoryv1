package com.spidio.DataLogix.web;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.xml.sax.InputSource;

import com.spidio.UserSegmenter.IndexCategoriesData;
import com.spidio.UserSegmenter.IndexCategoriesData1;
import com.spidio.UserSegmenter.IndexCategoriesData2;
import com.spidio.UserSegmenter.ProcessRefurl;
import com.spidio.UserSegmenter.TitleExtractor;
import com.spidio.UserSegmenter.TitleExtractorRegex;

public class WorkerThread implements Callable<String> {

	SearchHit hit;
	Client client;

	String id = null;

	Integer count = 0;
	Integer flag = 0;
	Integer count1 = 0;
	// String id;
	Integer count2 = 0;
	Integer count3 = 0;
	String channelName = null;
	// Data is copied in elasticsearch every 5 minutes, this module scans
	// elasticsearch records for past 5 minutes and classifies them in to
	// categories and corresponding subcategories
	Integer flag1 = 0;

	Integer flag2 = 0;

	Integer flag3 = 0;

	String fingerprintId = null;

	String request_time = null;

	String refurl = null;

	String title = null;

	String rt1 = null;

	String rt2 = null;

	String startDate;
	String endDate;

	Timestamp timestamp = null;
	Timestamp timestamp1 = null;

	Map<String, Object> result0;
	Map<String, Object> result;
	Map<String, Object> result1;
	Map<String, Object> result2;
	List<String> Id = new ArrayList<String>();
	List<String> requesttime = new ArrayList<String>();

	public WorkerThread(SearchHit hit, Client client, String startDate, String endDate) {
		this.hit = hit;
		this.client = client;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	// Configure Time range here for records enhancement
	// Ideal Time configuration for Engagement Time Module
	// Scroll until no hits are returned

	@Override
	public String call() {

		result = hit.getSource();
		fingerprintId = (String) result.get("cookie_id");

		// if (ComputeEngagementTime.cookieSet.containsKey(fingerprintId) == false) {

		if (ComputeEngagementTime.cookieSet.put(fingerprintId, true) == null) {

			SearchHit[] results = IndexCategoriesData2.searchDocumentContent(client, "enhanceduserdatabeta1", "core2",
					"cookie_id", fingerprintId);

			for (SearchHit hit0 : results) {

				try {

					Map<String, Object> result0 = hit0.getSource();
					refurl = (String) result0.get("referrer");

					title = (String) result0.get("page_title");
					id = (String) hit0.getId();

					request_time = (String) result0.get("request_time");

					// String gender1 = (String)result0.get("gender");
					// System.out.println("Test index"+gender);
					channelName = (String) result0.get("channel_name");

					Id.add(id);

					requesttime.add(request_time);

				} catch (Exception e) {

					e.printStackTrace();
					continue;

				}

			}

			for (int i = 0; i < requesttime.size(); i++) {

				try {

					Long diffminutes = new Long(0);

					if (i != (requesttime.size() - 1)) {
						rt1 = requesttime.get(i);
						rt2 = requesttime.get(i + 1);

						try {
							SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
							Date parsedDate = dateFormat.parse(rt1);
							timestamp = new java.sql.Timestamp(parsedDate.getTime());
						} catch (Exception e) { // this generic but you can control another types of exception
							// look the origin of excption
						}

						try {
							SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
							Date parsedDate1 = dateFormat1.parse(rt2);
							timestamp1 = new java.sql.Timestamp(parsedDate1.getTime());
						} catch (Exception e) { // this generic but you can control another types of exception
							// look the origin of excption
						}

						diffminutes = compareTwoTimeStamps(timestamp1, timestamp);

					} else {
						Random rn = new Random();
						int time = rn.nextInt(2) + 1;
						diffminutes = (long) time;

					}

					if (diffminutes < 0) {

						diffminutes = (long) 1;

					}

					requesttime.set(i, diffminutes.toString());

				} catch (Exception e) {

					e.printStackTrace();
					continue;

				}

			}

			for (int i = 0; i < Id.size(); i++) {

				if (Id.get(i) != null && Id.get(i).isEmpty() == false) {

					if (requesttime.get(i) != null && requesttime.get(i).isEmpty() == false) {

						if (Long.parseLong(requesttime.get(i)) < 0) {

							requesttime.set(i, new Long(1).toString());

						}

						try {
							IndexCategoriesData.updateDocument1(client, "enhanceduserdatabeta1", "core2", Id.get(i),
									"engagementTime", Long.parseLong(requesttime.get(i)));

							System.out.println("Measurement:" + Id.get(i) + ":" + requesttime.get(i));
						}

						catch (NoNodeAvailableException e) {

							try {
								Thread.sleep(10000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}

						catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}

			Id.clear();
			requesttime.clear();
		}

		return "";
	}

	public static long compareTwoTimeStamps(java.sql.Timestamp currentTime, java.sql.Timestamp oldTime) {
		long milliseconds1 = oldTime.getTime();
		long milliseconds2 = currentTime.getTime();

		long diff = milliseconds2 - milliseconds1;
		long diffSeconds = diff / 1000;
		long diffMinutes = diff / (60 * 1000);
		long diffHours = diff / (60 * 60 * 1000);
		long diffDays = diff / (24 * 60 * 60 * 1000);

		if (diffMinutes > 30) {
			Random rn = new Random();
			int time = rn.nextInt(5) + 1;
			diffMinutes = (long) time;
		}

		if (diffMinutes == 0) {
			Random rn = new Random();
			int time = 1;
			diffMinutes = (long) time;
		}

		if (diffMinutes < 0) {

			diffMinutes = 1;

		}
		return diffMinutes;
	}

	public static String getKeys(Map<String, Integer> map, Integer value) {

		String key = "";
		Integer count = 0;

		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			if (entry.getValue().equals(value)) {
				if (count == 0)
					key = entry.getKey();
				else {
					if (key.contains(entry.getKey()) == false)
						key = key + entry.getKey();
				}
				count++;
			}
		}

		return key;

	}

}