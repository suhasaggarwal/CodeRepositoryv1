package com.spidio.DataLogix.web;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.URL;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
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

public class ComputeEngagementTime {
	private static final long serialVersionUID = 1L;

	public static Map<String, Boolean> cookieSet = new ConcurrentHashMap<String, Boolean>(2000000);
	static Integer recordCount = 0;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/**
		 * This method returns ESclient instance.
		 *
		 * @return the es client ESClient instance
		 */

		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "personaq").build();

		Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("192.168.106.118", 9300))
		                                             .addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300))
		                                             .addTransportAddress(new InetSocketTransportAddress("192.168.106.120", 9300));
		
		String refurl = null;
		String title = null;
		String[] parts = null;
		String[] parts1 = null;
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

		Map<String, Object> result0;
		Map<String, Object> result;
		Map<String, Object> result1;
		Map<String, Object> result2;

		String startDate = args[0];

		String endDate = args[1];
		
		String channel_name = args[2];
/*
		SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta1").setTypes("core2")
				.setSearchType(SearchType.QUERY_AND_FETCH).setScroll(new TimeValue(600000))
				.setPostFilter(FilterBuilders.rangeFilter("request_time").from(startDate).to(endDate)).setSize(1000)
				.execute().actionGet();
*/
/*		
		SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta1").setScroll(new TimeValue(600000))
				.setQuery(QueryBuilders.matchQuery("channel_name", "ITWEBEN")).setSize(1000).execute().actionGet();
*/
		
		SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta1").setScroll(new TimeValue(600000)).setTypes("core2")
				    .setFetchSource(new String[]{"cookie_id"}, null)  
				    .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchQuery("channel_name", channel_name),
					FilterBuilders.rangeFilter("request_time").from(startDate).to(endDate).cache(false))).setSize(1000).execute().actionGet();
		
		
		// Scroll until no hits are returned
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 1);
		List<Callable<String>> lst = new ArrayList<Callable<String>>();
		try {
			System.setOut(new PrintStream(new File("ComputeEngagementTimeLogData.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {

			while (true) {
				// Scroll until no hits are returned

				for (SearchHit hit : response1.getHits().getHits()) {
					lst.add(new WorkerThread(hit,client,startDate,endDate));
					// Break condition: No hits are returned
				}
				// System.out.println("Number of scrolls:"+count3);
				List<Future<String>> maps = null;

				try {
					maps = executor.invokeAll(lst, 10, TimeUnit.MINUTES);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println("Response Size:" + response1.getHits());

				// System.out.println("Number of scrolls:"+count3)
				response1 = client.prepareSearchScroll(response1.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				lst.clear();
				maps.clear();

				if (response1.getHits().getHits().length == 0) {
					break;
				}

				recordCount++;
				System.out.println("RecordCount" + startDate + ":"+ endDate + recordCount);
			}

		} catch (Exception e1) {

			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {

			executor.shutdown();
			client.close();

		}

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
