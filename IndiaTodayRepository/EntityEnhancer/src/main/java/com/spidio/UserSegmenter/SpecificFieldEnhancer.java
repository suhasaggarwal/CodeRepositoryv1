package com.spidio.UserSegmenter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import util.GetMiddlewareData;

import com.publisherdata.Daos.AggregationModule;
import com.spidio.dataModel.DeviceObject;

public class SpecificFieldEnhancer {

	  public static Set<String> refurlset = new HashSet<String>(1000000);
	//public static Set<String> documentIds = new HashSet<String>(1000000);
	//public static Map<String, Boolean> cookieset = new ConcurrentHashMap<String, Boolean>(2000000);
	
	
	public static void main(String[] args) throws Exception {

		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		     //   .addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300))
	         //   .addTransportAddress(new InetSocketTransportAddress("192.168.106.120", 9300));

		String keywords = null;
		String description = null;
		String[] searchkeyword = null;
		String[] finalsearchkeyword = null;
		SearchHit[] matchingsegmentrecords = null;
		String city = null;
		String isp = null;
		String org = null;
		String mobiledevice = null;
		String os = null;
		String browser = null;
		String country = null;
		String userAgent = null;
		Integer recordCount = 0;
		DeviceObject deviceProperties = null;

		try {
			System.setOut(new PrintStream(new File("EntityEnhancerLogDataV16.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}

		
		String startDate = args[0];
		String endDate = args[1];
		String channel_name = args[2];
	 
	/*	
		String startDate = "2020-04-03 00:00:01";
	    String endDate = "2020-04-03 23:59:59";
    */		
		SearchResponse response1 = client.prepareSearch("enhanceduserdatabeta1").setScroll(new TimeValue(600000)).setTypes("core2")
				.setFetchSource(new String[]{"refcurrentoriginal"}, null)       
				.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchQuery("channel_name", channel_name),
						FilterBuilders.rangeFilter("request_time").from(startDate).to(endDate).cache(false))).setSize(1000).execute().actionGet();

		// Scroll until no hits are returned

		BufferedReader br = null;

		String authorName = null;
		FileReader fr = null;

		AggregationModule mod = AggregationModule.getInstance();

		try {
			mod.setUp();
		} catch (Exception e1) {

			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 1);
		List<Callable<String>> lst = new ArrayList<Callable<String>>();
		// ExecutorService executor = Executors.newFixedThreadPool(2);

		
		
		// BulkProcessor bulkprocessor = BulkProcessorModule.GetInstance(client);

		try {

			while (true) {
				// Scroll until no hits are returned
				try {

					for (SearchHit hit : response1.getHits().getHits()) {
						lst.add(new WorkerThread(hit, client, channel_name, mod, startDate, endDate));
					}
					// Break condition: No hits are returned }

					List<Future<String>> maps = executor.invokeAll(lst, 10, TimeUnit.MINUTES);

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
					System.out.println("RecordCount" + recordCount);

				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
				// Thread.sleep(100);
			}

		} catch (Exception e1) {

			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {

			System.out.println("FinalRecordCount" + recordCount);
			executor.shutdown();
			client.close();
		}

	}

}