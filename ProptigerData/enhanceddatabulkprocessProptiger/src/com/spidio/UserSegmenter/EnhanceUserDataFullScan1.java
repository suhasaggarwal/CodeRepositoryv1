package com.spidio.UserSegmenter;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.spidio.dataModel.DeviceObject;
import com.spidio.dataModel.LocationObject;

public class EnhanceUserDataFullScan1 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Client client = new TransportClient()
				.addTransportAddress(new InetSocketTransportAddress(
						"52.22.164.163", 9300));

		String keywords = null;
		String description = null;
		String[] searchkeyword = null;
		String[] finalsearchkeyword = null;
		SearchHit[] matchingsegmentrecords = null;
		String category = null;
		String subcategory = null;
		DeviceObject deviceProperties = null;
		LocationObject locationProperties = null;

		// SearchHit[] results =
		// IndexCategoriesData.searchEntireUserData(client,
		// "dmpuserdatabase","core2");

		SearchResponse response = client.prepareSearch("userprofilestore")
				.setTypes("core2").setSearchType(SearchType.QUERY_THEN_FETCH)
				.setScroll(new TimeValue(600000))
				.setQuery(QueryBuilders.matchAllQuery()).setSize(10).execute()
				.actionGet();
		// Scroll until no hits are returned
		while (true) {

			for (SearchHit hit : response.getHits().getHits()) {

				System.out.println("------------------------------");
				Map<String, Object> result = hit.getSource();
				String refurl = (String) result.get("referrer");
				String documentId = (String) result.get("_id");
				String ip = (String) result.get("ip");
				String userAgent = (String) result.get("br_user_agent");
				deviceProperties = ProcessDeviceData
						.getDeviceDetails(userAgent);
				locationProperties = ProcessIPAddress.getIPDetails(ip);
				// keywords = ProcessRefurl.getKeywords(refurl);
				// description = ProcessRefurl.getDescription(refurl);
				Map<String, String> data = new HashMap<String, String>();
				// if(keywords != null && !keywords.isEmpty())
				// data.put("keywords",keywords);
				data.put("city", locationProperties.getCity());
				data.put("country", locationProperties.getCountry());
				data.put("latitude", locationProperties.getLatittude()
						.toString());
				data.put("longitude", locationProperties.getLongitude()
						.toString());
				data.put("postalcode", locationProperties.getPostalCode());
				data.put("brandName", deviceProperties.getBrandName());
				data.put("modelName", deviceProperties.getModel_name());
				data.put("screen_width",
						deviceProperties.getPhysical_screen_width());
				data.put("screen_height",
						deviceProperties.getPhysical_screen_height());
				data.put("resolution_width",
						deviceProperties.getResolution_width());
				data.put("resolution_height",
						deviceProperties.getResolution_height());
				data.put("isWireless", deviceProperties.getWireless_device());

				client.prepareUpdate("enhanceduserprofilestore", "core2",
						documentId).setDoc(data).setRefresh(true).execute()
						.actionGet();

			}
			response = client.prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(600000)).execute().actionGet();
			// Break condition: No hits are returned

			// System.out.println("Number of scrolls:"+count3);

			if (response.getHits().getHits().length == 0) {
				break;
			}

		}

	}

}
