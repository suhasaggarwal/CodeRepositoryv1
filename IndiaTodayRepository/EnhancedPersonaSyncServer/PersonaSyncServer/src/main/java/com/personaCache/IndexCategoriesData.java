package com.personaCache;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

public class IndexCategoriesData {

	public static void main(String args[]) {
		new IndexCategoriesData();
	}

	public static Map<String, Object> putJsonDocument(String category,
			String subcategory, String content) {

		Map<String, Object> jsonDocument = new HashMap<String, Object>();

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		String postDate = dateFormat.format(date);
		jsonDocument.put("category", category);
		jsonDocument.put("subcategory", subcategory);
		jsonDocument.put("postDate", postDate);
		jsonDocument.put("content", content);

		return jsonDocument;
	}


	// If content is already there and classified, it is not further analysed

	public static SearchHit[] searchEntireUserDataSorted(Client client,
			String index, String type, String field, String value) {

		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.QUERY_AND_FETCH)
				.setQuery(QueryBuilders.matchAllQuery())
				.setFrom(0)
				.setSize(60)
				.setExplain(true)
				.addSort("postDate",
						org.elasticsearch.search.sort.SortOrder.DESC).execute()
				.actionGet();

		SearchHit[] results = response.getHits().getHits();
		//System.out.println("Current results: " + results.length);

		return results;
	}

	// Will scan entire User profile database and return records one by one

	public static SearchHit[] searchEntireUserDataDateWise(Client client,
			String index, String type, String starttime, String endTime) {

		// Sort in order of ascending time, old data get categorized first

		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.QUERY_AND_FETCH)
				.setPostFilter(
						FilterBuilders.rangeFilter("request_time")
								.from(starttime).to(endTime))
				.setQuery(QueryBuilders.matchAllQuery()
						)
				.addSort(
						SortBuilders.fieldSort("request_time").order(
								SortOrder.ASC)).setFrom(0).setSize(1000000)
				.setExplain(true).execute().actionGet();

		SearchHit[] results = response.getHits().getHits();
		//System.out.println("Current results: " + results.length);

		return results;
	}

	public static SearchHit[] searchEntireUserData(Client client, String index,
			String type) {

		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setSearchType(SearchType.QUERY_AND_FETCH)
				.setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();

		SearchHit[] results = response.getHits().getHits();
		//System.out.println("Current results: " + results.length);

		return results;
	}

	public static SearchHit[] searchDocumentContent(Client client,
			String index, String type, String field, String value) {

		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.QUERY_AND_FETCH)
				.setQuery(QueryBuilders.matchQuery(field, value))
				.addSort(
						SortBuilders.fieldSort("postDate")
								.order(SortOrder.DESC)).setSize(10).execute()
				.actionGet();

		SearchHit[] results = response.getHits().getHits();
		//System.out.println("Current results: " + results.length);

		return results;
	}

	public static IndexResponse doIndex(Client client, String index,
			String type, String id, Map<String, Object> data) {
            IndexResponse response = client.prepareIndex(index, type,id).setSource(data).get();
            return response;
	}

	public static SearchHit[] searchDocumentsimilarContent(Client client,
			String index, String type, String field, String value) {

		QueryBuilder qb = QueryBuilders.moreLikeThisQuery("content")
				.likeText(value).minTermFreq(1).maxQueryTerms(12);

		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.QUERY_AND_FETCH)
				.setQuery(qb)
				.addSort(
						SortBuilders.fieldSort("postDate")
								.order(SortOrder.DESC)).setSize(10).execute()
				.actionGet();

		SearchHit[] results = response.getHits().getHits();
		//System.out.println("Current results: " + results.length);

		return results;
	}
	
	


	public static void updateDocument(Client client, String index, String type,

	String id, Map<String, Object> enhanceddata) {

		//System.out.println("Categorisation Data Updated" + "\n");

		Map<String, Object> updateObject = enhanceddata;

	

		client.prepareUpdate(index, type, id)
				.setScript("ctx._source",
						ScriptType.INLINE).setScriptParams(updateObject)
				.execute().actionGet();

	}

}