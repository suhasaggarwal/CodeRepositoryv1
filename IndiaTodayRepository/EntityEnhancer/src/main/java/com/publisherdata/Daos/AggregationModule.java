package com.publisherdata.Daos;

import com.publisherdata.model.Article;
import com.publisherdata.model.PublisherReport;
import com.spidio.UserSegmenter.Entity;

import util.EhCacheKeyCodeRepository;
import util.EhCacheKeyCodeRepository1;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

public class AggregationModule {
	private static TransportClient client;
	private static SearchDao searchDao;
	private static AggregationModule INSTANCE;

	public static AggregationModule getInstance() {
		if (INSTANCE == null) {
			return new AggregationModule();
		}
		return INSTANCE;
	}

	public static void main(String[] args) throws Exception {
//AggregationModule mod = new AggregationModule();
		// mod.setUp();
//	mod.getProductData("lenskart","https://www.lenskart.com/ray-ban-rb3025-001-57-size-58-golden-brown-aviator-polarized-sunglasses.html");

	}

	public void setUp() throws Exception {
		if (client == null) {

		//	Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "personaq").build();
			Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
			client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
				//	.addTransportAddress(new InetSocketTransportAddress("192.168.106.118", 9300))
				//	.addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300))
				//	.addTransportAddress(new InetSocketTransportAddress("192.168.106.120", 9300));

			NodesInfoResponse nodeInfos = (NodesInfoResponse) client.admin().cluster().prepareNodesInfo(new String[0])
					.get();
			String clusterName = nodeInfos.getClusterName().value();
			System.out.println(String.format("Found cluster... cluster name: %s", new Object[] { clusterName }));

			searchDao = new SearchDao(client);
		}
		System.out.println("Finished the setup process...");
	}

	public static SearchDao getSearchDao() {
		return searchDao;
	}

	public List<String> getArticleList(String channel_name, String startdate, String enddate)
			throws CsvExtractorException, Exception {

		String query = "SELECT DISTINCT(refcurrentoriginal) FROM enhanceduserdatabeta1 where channel_name = '"
				+ channel_name + "' and request_time between '" + startdate + "'" + " and " + "'" + enddate
				+ "' group by refcurrentoriginal limit 20000000";
		System.out.println(query);
		CSVResult csvResult = getCsvResult(false, query);
		List<String> headers = csvResult.getHeaders();
		List<String> lines = csvResult.getLines();
		List<String> articleList = new ArrayList();
		if (lines.size() > 0) {
			for (int i = 0; i < lines.size(); i++) {
				System.out.println(lines.get(i));
				articleList.add(lines.get(i));
			}
		}

		return articleList;
	}
	
	
	public List<String> getSortedArticleList(String channel_name, String startdate, String enddate)
			throws CsvExtractorException, Exception {

		String query = "Select count(*),refcurrentoriginal from enhanceduserdatabeta1 where channel_name = '"
				+ channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'"
				+ " GROUP BY refcurrentoriginal limit 200";
		System.out.println(query);
		CSVResult csvResult = getCsvResult(false, query);
		List<String> headers = csvResult.getHeaders();
		List<String> lines = csvResult.getLines();
		List<String> articleList = new ArrayList();
		if (lines.size() > 0) {
			for (int i = 0; i < lines.size(); i++) {
				System.out.println("Article Url:"+lines.get(i).trim());
				articleList.add(lines.get(i).split(",")[0].trim());
			}
		}

		return articleList;
	}
	

	public boolean SearchArticle(String ArticleUrl) throws CsvExtractorException, Exception {

		String query = "SELECT * FROM entity where Entity1 = '" + ArticleUrl + "'";
		System.out.println(query);
		CSVResult csvResult = getCsvResult(false, query);
		List<String> headers = csvResult.getHeaders();
		List<String> lines = csvResult.getLines();
		List<String> articleList = new ArrayList();
		if (lines.size() > 0) {

			return true;

		}

		return false;

	}

	public Entity getProductData(String channel_name, String ArticleUrl) throws CsvExtractorException, Exception {

		String query = "SELECT Entity1,Entity2,Entity3,Entity4,Entity5,Entity6,Entity7,Entity8,Entity9,Entity10,Entity11,Entity12 FROM entity where Entity1  = '"
				+ ArticleUrl
				+ "'";
		// String query = "SELECT
		// Entity1,Entity2,Entity3,Entity4,Entity5,Entity6,Entity8,Entity9,Entity10,Entity11,Entity12
		// FROM entity where Entity1 Like '%"+ArticleUrl+"%'";
		System.out.println(query);
		CSVResult csvResult = getCsvResult(false, query);
		System.out.println("CSVResult:"+csvResult);
		List<String> headers = null;
		List<String> lines = null;
		if (csvResult != null) {
		headers = csvResult.getHeaders();
		lines = csvResult.getLines();
		}
		List<String> articleList = new ArrayList();
		Entity obj = new Entity();
		System.out.println(query);
    
			if ((lines != null) && (!lines.isEmpty()) && (!((String) lines.get(0)).isEmpty())) {
				System.out.println("Lines:"+lines);
			    
				String[] data = ((String) lines.get(0)).replace(",,",",Undetermined,").split(",");
				System.out.println("Data:"+ data);
				
	              for (int i=0; i<headers.size();i++) {
	            	  System.out.println("Headers:"+ headers);
	  					
	            	  
					if (data.length > 0 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity1"))
						obj.setEntity1(data[i]);

					if (data.length > 1 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity2"))
						obj.setEntity2(data[i]);

					if (data.length > 2 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity3"))
						obj.setEntity3(data[i]);

					if (data.length > 3 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity4")) 
						obj.setEntity4(data[i]);

					if (data.length > 4 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity5"))
						obj.setEntity5(data[i]);

					if (data.length > 5 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity6"))
						obj.setEntity6(data[i]);

					if (data.length > 6 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity7"))
						obj.setEntity7(data[i]);

					if (data.length > 7 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity8"))
						obj.setEntity8(data[i]);

					if (data.length > 8 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity9"))
						obj.setEntity9(data[i]);

					if (data.length > 9 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity10"))
						obj.setEntity10(data[i]);

					if (data.length > 10 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity11"))
						obj.setEntity11(data[i]);
					
					if (data.length > 11 && data[i] != null && !data[i].isEmpty() && headers.get(i).equals("Entity12"))
						obj.setEntity12(data[i]);

				}

			}
			 System.out.println("Product2:"+obj);
			return obj;

	//	}
		
	//	return object;
	}

	private static CSVResult getCsvResult(boolean flat, String query)
			throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
		return getCsvResult(flat, query, false, false);
	}

	private static CSVResult getCsvResult(boolean flat, String query, boolean includeScore, boolean includeType)
			throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
		SearchDao searchDao = getSearchDao();
		QueryAction queryAction = searchDao.explain(query);
		// Caches Elasticsearch Queries in Ehcache, this saves lot of latencies for
		// fetching data from the Network
		// Key,Value Format(Corresponding SQL query Equivalent, Query Result)
		// To scale up - Do vertical scaling for ehcache, use horizontal scaling for
		// distributed Ehcache set up, if there are multiple Publisher Dashboard nodes
		// but it is not applicable at present as we use single machine only - but it
		// will add to network latencies
		EhCacheKeyCodeRepository ehcache = EhCacheKeyCodeRepository.getInstance();
		System.out.println("Ehcache:"+ehcache);
		Object execution = null;
		CSVResult results = ehcache.get(query, false);
		// CSVResult results = null;
		if (results == null || results.equals("")) {

			execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);

			results = new CSVResultsExtractor(includeScore, includeType).extractResults(execution, flat, ",");
			System.out.println("Result1:"+results);
			if (results != null) {
			ehcache.put(query, results);
			}
			return results;
		}

		else {

			return results;

		}
	}

	public static void sumTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
	}

	private static Aggregations query(String query) throws SqlParseException, SQLFeatureNotSupportedException {
		SqlElasticSearchRequestBuilder select = getSearchRequestBuilder(query);
		return ((SearchResponse) select.get()).getAggregations();
	}

	private static SqlElasticSearchRequestBuilder getSearchRequestBuilder(String query)
			throws SqlParseException, SQLFeatureNotSupportedException {
		SearchDao searchDao = getSearchDao();
		return (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
	}

	private static InetSocketTransportAddress getTransportAddress() {
		String host = System.getenv("ES_TEST_HOST");
		String port = System.getenv("ES_TEST_PORT");
		if (host == null) {
			host = "localhost";
			System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default '172.16.105.231'");
		}
		if (port == null) {
			port = "9300";
			System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
		}
		System.out.println(String.format("Connection details: host: %s. port:%s.", new Object[] { host, port }));
		return new InetSocketTransportAddress(host, Integer.parseInt(port));
	}
}
