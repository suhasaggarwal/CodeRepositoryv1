package com.publisherdata.Daos;

import com.publisherdata.model.Article;
import com.publisherdata.model.PublisherReport;
import util.EhcacheKeyCodeRepository;
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

			
/*		
			Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "personaq").build();
			client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("192.168.106.118", 9300))
					.addTransportAddress(new InetSocketTransportAddress("192.168.106.119", 9300))
					.addTransportAddress(new InetSocketTransportAddress("192.168.106.120", 9300));
*/         
		
			
			Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();

			client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
         
         
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

	public List<String> getCookieList(String startdate, String enddate, String channel_name )
			throws CsvExtractorException, Exception {

		String query = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '"
				+ channel_name + "' and request_time between '" + startdate + "'" + " and " + "'" + enddate
				+ "' group by cookie_id limit 2000000";
		System.out.println(query);
		CSVResult csvResult = getCsvResult(false, query);
		List<String> headers = csvResult.getHeaders();
		List<String> lines = csvResult.getLines();
		List<String> cookieList = new ArrayList();
		if (lines.size() > 0) {
			for (int i = 0; i < lines.size(); i++) {
				cookieList.add(lines.get(i));
			}
		}

		return cookieList;
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
		//System.out.println("Ehcache:"+ehcache);
		Object execution = null;
	
        execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);

        CSVResult results = new CSVResultsExtractor(includeScore, includeType).extractResults(execution, flat, ",");
			//System.out.println("Result1:"+results);
		return results;
		

		
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
