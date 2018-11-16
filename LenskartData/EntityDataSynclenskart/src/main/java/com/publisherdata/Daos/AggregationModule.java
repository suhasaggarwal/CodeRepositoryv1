package com.publisherdata.Daos;

import com.publisherdata.model.PublisherReport;
import com.spidio.UserSegmenter.Entity;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

public class AggregationModule
{
  private static TransportClient client;
  private static SearchDao searchDao;
  private static AggregationModule INSTANCE;
  
  public static AggregationModule getInstance()
  {
    if (INSTANCE == null) {
      return new AggregationModule();
    }
    return INSTANCE;
  }
  
  public static void main(String[] args)
    throws Exception
  {
//AggregationModule mod = new AggregationModule();
	//mod.setUp();
//	mod.getProductData("lenskart","https://www.lenskart.com/ray-ban-rb3025-001-57-size-58-golden-brown-aviator-polarized-sunglasses.html");
	  
	  
  }
  
  public void setUp()
    throws Exception
  {
    if (client == null)
    {

  	  Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch").build();
   
  	  
      Client client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress(
        "localhost", 9300));
      
      
      NodesInfoResponse nodeInfos = (NodesInfoResponse)client.admin().cluster().prepareNodesInfo(new String[0]).get();
      String clusterName = nodeInfos.getClusterName().value();
      System.out.println(String.format("Found cluster... cluster name: %s", new Object[] { clusterName }));
      
      searchDao = new SearchDao(client);
    }
    System.out.println("Finished the setup process...");
  }
  
  public static SearchDao getSearchDao()
  {
    return searchDao;
  }
  
  public List<String> getArticleList(String channel_name,String startdate,String enddate)
    throws CsvExtractorException, Exception
  {
    
    String query = "SELECT DISTINCT(refcurrentoriginal) FROM enhanceduserdatabeta1 where channel_name = '"+channel_name+"' and request_time between '" + startdate + "'" + " and " + "'" + enddate + "' group by refcurrentoriginal limit 20000000";
    System.out.println(query);
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<String> articleList = new ArrayList();
    if (lines.size() > 0) {
      for (int i = 0; i < lines.size(); i++)
      {
        articleList.add(lines.get(i));
       }
      }
    
  
   return articleList;
  }
  
  
  public boolean SearchArticle(String ArticleUrl)
		    throws CsvExtractorException, Exception
		  {
		    
		    String query = "SELECT * FROM entity where Entity1 = '"+ArticleUrl+"'";
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
 
 
  public Entity getProductData(String channel_name,String ArticleUrl)
		    throws CsvExtractorException, Exception
		  {
		    
	    String query = "SELECT Entity1,Entity2,Entity3,Entity4,Entity5,Entity6,Entity8,Entity9,Entity10,Entity11,Entity12 FROM entity where Entity1 Like '%"+ArticleUrl.trim()+"%' GROUP BY Entity1,Entity2,Entity3,Entity4,Entity5,Entity6,Entity8,Entity9,Entity10,Entity11,Entity12";
	   // String query = "SELECT Entity1,Entity2,Entity3,Entity4,Entity5,Entity6,Entity8,Entity9,Entity10,Entity11,Entity12 FROM entity where Entity1 Like '%"+ArticleUrl+"%'";
	    System.out.println(query);
	    CSVResult csvResult = getCsvResult(false, query);
	    List<String> headers = csvResult.getHeaders();
	    List<String> lines = csvResult.getLines();
	    List<String> articleList = new ArrayList();
	    Entity obj = new Entity();
	    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
	    {
	     for (int i = 0; i < lines.size(); i++)
	      {
	        
	    	  
	    	 
	        
	         String[] data = ((String)lines.get(i)).split(",");
	         
	         if(data.length > 0 && data[0]!=null && !data[0].isEmpty())
	         obj.setEntity1(data[0]);
	        
	         if(data.length > 1 && data[1]!=null && !data[1].isEmpty())
	         obj.setEntity2(data[1]);
	        
	         if(data.length > 2 && data[2]!=null && !data[2].isEmpty())
	         obj.setEntity3(data[2]);
	        
	         if(data.length > 3 && data[3]!=null && !data[3].isEmpty())
	         obj.setEntity4(data[3]);
	         

	         if(data.length > 4 && data[4]!=null && !data[4].isEmpty())
	         obj.setEntity5(data[4]);
	         
	         
	         if(data.length > 5 && data[5]!=null && !data[5].isEmpty())
	         obj.setEntity6(data[5]);
	      
	      
	         	         
	         if(data.length > 6 && data[6]!=null && !data[6].isEmpty())
	         obj.setEntity8(data[6]);
	         
	         
	         
	         if(data.length > 7 && data[7]!=null && !data[7].isEmpty())
	         obj.setEntity9(data[7]);
	      
	      
	         
	         if(data.length > 8 && data[8]!=null && !data[8].isEmpty())
	         obj.setEntity10(data[8]);
	      
	         
	         if(data.length > 9 && data[9]!=null && !data[9].isEmpty())
	         obj.setEntity11(data[9]);
	      
	         if(data.length > 10 && data[10]!=null && !data[10].isEmpty())
		     obj.setEntity12(data[10]);
	      
	      }
	      
	    }
	       return obj;
	       
		  }   
  
  private static CSVResult getCsvResult(boolean flat, String query)
    throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException
  {
    return getCsvResult(flat, query, false, false);
  }
  
  private static CSVResult getCsvResult(boolean flat, String query, boolean includeScore, boolean includeType)
    throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException
  {
    SearchDao searchDao = getSearchDao();
    QueryAction queryAction = searchDao.explain(query);
    Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
    return new CSVResultsExtractor(includeScore, includeType).extractResults(execution, flat, ",");
  }
  
  public static void sumTest()
    throws IOException, SqlParseException, SQLFeatureNotSupportedException
  {}
  
  private static Aggregations query(String query)
    throws SqlParseException, SQLFeatureNotSupportedException
  {
    SqlElasticSearchRequestBuilder select = getSearchRequestBuilder(query);
    return ((SearchResponse)select.get()).getAggregations();
  }
  
  private static SqlElasticSearchRequestBuilder getSearchRequestBuilder(String query)
    throws SqlParseException, SQLFeatureNotSupportedException
  {
    SearchDao searchDao = getSearchDao();
    return (SqlElasticSearchRequestBuilder)searchDao.explain(query).explain();
  }
  
  private static InetSocketTransportAddress getTransportAddress()
  {
    String host = System.getenv("ES_TEST_HOST");
    String port = System.getenv("ES_TEST_PORT");
    if (host == null)
    {
      host = "172.16.105.231";
      System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default '172.16.105.231'");
    }
    if (port == null)
    {
      port = "9300";
      System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
    }
    System.out.println(String.format("Connection details: host: %s. port:%s.", new Object[] { host, port }));
    return new InetSocketTransportAddress(host, Integer.parseInt(port));
  }
}
