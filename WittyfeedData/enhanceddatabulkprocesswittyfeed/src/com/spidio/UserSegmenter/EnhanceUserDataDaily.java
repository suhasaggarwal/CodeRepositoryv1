package com.spidio.UserSegmenter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import net.sourceforge.wurfl.core.WURFLHolder;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.maxmind.geoip.LookupService;
import com.spidio.dataModel.DeviceObject;
import com.spidio.dataModel.LocationObject;
import com.spidio.dataModel.LookUpService;
import com.spidio.dataModel.LookUpServiceISP;
import com.spidio.dataModel.LookUpServiceOrganisation;
import com.spidio.dataModel.WurflHolder;
import com.spidio.util.GetWurflData;

public class EnhanceUserDataDaily {

	// Enhances existing data points such as geo-based,device-based,Time of the
	// day patterns and update it in corresponding Elasticsearch Document.
	// These enhanced data points are later used to generate publisher based
	// report codes which gives full profile of publishers Audience.

private static TransportClient client;
	
	public static void setUp()
		    throws Exception
		  {
		    if (client == null)
		    {
		      client = new TransportClient();
		      client.addTransportAddress(getTransportAddress());
		      
		      NodesInfoResponse nodeInfos = (NodesInfoResponse)client.admin().cluster().prepareNodesInfo(new String[0]).get();
		      String clusterName = nodeInfos.getClusterName().value();
		      System.out.println(String.format("Found cluster... cluster name: %s", new Object[] { clusterName }));
		      
		    }
		    System.out.println("Finished the setup process...");
		  }

	private static InetSocketTransportAddress getTransportAddress()
	  {
	    String host = null;
	    String port = null;
	    if (host == null)
	    {
	      host = "172.16.105.231";
	     // System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default '172.16.105.231'");
	    }
	    if (port == null)
	    {
	      port = "9300";
	    //  System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
	    }
	    System.out.println(String.format("Connection details: host: %s. port:%s.", new Object[] { host, port }));
	    return new InetSocketTransportAddress(host, Integer.parseInt(port));
	  }
	
	
	
	

	  public static String getHostName(String url)
	  {
	    URI uri = null;
	    try
	    {
	      uri = new URI(url);
	    }
	    catch (URISyntaxException e)
	    {
	      e.printStackTrace();
	    }
	    String hostname = uri.getHost();
	    if (hostname != null) {
	      return hostname.startsWith("www.") ? hostname.substring(4) : hostname;
	    }
	    return hostname;
	  }
	
	
	 
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		setUp();
		
	    String startdate = "now-10d";
	    		
	    String enddate =  "now";   		
	
	    boolean status = false;
	 
	    WurflHolder.getInstance();
	    LookUpService.getInstance();
	    LookUpServiceISP.getInstance();
	    LookUpServiceOrganisation.getInstance();
	    Set<String> lines = new HashSet<String>(FileUtils.readLines(new File("/root/websites.txt")));
	    status = EnhanceUserDataDailyv1.generateData(startdate, enddate, client,lines);
	   
	  

	        
	}    
}
