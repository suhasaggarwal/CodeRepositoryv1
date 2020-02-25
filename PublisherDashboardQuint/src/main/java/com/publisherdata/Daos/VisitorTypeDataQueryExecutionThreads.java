package com.publisherdata.Daos;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.nlpcn.es4sql.SearchDao;

import util.GetMiddlewareData;

import com.publisherdata.model.Article;
import com.publisherdata.model.PublisherReport;

import javolution.util.FastMap;

public class VisitorTypeDataQueryExecutionThreads implements Callable<List<PublisherReport>>{

	 public String query=null;
	 public String startdate = null;
	 public String enddate = null;
	 public String channel_name = null;
	 public String metric = null;
	 
	 public List<PublisherReport> pubreport = new ArrayList();
     
	 public  VisitorTypeDataQueryExecutionThreads(String metric,String startdate, String enddate, String channel_name, String query,TransportClient client,SearchDao searchDao) {
       this.query = query;
       this.startdate = startdate;
	   this.enddate = enddate;
	   this.channel_name = channel_name;
	   this.metric = metric;
	 }

	 public List<PublisherReport> call() {

		  PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream("combinedVisitorthread.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
			System.setOut(out);
		 
		 
	    AggregationModule module = new AggregationModule();
 	    List<PublisherReport> pubreport = new ArrayList();
 		System.out.println(query);
 		
 		if(query.equals("newVisitors"))
 		{
 		final long startTime1 = System.currentTimeMillis();
 		
		try {
			pubreport = module.countNewUsersChannelDatewise(startdate, enddate, channel_name, metric);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    final long endTime1 = System.currentTimeMillis();
 		
 		System.out.println("Total combined count execution time: " + (endTime1 - startTime1) );
 		}
 		
 		
 		if(query.equals("returningVisitors"))
 		{
 		final long startTime1 = System.currentTimeMillis();
 		
		try {
			pubreport = module.countReturningUsersChannelDatewise(startdate, enddate, channel_name, metric);
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    final long endTime1 = System.currentTimeMillis();
 		
 		System.out.println("Total combined count execution time: " + (endTime1 - startTime1) );
 		}
 		
 		
 		if(query.equals("loyalVisitors"))
 		{
 		final long startTime1 = System.currentTimeMillis();
 		
		try {
			pubreport = module.countLoyalUsersChannelDatewise(startdate, enddate, channel_name, metric);
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    final long endTime1 = System.currentTimeMillis();
 		
 		System.out.println("Total combined count execution time: " + (endTime1 - startTime1) );
 		}
 		
 		
 		
 		if(query.equals("newVisitorsAggregate"))
 		{
 		final long startTime1 = System.currentTimeMillis();
 		
		try {
			pubreport = module.countNewUsersChannelDatewisegroupby(startdate, enddate, channel_name, metric);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    final long endTime1 = System.currentTimeMillis();
 		
 		System.out.println("Total combined count execution time: " + (endTime1 - startTime1) );
 		}
 		
 		
 	   
 		if(query.equals("returningVisitorsAggregate"))
 		{
 		final long startTime1 = System.currentTimeMillis();
 		
		try {
			pubreport = module.countReturningUsersChannelDatewisegroupby(startdate, enddate, channel_name, metric); 
			;
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    final long endTime1 = System.currentTimeMillis();
 		
 		System.out.println("Total combined count execution time: " + (endTime1 - startTime1) );
 		}
 		
 		
 		if(query.equals("loyalVisitorsAggregate"))
 		{
 		final long startTime1 = System.currentTimeMillis();
 		
		try {
			pubreport = module.countLoyalUsersChannelDatewisegroupby(startdate, enddate, channel_name, metric);
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    final long endTime1 = System.currentTimeMillis();
 		
 		System.out.println("Total combined count execution time: " + (endTime1 - startTime1) );
 		}
 		
 		
 		
 		
 			
 		return pubreport;
	
 	    }
	 }
	 
	
 	    
