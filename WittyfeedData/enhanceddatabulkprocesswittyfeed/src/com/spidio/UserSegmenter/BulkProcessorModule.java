package com.spidio.UserSegmenter;


import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class BulkProcessorModule {
   
	public static BulkProcessor bulkProcessor;
	
	
	public static BulkProcessor GetInstance(Client client) {
       
    
		
		 BulkProcessor bulkProcessor = BulkProcessor.builder(
				  client,  
				  new BulkProcessor.Listener() {
				              
				      @Override
				      public void afterBulk(long arg0, BulkRequest arg1,
				      		BulkResponse arg2) {
				      	// TODO Auto-generated method stub
				      	System.out.println("Finished bulk response");
				      	
				      }
				      
				      @Override
				      public void afterBulk(long arg0, BulkRequest arg1,
				      		Throwable failure) {
				      	// TODO Auto-generated method stub
				      	System.out.println("Failed to finish bulk response");
				      	failure.printStackTrace();
				      	
				      }
				      
				      @Override
				      public void beforeBulk(long arg0, BulkRequest arg1) {
				      	// TODO Auto-generated method stub
				      	System.out.println("Starting bulk response");
				      	
				      } 
				              })
				      .setBulkActions(10000) 
				      .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)) 
				      .setFlushInterval(TimeValue.timeValueSeconds(10)) 
				      .setConcurrentRequests(3) 
				      .build();
	
		  return bulkProcessor;
	      }
	
	
	
	
	

    
   
}
