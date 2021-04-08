package com.personaCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;



public class EhcacheLogParser{

    /**
     * @param args the command line arguments
     */
    public static String LogFolderPath="I:\\LogData";
    public static String accessLogPrefix="ehcachestats";
    public static String accessLogFileExt="";
    
    public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {
     
    	RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("localhost", 9200, "http")));
    
        String logEntry;
        
        if(args[0]==null){
            System.err.println("Enter log folder path in args");
            System.exit(1);
        }
        String logFolderPathArg=args[0];
      
        File logFolder=new File(logFolderPathArg);
        
        if(logFolder.isDirectory()) {
            
            String[] logFileList=logFolder.list();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            Map<String,String> map = new HashMap<String,String>();
            map.put("timestamp",new Timestamp(System.currentTimeMillis()).toString());
            for(String lf : logFileList){
                if(lf.startsWith(accessLogPrefix)){
                    BufferedReader br=new BufferedReader(new FileReader(logFolderPathArg+lf));
        
                    logEntry=br.readLine();
                   
                    
                    while(logEntry!=null){       
                    
                        if(logEntry.equals(""))break; 
                        String [] parts = logEntry.split(":");
                        System.out.println(parts[0] + ":" + parts[1]);
                        map.put(parts[0],parts[1]);                     		  
                       }
                   
                   
                    
                    
                    
                    br.close(); 
                
                } 
                
               
                
                builder.map(map); 
            }
                    IndexRequest indexRequest = new IndexRequest("Ehcachelogs");
          		    indexRequest.source(builder);

          		    IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
          		     
                }
            
                 
            }
            
      }
