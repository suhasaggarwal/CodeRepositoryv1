package com.personaCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
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




interface LogExample {
  /** The number of fields that must be found. */
  public static final int NUM_FIELDS = 9;

  /** The sample log entry to be parsed. */
  public static final String logEntryLine = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"";

}

public class ApacheLogParser implements LogExample{

    /**
     * @param args the command line arguments
     */
    public static String LogFolderPath="I:\\LogData";
    public static String accessLogPrefix="PersonaLogs";
    public static String accessLogFileExt="";
    
    public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {
     
    	RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("localhost", 9200, "http")));
    
    	
    	String logEntryPatternCombinedLogFormat = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
        String logEntryPatternCLF = "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"(.*?)\" (\\S+) (\\S+)( \"(.*?)\" \"(.*?)\")?";
        String customregexp = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S+)?(\\S+)\\s?(\\S+)?\\s?(\\S+)? (\\d{3}|-) (\\d+|-) ?([^\"]*)\"?\\s?\"?([^\"]*)?";
        String customregexp1 = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S+)?(\\S+)\\s?(\\S+)?\\s?(\\S+)? (\\d{3}|-) (\\d+|-) ?([^\"]*)\"?\\s?\"?([^\"]*)?(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])? [+-]?([0-9]*[.]?[0-9]+)";
        String customregexp2 = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S+)?(\\S+)\\s?(\\S+)?\\s?(\\S+)? (\\d{3}|-) (\\d+|-) ?([^\"]*)\"?\\s?\"?([^\"]*)?(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])? [+-]?([0-9]*[.]?[0-9]+) (\\d+|-)";
        String logEntry;
        
        if(args[0]==null){
            System.err.println("Enter log folder path in args");
            System.exit(1);
        }
        String logFolderPathArg=args[0];
       
        System.out.println(customregexp2);

        Pattern p = Pattern.compile(customregexp2);
        File logFolder=new File(logFolderPathArg);
        
        if(logFolder.isDirectory()) {
            
            String[] logFileList=logFolder.list();
            
            for(String lf : logFileList){
                if(lf.startsWith(accessLogPrefix)){
                    BufferedReader br=new BufferedReader(new FileReader(logFolderPathArg+lf));
        
                    logEntry=br.readLine();
                    
                    while(logEntry!=null){
                        if(logEntry.equals(""))break;
                        Matcher matcher = p.matcher(logEntry);
                        if (!matcher.matches()) {
                          System.err.println("Bad log entry (or problem with RE?):");
                          System.err.println(logEntry);
                          return;
                        }
 
                        String request = matcher.group(7);
                        String [] parts = request.split("\\?");
                        String [] requestparameters = parts[1].split("&");
                        String localstorageID  = requestparameters[0].split("=")[1];   
                        String tunepersonaSize  = requestparameters[1].split("=")[1];  		
                        String timestamp = matcher.group(4);
                        String ipaddress =  matcher.group(1);
                        String id = localstorageID;
                        String tuneparam = tunepersonaSize;
                        String statuscode = matcher.group(9);
                        String personasize = matcher.group(10);
                        String url = matcher.group(14);
                        String uagent = matcher.group(11);
                        String requestprocessingtime = matcher.group(16);
                        XContentBuilder builder = XContentFactory.jsonBuilder()
                        		  .startObject()
                        		  .field("timestamp", new Timestamp(System.currentTimeMillis()).toString())
                        		  .field("requesttime", matcher.group(4))
                        		  .field("ipaddress", matcher.group(1))
                        		  .field("localstorageid", localstorageID)
                        		  .field("tuningparameter", tunepersonaSize)
                        		  .field("statuscode", matcher.group(9))
                        		  .field("personasize", matcher.group(10))
                        		  .field("pageurl", matcher.group(14))
                        		  .field("useragent", matcher.group(11))
                        		  .field("requestprocessingtime", matcher.group(16))
                        		  .endObject();
                       
                              
                        		  IndexRequest indexRequest = new IndexRequest("personalogs");
                        		  indexRequest.source(builder);

                        		  IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                        		  
                    }
                     br.close();
                }
            }
            
        }
    }
}
