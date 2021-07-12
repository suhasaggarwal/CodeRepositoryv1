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
import java.util.TimeZone;
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

public class EhcacheLogParser {

	/**
	 * @param args the command line arguments
	 */
	public static String LogFolderPath = "/home/reportsystem";
	public static String accessLogPrefix = "EhcacheStats";
	public static String accessLogFileExt = "";

	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {

		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("192.168.103.138", 9200, "http")));

		String logEntry;

		if (args[0] == null) {
			System.err.println("Enter log folder path in args");
			System.exit(1);
		}
		String logFolderPathArg = args[0];
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		File logFolder = new File(logFolderPathArg);

		if (logFolder.isDirectory()) {

			String[] logFileList = logFolder.list();

			for (String lf : logFileList) {
				if (lf.startsWith(accessLogPrefix)) {
					BufferedReader br = new BufferedReader(new FileReader(logFolderPathArg + lf));
					Map<String, String> map = new HashMap<String, String>();					
					while ((logEntry = br.readLine()) != null) {
						map.put("timestamp", new Timestamp(System.currentTimeMillis()).toString());
						if (logEntry.equals(""))
							break;
						String[] parts = logEntry.split(":");
						System.out.println(parts[0] + ":" + parts[1]);
						map.put(parts[0].toLowerCase().replaceAll(" ", "").replaceAll("%", ""), parts[1]);
					}

					XContentBuilder builder = XContentFactory.jsonBuilder();
					builder.map(map);
					IndexRequest indexRequest = new IndexRequest("personafetchstats");
					indexRequest.source(builder);
					IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
					br.close();

				}

			}

		}

	}

}

interface LogExample {
  /** The number of fields that must be found. */
  public static final int NUM_FIELDS = 9;

  /** The sample log entry to be parsed. */
  public static final String logEntryLine = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"";

}
