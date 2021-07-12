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
import io.github.mngsk.devicedetector.Detection;
import io.github.mngsk.devicedetector.DeviceDetector;
import io.github.mngsk.devicedetector.DeviceDetector.DeviceDetectorBuilder;

public class CookiePoolLogParser implements LogExample {

	/**
	 * @param args the command line arguments
	 */
	public static String LogFolderPath = "/home/reportsystem";
	public static String cookiepoolLogPrefix = "ImpressionProjections";

	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {

		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("192.168.103.138", 9200, "http")));

	
		String logEntry = null;

		if (args[0] == null) {
			System.err.println("Enter log folder path in args");
			System.exit(1);
		}
		String logFolderPathArg = args[0];
		File logFolder = new File(logFolderPathArg);
		if (logFolder.isDirectory()) {

			String[] logFileList = logFolder.list();

			for (String lf : logFileList) {
				if (lf.startsWith(cookiepoolLogPrefix)) {
					BufferedReader br = new BufferedReader(new FileReader(logFolderPathArg + "/" + lf));
					while ((logEntry = br.readLine()) != null) {
						if (logEntry.equals(""))
							break;
						String[] parts = logEntry.split(",");
						String Categories = parts[0];
						String Audiencepool = parts[1];
						String DailyImpressionProjectionForecast = parts[2];
						String ChannelName = parts[3];
						XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
								.field("Categories",Categories)
								.field("AudiencePool",Audiencepool)
								.field("MinimumDailyImpressionBurnForecast",DailyImpressionProjectionForecast)
								.field("ChannelName",ChannelName)
								.endObject();

						IndexRequest indexRequest = new IndexRequest("cookiepoolaajtakapril");
						indexRequest.source(builder);

						IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

					}
					br.close();
				}
			}

		}
	}
}
