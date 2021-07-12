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

public class ApacheLogParser implements LogExample {

	/**
	 * @param args the command line arguments
	 */
	public static String LogFolderPath = "/home/reportsystem";
	public static String accessLogPrefix = "PersonaLogs";
	public static String accessLogFileExt = "";

	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {

		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("192.168.103.138", 9200, "http")));

		String logEntryPatternCombinedLogFormat = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
		String logEntryPatternCLF = "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"(.*?)\" (\\S+) (\\S+)( \"(.*?)\" \"(.*?)\")?";
		String customregexp = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S+)?(\\S+)\\s?(\\S+)?\\s?(\\S+)? (\\d{3}|-) (\\d+|-) ?([^\"]*)\"?\\s?\"?([^\"]*)?";
		String customregexp1 = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S+)?(\\S+)\\s?(\\S+)?\\s?(\\S+)? (\\d{3}|-) (\\d+|-) ?([^\"]*)\"?\\s?\"?([^\"]*)?(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])? [+-]?([0-9]*[.]?[0-9]+)";
		String customregexp2 = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S+)?(\\S+)\\s?(\\S+)?\\s?(\\S+)? (\\d{3}|-) (\\d+|-) ?([^\"]*)\"?\\s?\"?([^\"]*)?(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])? [+-]?([0-9]*[.]?[0-9]+) (\\d+|-)";
		String logEntry = null;

		if (args[0] == null) {
			System.err.println("Enter log folder path in args");
			System.exit(1);
		}
		String logFolderPathArg = args[0];

		System.out.println(customregexp2);
		Pattern p = Pattern.compile(customregexp2);
		File logFolder = new File(logFolderPathArg);
		if (logFolder.isDirectory()) {

			String[] logFileList = logFolder.list();

			for (String lf : logFileList) {
				if (lf.startsWith(accessLogPrefix)) {
					BufferedReader br = new BufferedReader(new FileReader(logFolderPathArg + "/" + lf));
					while ((logEntry = br.readLine()) != null) {
						//logEntry = "47.9.200.13 - - [08/Apr/2021:00:47:54 +0530] GET /publisherv1/getCookieData?localstorageid=a914a830-acd8-4490-a82b-61352a45c0a4&tunepersonasize=20 HTTP/1.0 200 1828 Mozilla/5.0 (Linux; Android 5.1.1; SM-J320F Build/LMY47V; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/83.0.4103.101 Mobile Safari/537.36 https://www.aajtak.in/crime/police-and-intelligence/story/mumbai-antilia-case-accused-sachin-waze-nia-custody-cbi-inquiry-lawyer-court-crime-1234829-2021-04-07 0.003 2";
						if (logEntry.equals(""))
							break;
						Matcher matcher = p.matcher(logEntry);
						if (!matcher.matches()) {
							System.err.println("Bad log entry (or problem with RE?):");
							System.err.println(logEntry);
							continue;
						}

						String request = matcher.group(7);
						String[] parts = request.split("\\?");
						String[] requestparameters = parts[1].split("&");
						String localstorageID = requestparameters[0].split("=")[1];
						String tunepersonaSize = requestparameters[1].split("=")[1];
						String timestamp = matcher.group(4);
						String ipaddress = matcher.group(1);
						String id = localstorageID;
						String tuneparam = tunepersonaSize;
						String statuscode = matcher.group(9);
						String personasize = matcher.group(10);
						String url = matcher.group(14).concat(matcher.group(15));
						String uagent = matcher.group(11);
						DeviceDetector dd = new DeviceDetectorBuilder().build();						
						Detection detection = dd.detect(uagent);
						String device = detection.getDevice().map(d -> d.toString()).orElse("unknown");
						String os = detection.getOperatingSystem().map(d -> d.toString()).orElse("unknown");
						String userclient = detection.getClient().map(d -> d.toString()).orElse("unknown");
                        String type = "unknown";
                        String brand = "unknown";
                        String model = "unknown";
						if (detection.getDevice().isPresent()) {
							type = detection.getDevice().get().getType(); // bot, browser, feed reader...
							brand = detection.getDevice().get().getBrand().orElse("unknown");
							model = detection.getDevice().get().getModel().orElse("unknown");
						}	
						String requestprocessingtime = matcher.group(16);
						XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
								.field("timestamp", new Timestamp(System.currentTimeMillis()).toString())
								.field("requesttime", matcher.group(4)).field("ipaddress", matcher.group(1))
								.field("localstorageid", localstorageID.replace("-", "_"))
								.field("tuningparameter", tunepersonaSize).field("statuscode", matcher.group(9))
								.field("personasize", matcher.group(10)).field("pageurl", url)
								.field("useragent", matcher.group(11)).field("requestprocessingtime", matcher.group(16))
								.field("device",device)
								.field("operatingsystem",os)
								.field("client",userclient)
								.field("devicetype",type)
								.field("brand",brand)
								.field("model",model)
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
