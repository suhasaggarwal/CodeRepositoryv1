package util;




import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;

import com.publisherdata.Daos.AggregationModule;
import com.spidio.UserSegmenter.Entity;

public class GetUniqueData {

//Campaign data will be loaded from form and individual fields will be populated
	
	
//	String ServerConnectionURL = "jdbc:mysql://172.16.105.231:3306/wurfldb";
	
	//public static Connection con = DBConnector1.getPooledConnection("jdbc:mysql://205.147.102.47:3306/middleware");
	
	
	
	public static void main(String[] args) throws IOException {
		
		
		// StringBuilder sb = new StringBuilder();
		
		Set<String> lines= new HashSet<String>();
		List<String> lines1 = new ArrayList<String>();
		FileWriter writer = null;
			try {
				writer = new FileWriter("indiatodayurlList3");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		
		
	     try (BufferedReader br = Files.newBufferedReader(Paths.get("indiatodayurlListac"))) {

	            // read line by line
	            String line;
	            while ((line = br.readLine()) != null) {
	                line = line.split("\\?")[0];
	                if(lines.contains(line) == false) {
	                	writer.write(line+"\n");
	                }
	            }

	        } catch (IOException e) {
	            System.err.format("IOException: %s%n", e);
	        }
		
	}
	
	

		 

   
}