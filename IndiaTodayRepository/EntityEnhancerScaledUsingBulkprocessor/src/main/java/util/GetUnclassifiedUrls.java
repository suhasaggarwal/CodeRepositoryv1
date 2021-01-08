package util;

import java.io.File;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;

import com.publisherdata.Daos.AggregationModule;

public class GetUnclassifiedUrls {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			System.setOut(new PrintStream(new File(args[0])));
		} catch (Exception e) {
			e.printStackTrace();
		}

		
	    AggregationModule mod = AggregationModule.getInstance();
	    List<String> ArticleList = new ArrayList<String>();
	    
		try {
			mod.setUp();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			ArticleList = mod.GetUnclassifiedUrls();
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for (String article : ArticleList){	
			System.out.println(article);
		}
		
		
	}

}
