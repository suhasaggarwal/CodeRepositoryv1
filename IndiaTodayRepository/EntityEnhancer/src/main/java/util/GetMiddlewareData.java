package util;




import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;

import com.publisherdata.Daos.AggregationModule;
import com.spidio.UserSegmenter.Entity;

public class GetMiddlewareData {

//Campaign data will be loaded from form and individual fields will be populated
	
	
//	String ServerConnectionURL = "jdbc:mysql://172.16.105.231:3306/wurfldb";
	
	//public static Connection con = DBConnector1.getPooledConnection("jdbc:mysql://205.147.102.47:3306/middleware");
	
	
	
	public static void main(String[] args) throws IOException {
		
	    AggregationModule mod = AggregationModule.getInstance();
	    List<String> ArticleList = new ArrayList<String>();
	    
	    DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	   
	    Calendar cal = Calendar.getInstance();
	   // cal.add(Calendar.MINUTE, -400);
	
		String endtimestamp = sdf.format(cal.getTime()).toString();
		
		Calendar cal1 = Calendar.getInstance();
	  
		cal1.add(Calendar.MINUTE,-800);
		String starttimestamp = sdf.format(cal1.getTime()).toString();
	    
		starttimestamp = starttimestamp .replace("/", "-");
		endtimestamp = endtimestamp.replace("/", "-");
	    
	//	starttimestamp = args[2];
	//	endtimestamp = args[3];
		
		String channel_name= args[0];
		
		try {
			mod.setUp();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			ArticleList = mod.getSortedArticleList(channel_name,starttimestamp, endtimestamp);
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		GetMiddlewareData.indexArticleData(mod,ArticleList,args[1]);	
	//	GetMiddlewareData.get91mobilesData(mobilesId); 
	}
	
	

   public static void indexArticleData( AggregationModule module, List<String> Articles, String filename ) {
	   
	    
		Boolean flag = false;
	    String url = "";
		List<String> ArticleList = new ArrayList<String>();
		FileWriter writer = null;
		try {
			writer = new FileWriter(filename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		for(int i=0; i< Articles.size(); i++)
	    {
	     	url = Articles.get(i).split("\\?")[0];
	    try {
			if (url != null && !url.isEmpty()) {
	    	flag = module.SearchArticle(url);
			}
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	   
	    if (flag == false){
	       ArticleList.add(Articles.get(i));
	       try {
			writer.write(Articles.get(i)+"\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	       }
	    }
	      try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  
}


   public static Entity getEntityData(String channel_name, AggregationModule module, String url ) {
	   
	 
	   Entity product = null;
	   
       try {
		    product = module.getProductData(channel_name, url);
	        System.out.println("Product1:"+product);
       } catch (CsvExtractorException e) {
		// TODO Auto-generated catch block
    	   System.out.println("In Exception 1:"+product);
    	   e.printStackTrace();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		   System.out.println("In Exception 2:"+product);
		   e.printStackTrace();
	}
       
       return product;
		
		 
}
   
   
   
   
   
   
   
   
   
   
   
   
   
   
}