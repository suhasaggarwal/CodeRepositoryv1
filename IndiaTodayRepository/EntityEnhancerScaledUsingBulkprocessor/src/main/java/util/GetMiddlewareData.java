package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;

import com.publisherdata.Daos.AggregationModule;
import com.spidio.UserSegmenter.Entity;

public class GetMiddlewareData {

//Campaign data will be loaded from form and individual fields will be populated
	
	
//	String ServerConnectionURL = "jdbc:mysql://172.16.105.231:3306/wurfldb";
	
	//public static Connection con = DBConnector1.getPooledConnection("jdbc:mysql://205.147.102.47:3306/middleware");
	
	
	
	public static void main(String[] args) throws IOException {
		
		try {
			System.setOut(new PrintStream(new File(args[1])));
		} catch (Exception e) {
			e.printStackTrace();
		}

		
	    AggregationModule mod = AggregationModule.getInstance();
	    List<String> ArticleList = new ArrayList<String>();
	    List<String> ArticleListSectionWise = new ArrayList<String>();
	    DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	   
	    Calendar cal = Calendar.getInstance();
	   // cal.add(Calendar.MINUTE, -400);
	
		String endtimestamp = sdf.format(cal.getTime()).toString();
		
		Calendar cal1 = Calendar.getInstance();
	  
		cal1.add(Calendar.MINUTE,-180);
		String starttimestamp = sdf.format(cal1.getTime()).toString();
	    
		starttimestamp = starttimestamp .replace("/", "-");
		endtimestamp = endtimestamp.replace("/", "-");
	    
	//	starttimestamp = args[2];
	//	endtimestamp = args[3];
		
		starttimestamp = "now-30m";
		endtimestamp = "now";

		String channel_name = args[0];
		String section_name = args[2];
	    String limit = args[3];
	    String limitdepth = args[4];
		try {
			mod.setUp();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			ArticleList = mod.getSortedArticleListRecent(channel_name,starttimestamp, endtimestamp,limitdepth);
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			ArticleListSectionWise = mod.getSortedArticleListSectionWiseRecent(channel_name,starttimestamp, endtimestamp,section_name,limit);
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		ArticleList.addAll(ArticleListSectionWise);
	//	GetMiddlewareData.indexArticleData(mod,ArticleList,args[1]);
		GetMiddlewareData.indexArticleDataBatches(mod,ArticleList,args[1]);
	//	GetMiddlewareData.get91mobilesData(mobilesId); 
	}
	
	

   public static void indexArticleDataBatches(AggregationModule module, List<String> Articles, String filename) {
	    String url = "";
		List<String> ArticleList = new ArrayList<String>();
		List<String> ArticleListtoIndex = new ArrayList<String>();
		FileWriter writer = null;
		StringBuilder articleList= new StringBuilder();
		Set<String> articleSet = new HashSet<String>();
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
			if (url != null && !url.isEmpty() && i != Articles.size()-1) {
	    	    if (articleSet.contains(url) == false && isValidURL(url)) {
				   articleList.append("'"+url+"'"+",");
				   ArticleList.add(url);
	    	    }
	    	      
				articleSet.add(url);
			}
			else {
				if (isValidURL(url)) {
				articleList.append("'"+url+"'");
				ArticleList.add(url);
				}
				
			}
	     } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    }
		
		try {
			ArticleListtoIndex = module.SearchArticleBatches(articleList.toString(), ArticleList);
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String article : ArticleListtoIndex){	
			System.out.println(article);
		}	   
	   
	//	System.out.println("Final Article List");
}

public static void indexArticleData(AggregationModule module, List<String> Articles, String filename) {

	Boolean flag = false;
	String url = "";
	List<String> ArticleList = new ArrayList<String>();
	FileWriter writer = null;
	BufferedWriter buffer = null;
	try {
		writer = new FileWriter(filename);
		buffer = new BufferedWriter(writer);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	for (int i = 0; i < Articles.size(); i++) {
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

		if (flag == false) {
			ArticleList.add(Articles.get(i));
			// try {
			System.out.println(Articles.get(i));
			// buffer.write(Articles.get(i)+"\n");
			// } catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			// }
		}
	}
	try {
		buffer.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

}

public static boolean isValidURL(String url)
{
    // Regex to check valid URL
    String regex = "((http|https)://)(www.)?"
          + "[a-zA-Z0-9@:%._\\+~#?&//=]"
          + "{2,256}\\.[a-z]"
          + "{2,6}\\b([-a-zA-Z0-9@:%"
          + "._\\+~#?&//=]*)";

    // Compile the ReGex
    Pattern p = Pattern.compile(regex);

    // If the string is empty
    // return false
    if (url == null) {
        return false;
    }

    // Find match between given string
    // and regular expression
    // using Pattern.matcher()
    Matcher m = p.matcher(url);

    // Return if the string
    // matched the ReGex
    return m.matches();
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