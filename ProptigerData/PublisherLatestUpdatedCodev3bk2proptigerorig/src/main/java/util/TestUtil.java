package util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.publisherdata.Daos.AggregationModule;
import com.publisherdata.model.Article;
import com.websystique.springmvc.service.ReportService;
import com.websystique.springmvc.service.ReportServiceImpl;

public class TestUtil {
	
	public static Integer [] a = new Integer[128]; 
	public static Integer num_denominations = 4;
	public static Integer [] table = new Integer[128]; 
	
	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub

		
		a[0]=2;
		a[1]=3;
		a[2]=5;
		a[3]=6;
		
		for( int i=0; i<127; i++){
			
			table[i]=-1;
			
		}
		
		
		int answer = Makingchange(10);		
		System.out.println(answer);
		/* 
		 * 
		GetMiddlewareData.getArticleData("1");
		GetMiddlewareData.getSectionData("1");
		Article obj1 = GetMiddlewareData.getArticleMetaData("http://womansera.com/trending/celerbrity-gossip/deepikas-comment-shahids-picture-will-make-go-aww");
		Date date1=new SimpleDateFormat("yyyy-MM-dd").parse("2017-01-01");
		Date date2= new SimpleDateFormat("yyyy-MM-dd").parse("2017-01-19");
		int days = AggregationModule.getDifferenceDays(date1,date2);
		System.out.println(days);
		List<Long> dates = AggregationModule.getDaysBetweenDates(date1, date2);
		System.out.println(dates);
		*/
		
		//String date = java.time.LocalDate.now().toString(); 
		//System.out.println(date);
		//for(int i=0;i<200;i++){
			
			//System.out.println("Segmentb"+i);
	//	}
		/*
		String starttimestamp= "";
		String endtimestamp="";
		String dateRange="";
		DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
        endtimestamp=sdf.format(cal.getTime()).toString();
		
		cal.add(Calendar.MINUTE, -15);
		starttimestamp = sdf.format(cal.getTime()).toString();
        dateRange=starttimestamp+","+endtimestamp;
		System.out.println(dateRange);
		
	 	*/
	}
	
	    public static Integer Makingchange(int n){
	                	
	     if( n < 0)
	    	 return -1;
	     
	     if( n ==0)
	    	 return 0;
	     
	     if(table[n]!= -1)
	    	return table[n];
	     
	     int ans = -1;
	     
	     for(int i=0; i< num_denominations; ++i){
	    	 
	    	 ans = Max(ans,Makingchange(n - a[i]));
	    	 
	     } 
	    	 
	     return table[n] = ans+1; 	 
	    	 
	     }
	    	
	    	
	    
	    public static  Integer Max(int a, int b){
        	
		       if(a<=b)
                return b;
		       
		       else
		    	   return a;		    	   
		     }
		    	
	    
	    
	    
	    
	    
	    }
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	


