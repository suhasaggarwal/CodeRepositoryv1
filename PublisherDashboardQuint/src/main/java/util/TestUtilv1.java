package util;

import java.io.File;
import java.io.PrintStream;
import java.text.ParseException;

public class TestUtilv1 {
	
	public static Integer [] a = new Integer[128]; 
	public static Integer num_denominations = 4;
	public static Integer [] table = new Integer[128]; 
	
	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub

		
		 try {
		     System.setOut(new PrintStream(new File("TagCleaner1.txt")));
		    } catch (Exception e) {
		         e.printStackTrace();
		    }
      for(int i=0; i<21000; i++){
    	  
    	  System.out.println("a"+i+";b"+i+";1;d"+i);
    	  
      }
	    
	    
	    
	    
	    }
	
}	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	


