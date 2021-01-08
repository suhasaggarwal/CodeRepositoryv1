package util;


import java.text.SimpleDateFormat;
import java.util.Date;
public class TestClass {
  public static void main(String[] args) {
/*	  
	SimpleDateFormat myFormat = new SimpleDateFormat("dd MM yyyy");
    String inputString1 = "25 04 2020";
    String inputString2 = "13 12 2020";
    try{
      Date oldDate = myFormat.parse(inputString1);
      Date newDate = myFormat.parse(inputString2);
      int diffInDays = (int)( (newDate.getTime() - oldDate.getTime())
                 / (1000 * 60 * 60 * 24) );
      System.out.println(diffInDays);
    }catch(Exception ex){
       System.out.println(ex);
    }
    
   */
	  
	String testString = "This is a test String&data-datav1/" ; 
	testString = testString.replaceAll("[^\\sa-zA-Z0-9]", " ").replaceAll("/$"," ");
	System.out.println(testString);
	  
	
	
	  
	  
	  
  
  }
}
