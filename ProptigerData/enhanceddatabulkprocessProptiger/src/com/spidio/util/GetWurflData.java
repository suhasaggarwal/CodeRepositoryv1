package com.spidio.util;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class GetWurflData {

//Campaign data will be loaded from form and individual fields will be populated
	
	
//	String ServerConnectionURL = "jdbc:mysql://localhost:3306/wurfldb";
	
	public static Connection con = DBConnector.getPooledConnection("jdbc:mysql://205.147.102.47:3306/wurfldb");
	
	
	
	public static void main(String[] args) throws IOException {
		
		String mobilesId = "Sony E5653 Xperia M5 2015_september";
		
		
		Calendar cal = Calendar.getInstance();
	     
		   
	     DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	     Date timestamp = null;
		try {
			timestamp = sdf.parse("2017-10-13 12:56:56");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	     
	     cal.setTime(timestamp);
	     cal.add(Calendar.HOUR_OF_DAY, -24 * 2);
	     Date timeAfterAnHour = cal.getTime();
	     String timestamp1 = sdf.format(timeAfterAnHour);
		
		GetWurflData.get91mobilesData(mobilesId); 
	}
	
	
	
  

   public static String get91mobilesData( String mobilesId ) {
	   
		  
	   
	    String ServerConnectionURL = "jdbc:mysql://205.147.102.47:3306/wurfldb";
		String DBUser = "root";
	    String DBPass = "qwerty12@";
	    String DBName = "";
		String TABLENAME = "91mobilesproperties";
		
	    String status1 = "false";
	    Statement stmt = null;
	    String query1 = null;
	    String query2 = null;
	    String query3 = null;
	    String query4 = null;
	    String query5 = null;
	    String query6 = null;
	    String query7 = null;
	    String query8 = null;
	    String query9 = null;
	    String query10 = null;
	    String pricerange = null;
	    String processor = null;
	    String display = null;
	    String primarycamera = null;
	    String battery = null;
	    String frontcamera = null;
	    ResultSet rs = null;
	    Integer lastid = 0;
	    Integer lastid1 = 0;
	    Integer campId = 0;
	    String mobileId1 = null;
	    String [] deviceProperties = new String[2]; 
	    
			try {
				stmt = con.createStatement();
			} catch (SQLException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
		
		 query1 = "Select 91mobilesId from wurfldb where WurflId = '"+mobilesId.trim()+"'";
		 
		 System.out.println(query1);
		 
		 try {
				rs =  stmt.executeQuery(query1);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 
		 try {
				if(rs.next()){		 
				 mobileId1 = rs.getString("91mobilesId");
				 
				  
				 }
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
	     query2 = "Select * from 91mobilesproperties Where 91mobilesId like '"+ mobileId1.trim()+"'";
	     System.out.println(query2);
	     
	     try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 try {
			rs =  stmt.executeQuery(query2);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		 try {
			if(rs.next()){		 
			 pricerange = rs.getString("pricerange");
			 processor = rs.getString("processor");
			 display = rs.getString("display");
			 primarycamera = rs.getString("primarycamera");
			 battery = rs.getString("battery");
			 frontcamera  =  rs.getString("frontcamera");
			  
			 }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	     
	     System.out.println(pricerange+":"+processor+":"+display+":"+primarycamera+":"+battery+":"+frontcamera);
	  
	    return pricerange+":"+processor+":"+display+":"+primarycamera+":"+battery+":"+frontcamera;
				    

	    
			// Run specific query
	

  
  }
   
   
   
	    

}
