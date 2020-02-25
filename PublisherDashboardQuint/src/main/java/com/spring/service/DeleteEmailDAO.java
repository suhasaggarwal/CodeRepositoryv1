package com.spring.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import util.DBConnector5;
import util.DBUtil;

public class DeleteEmailDAO {

	public static boolean DeleteEmail(String emailid){
	
	boolean status=false;
	String roleId = null;
	PreparedStatement ps = null;
	Connection con = null;
	try{
		    String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/cuberootapp";
			String DBUser = "root";
		    String DBPass = "qwerty12@";
		    String DBName = "cuberootapp";
			String TABLENAME = "";
			
			DBConnector5 connector = new DBConnector5();
			con = connector.getPooledConnection();
		   
			
		    Statement stmt = null;
		
		    ps=con.prepareStatement("delete from User where Email=?");
		    ps.setString(1,emailid);
//		ps.setString(2,pass);
		    status=ps.execute();
		//    status=rs.next();
//Based on roleIds obtained,we can grant specific access view corresponding to Advertiser/Publisher		
		
	}catch(Exception e){System.out.println(e);}
	 finally{
  	   
			DBUtil.close(ps);
			DBUtil.close(con);
			
		} 
	
	
	return status;
	}

}
