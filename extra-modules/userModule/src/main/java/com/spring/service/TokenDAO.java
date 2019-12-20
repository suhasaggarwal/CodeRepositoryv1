package com.spring.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.spring.util.DBConnector;
import com.spring.util.DBUtil;

public abstract class TokenDAO {

	
	public static boolean VerifyToken(String token){
	
	boolean status=false;
	String roleId = null;
	Connection con = null;
	PreparedStatement ps = null;
	ResultSet rs = null;
	try{
		    String ServerConnectionURL = "jdbc:mysql://52.90.244.81:3306/cuberootapp";
			String DBUser = "root";
		    String DBPass = "Qwerty12@";
		    String DBName = "cuberootapp";
			String TABLENAME = "";
			
			DBConnector connector = new DBConnector();
			con = connector.getPooledConnection();
		   
			
		    Statement stmt = null;
		
		    ps=con.prepareStatement("select * from User where SignupToken=?");
		    ps.setString(1,token);
//		ps.setString(2,pass);
		    rs=ps.executeQuery();
		    status=rs.next();
//Based on roleIds obtained,we can grant specific access view corresponding to Advertiser/Publisher		
		
	}catch(Exception e){System.out.println(e);}
	 finally {
			
		    DBUtil.close(rs);
			DBUtil.close(ps);
			DBUtil.close(con);
		
		}

	
	
	return status;
	}

}
