package com.spring.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import util.DBConnector5;
import util.DBUtil;



public class UserDao {

	public static String GetUserRole(String emailId){
		
		String roleId = null;
		Connection con = null;
		ResultSet rs=null;
		PreparedStatement ps=null;
		try{
			    String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/cuberootapp";
				String DBUser = "root";
			    String DBPass = "qwerty12@";
			    String DBName = "cuberootapp";
				String TABLENAME = "";
				
				DBConnector5 connector = new DBConnector5();
				con = connector.getPooledConnection();
			
			    Statement stmt = null;
			
			    ps=con.prepareStatement("select roletype from userrole where emailid=?");
			    ps.setString(1,emailId);
	//		ps.setString(2,pass);
			
			   rs=ps.executeQuery();
			   roleId = rs.getString("roletype");
	//Based on roleIds obtained,we can grant specific access view corresponding to Advertiser/Publisher		
			
		}catch(Exception e){System.out.println(e);}
        finally {
			
		    DBUtil.close(rs);
			DBUtil.close(ps);
			DBUtil.close(con);
		
		} 
		
		
		return roleId;
		}
}
