package com.spring.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.spring.model.User;
import com.spring.util.DBConnector;
import com.spring.util.DBUtil;

public class UserDetailsDAO {

public static User GetUserDetails(String emailId){
		
		String roleId = null;
		User user = null;
		Connection con = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try{
			    String ServerConnectionURL = "jdbc:mysql://52.90.244.81:3306/cuberootapp";
				String DBUser = "root";
			    String DBPass = "Qwerty12@";
			    String DBName = "cuberootapp";
				String TABLENAME = "";
				
				DBConnector connector = new DBConnector();
				con = connector.getPooledConnection();
			
			    Statement stmt = null;
			
			    ps=con.prepareStatement("select * from User where Email=?");
			    ps.setString(1,emailId);
	//		ps.setString(2,pass);
			
			   
			    
			    rs=ps.executeQuery();
			    if(rs.next())  {
			    user = new User();
			    user.setFirstname(rs.getString("FirstName"));
		        user.setLastname(rs.getString("LastName"));
		        user.setUsername(rs.getString("UserName"));
		        user.setEmail(rs.getString("Email"));
		        user.setPassword(rs.getString("Password"));
		        user.setUsertype(rs.getString("UserType"));
		        user.setCompanyname(rs.getString("CompanyName"));
		        user.setWebsiteurl(rs.getString("WebURL"));
		        user.setAddress(rs.getString("Address"));
		        user.setState(rs.getString("State"));
		        user.setCity(rs.getString("City"));
		        user.setPhone(rs.getString("Phone"));
		        user.setZipcode(rs.getString("ZipCode"));
			   }
		      
	//Based on roleIds obtained,we can grant specific access view corresponding to Advertiser/Publisher		
			
		}catch(Exception e){e.printStackTrace();}
        finally {
			
		    DBUtil.close(rs);
			DBUtil.close(ps);
			DBUtil.close(con);
		
		}
		
		
		return user;
		}



public static String GetUserType(String emailId){
	
	String roleId = null;
	String usertype = null;
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
		
		    ps=con.prepareStatement("select UserType from User where Email=?");
		    ps.setString(1,emailId);
//		ps.setString(2,pass);
		
		   
		    
		    rs=ps.executeQuery();
		    if(rs.next())  {
	         usertype = rs.getString("UserType");
	      
		   }
	      
//Based on roleIds obtained,we can grant specific access view corresponding to Advertiser/Publisher		
		
	}catch(Exception e){e.printStackTrace();}
	 finally {
			
		    DBUtil.close(rs);
			DBUtil.close(ps);
			DBUtil.close(con);
		
		} 
	return usertype;
	}
















}