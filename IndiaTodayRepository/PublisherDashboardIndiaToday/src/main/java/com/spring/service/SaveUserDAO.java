package com.spring.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import util.DBConnector5;
import util.DBUtil;



public class SaveUserDAO {

   public static String saveUser(String firstname,String lastname,String emailid,String username,String password, String usertype,String monthlyvisitors,String companyname,String weburl,String address,String state,String city,String zipcode,String phone, String token ){
	    String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/cuberootapp";
		String DBUser = "root";
	    String DBPass = "qwerty12@";
	    String DBName = "cuberootapp";
		String TABLENAME = "";
		Connection con = null;
		DBConnector5 connector = new DBConnector5();
		con = connector.getPooledConnection();
	    String status = "false";
	    Statement stmt = null;
	    String query = null;
	    if(usertype.equals("advertiser"))
	    query = "INSERT INTO User(FirstName,LastName,Email,UserName,Password,UserType,CompanyName,WebURL,Address,State,City,Zipcode,Phone,SignupToken) VALUES("+"'"+firstname+"'"+","+"'"+lastname+"'"+","+"'"+emailid+"'"+","+"'"+username+"'"+","+"'"+password+"'"+","+"'"+usertype+"'"+","+"'"+companyname+"'"+","+"'"+weburl+"'"+","+"'"+address+"'"+","+"'"+state+"'"+","+"'"+city+"'"+","+"'"+zipcode+"'"+","+"'"+phone+"'"+","+"'"+token+"'"+")";
	    else
	    query = "INSERT INTO User(FirstName,LastName,Email,UserName,Password,UserType,MonthlyVisitors,CompanyName,WebURL,Address,State,City,Zipcode,Phone,SignupToken) VALUES("+"'"+firstname+"'"+","+"'"+lastname+"'"+","+"'"+emailid+"'"+","+"'"+username+"'"+","+"'"+password+"'"+","+"'"+usertype+"'"+","+"'"+monthlyvisitors+"'"+","+"'"+companyname+"'"+","+"'"+weburl+"'"+","+"'"+address+"'"+","+"'"+state+"'"+","+"'"+city+"'"+","+"'"+zipcode+"'"+","+"'"+phone+"'"+","+"'"+token+"'"+")";
	    
	    System.out.println(query);
	try {
		
		stmt = con.createStatement();
		stmt.executeUpdate(query);
		status="true";
		
		
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} finally {
		
		DBUtil.close(stmt);
		DBUtil.close(con);
	
	}

/*	
	
   if(usertype.equals("advertiser")){
	   
	  query = "INSERT INTO UserRole (emailid,RoleType) VALUES ("+emailid+","+"1)";
	  try {
		stmt = con.createStatement();
	    stmt.executeUpdate(query);
	  } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   }
	// Run specific query
	

   if(usertype.equals("publisher")){
	   
	  query = "INSERT INTO UserRole (emailid,RoleType) VALUES ("+emailid+","+"2)";
	  try {
			stmt = con.createStatement();
		    stmt.executeUpdate(query);
		  } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
	   }
	   
	   
   }
			// Run specific query
*/	
   return status;
   
   }





}
