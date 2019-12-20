package com.spring.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.spring.util.DBConnector;
import com.spring.util.DBUtil;

public class UpdateUserDAO {

	 public static String updateUser(String address,String state,String city,String zipcode,String phone,String emailid ){
		    String ServerConnectionURL = "jdbc:mysql://52.90.244.81:3306/cuberootapp";
			String DBUser = "root";
		    String DBPass = "Qwerty12@";
		    String DBName = "cuberootapp";
			String TABLENAME = "";
			Connection con = null;
			DBConnector connector = new DBConnector();
			con = connector.getPooledConnection();
		    String status = "false";
		    Statement stmt = null;
		
		String query = "Update User Set Address ="+ " '"+address+"' "+","+"Set State =" + "' "+state+"' "+","+ " SET City =" + "' "+city+"' "+"," +"Set Zipcode ="+"' "+zipcode+"' "+","+ "Set Phone ="+ "' "+phone+"' "+" where email ="+emailid;
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

	
	 
	 public static String updateUserLoginState(String AccountState, String token ){
		    String ServerConnectionURL = "jdbc:mysql://52.90.244.81:3306/cuberootapp";
			String DBUser = "root";
		    String DBPass = "Qwerty12@";
		    String DBName = "cuberootapp";
			String TABLENAME = "";
			Connection con = null;
			DBConnector connector = new DBConnector();
			con = connector.getPooledConnection();
		    String status = "false";
		    Statement stmt = null;
		
		String query = "Update User Set AccountState ="+ " '"+"Confirmed"+"' "+"where SignupToken="+"'"+token+"'";
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


	 public static String updateUserPassword(String token, String emailid){
		    String ServerConnectionURL = "jdbc:mysql://52.90.244.81:3306/cuberootapp";
			String DBUser = "root";
		    String DBPass = "Qwerty12@";
		    String DBName = "cuberootapp";
			String TABLENAME = "";
			Connection con = null;
			DBConnector connector = new DBConnector();
			con = connector.getPooledConnection();
		    String status = "false";
		    Statement stmt = null;
		
		String query = "Update User Set Password = "+"'"+token+"'"+" Where Email = "+"'"+emailid+"'";
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
		}finally {
			
		   
			DBUtil.close(stmt);
			DBUtil.close(con);
		
		}


	   return status;
	   
	   }	




}
