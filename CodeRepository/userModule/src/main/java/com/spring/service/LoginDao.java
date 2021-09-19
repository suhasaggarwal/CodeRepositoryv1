package com.spring.service;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.spring.util.DBConnector;
import com.spring.util.DBUtil;

public class LoginDao {

public static boolean validate(String name,String pass){
boolean status=false;
Connection con = null;
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
	
    ps=con.prepareStatement("Select * from User where Email=? and Password=? and AccountState=?");
	ps.setString(1,name);
	ps.setString(2,pass);
	ps.setString(3,"Confirmed");
	ResultSet rs=ps.executeQuery();
	status=rs.next();
	
	
}catch(Exception e){System.out.println(e);}
finally{
	   
	DBUtil.close(ps);
	DBUtil.close(con);
	
} 



return status;
}
}
