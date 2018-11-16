package util;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;



public class DBConnector2 {

	Connection conn = null;

	public Connection getPooledConnection(String url) {

	    BasicDataSource bds = new BasicDataSource();
	    bds.setDriverClassName("com.mysql.jdbc.Driver");
	    bds.setUrl(url);
	    bds.setUsername("root");
	    bds.setPassword("qwerty12@");

	    try{
	        System.out.println("Attempting Database Connection");
	        conn = bds.getConnection();
	        System.out.println("Connected Successfully");
	    }catch(SQLException e){
	        System.out.println("Caught SQL Exception: " + e);
	    }
	    return conn;
	}




}
