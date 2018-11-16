package util;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnector5
{
    
    
   public Connection getPooledConnection()
   {
	   try {
		Class.forName("com.mysql.jdbc.Driver");
	} catch (ClassNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	   Connection conn = null;
	   try {
		conn = DriverManager.getConnection("jdbc:mysql://205.147.102.47:3306/cuberootapp","root","qwerty12@");
	    
	   
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
      return conn;
    }   

}
 

