package com.spidio.UserSegmenter;





import java.sql.Connection;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

public class DBConnector2
{
    
    
   public Connection getPooledConnection(String url)
   {
       try
       {
          Context initCtx = new InitialContext();
          if (initCtx == null ) { throw new Exception("Boom - No Context"); }
          Context envCtx = (Context) initCtx.lookup("java:comp/env");
          DataSource ds = (DataSource) envCtx.lookup("jdbc/eros");
          if (ds == null ) { throw new Exception("Boom - No DataSource"); }  
          return ds.getConnection();          
        }
       catch(Exception e)
       {
          //e.printStackTrace();
          throw new RuntimeException("Database Not available");
       }     
      
    }   

}