package com.spidio.UserSegmenter;



import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetMiddlewareData {

	/*
	 public static Map<String,String> sectionMap;
	  public static Map<String,String> articleMap;
	  public static Map<String,String> siteMap;
	  
	  
	  static {
	      Map<String, String> siteMap1 = new HashMap<String,String>();
	      List<Site> sitedata = GetMiddlewareData.getSiteData("1");
	     
	      try {

	         
	         
	        for(Site site: sitedata){

	             try{
	          	 siteMap.put(site.getSiteId(),site.getSiteName());
	             }
	             catch(Exception e)
	             {
	          	     
	            	 e.printStackTrace(); 
	                 continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      siteMap = Collections.unmodifiableMap(siteMap);  
	  
	      //    System.out.println(citycodeMap);
	  }
	
	  

	  static {
	      Map<String, String> sectionMap1 = new HashMap<String,String>();
	      for (Map.Entry<String, String> entry : siteMap.entrySet())
	      {
	       
	      List<Site> sitedata = GetMiddlewareData.getSiteData(entry.getKey());
	     
	      try {

	         
	         
	        for(Site site: sitedata){
	             
	        	try{
	          	 sectionMap1.put(site.getSiteId(),site.getSiteName());
	        	}
	             catch(Exception e)
	             {
	          	     
	            	 e.printStackTrace(); 
	                 continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 
	      
	      }
	      
	      sectionMap = Collections.unmodifiableMap(sectionMap1);  
	  
	      //    System.out.println(citycodeMap);
	  }
	  
	  
	  
	 
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	
	  static {
	      Map<String, String> articleMap1 = new HashMap<String,String>();
	      for (Map.Entry<String, String> entry : siteMap.entrySet())
	      {
	       
	      List<Article> articledata = GetMiddlewareData.getArticleData(entry.getKey());
	     
	      try {

	         
	         
	        for(Article article: articledata){

	             try{
	          	 articleMap1.put(article.getId(), article.getArticleName());
	             }
	             catch(Exception e)
	             {
	          	     
	            	 e.printStackTrace(); 
	                 continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 
	      
	      }
	      
	      articleMap = Collections.unmodifiableMap(articleMap1);  
	  
	      //    System.out.println(citycodeMap);
	  }
	 
	  
	  */
	  
	  
	  
	  
	  
	  
	  
	  
	  
	public static void main(String[] args) throws IOException {
		
	//	String mobilesId = "Sony E5653 Xperia M5 2015_september";
		
	//	GetMiddlewareData.get91mobilesData(mobilesId); 
	//	GetMiddlewareData.getArticleTags("1");
	}
	
	
	
	 

  
   public static List<String> getArticleUrls () {
	   String ServerConnectionURL = "jdbc:mysql://205.147.102.47:3306/middleware";
	 		String DBUser = "root";
	 	    String DBPass = "qwerty12@";
	 	    String DBName = "";
	 		String TABLENAME = "";
	 		Connection con = null;
	 		DBConnector connector1 = new DBConnector();
	 		con = connector1.getPooledConnection(ServerConnectionURL);
	 	    String status1 = "false";
	 	    Statement stmt = null;
	 	    String query0 = null;
	 	    String query1 = null;
	 	//    dateadded = new Timestamp(System.currentTimeMillis()).toString();
	 	  //  System.out.println("Getting Article Data");
	 	   
	 	    String articleid = null;
	 	    String articleurl = null;
	 	    String articlename = null;
	 	    String articletitle = null;
	 	    String siteid = null;
	 	    String author = null;
	 	    String publishdate = null;
	 	    String mainimage = null;
	 	    String tag = null;
	 	    String authorId = null;
	 	    String sectionid= null;
	 	    query0 = "Select * from Article where ArticleUrl Like '%northeast%' AND AdditionTime >= NOW() - INTERVAL 48 HOUR";
	 	    List<String> ArticleUrls = new ArrayList<String>();
 	 	    Statement st = null;
	 		try {
	 			st = con.createStatement();
	 		} catch (SQLException e1) {
	 			// TODO Auto-generated catch block
	 			e1.printStackTrace();
	 		}
	 	      
	 	      // execute the query, and get a java resultset
	 	     ResultSet rs = null;
	 		try {
	 			rs = st.executeQuery(query0);
	 		    
	 		
	 		} catch (SQLException e1) {
	 			// TODO Auto-generated catch block
	 			e1.printStackTrace();
	 		}
	 		
	 		 try {
	 			while(rs.next()){
	 	    	     
	 				
	 			    ArticleUrls.add(rs.getString("ArticleUrl"));
	 				
	 	    	
	 			}    
	 		 }  
	 	      catch(Exception e){
	 	    	  
	 	    	  
	 	      }
	 		 finally {
					
				    DBUtil.close(rs);
					DBUtil.close(st);
					DBUtil.close(con);
				
				}   
	 	         
	 	        
	 	    return ArticleUrls;
		 
 }
  
	 

   public static Product getProductDetails( String url ) {
	   String ServerConnectionURL = "jdbc:mysql://205.147.102.47:3306/middleware";
	 		String DBUser = "root";
	 	    String DBPass = "qwerty12@";
	 	    String DBName = "";
	 		String TABLENAME = "";
	 		Connection con = null;
	 		DBConnector connector1 = new DBConnector();
	 		con = connector1.getPooledConnection(ServerConnectionURL);
	 	    String status1 = "false";
	 	    Statement stmt = null;
	 	    String query0 = null;
	 	    String query1 = null;
	 	//    dateadded = new Timestamp(System.currentTimeMillis()).toString();
	 	    System.out.println("Get Product Data");
	 	   
	 	    String productId = null;
	 	    String productUrl = null;
	 	    String brand = null;
	 	    String description = null;
	 	    String price = null;
	 	    String category = null;
	 	    String seller = null;
	 	    String productImage = null;
	 	    Product product = new Product();
	 	    query0 = "Select * from ProductDetails where ProductUrl like '%"+url+"%'";
	 	    
	 	    Statement st = null;
	 		try {
	 			st = con.createStatement();
	 		} catch (SQLException e1) {
	 			// TODO Auto-generated catch block
	 			e1.printStackTrace();
	 		}
	 	      
	 	      // execute the query, and get a java resultset
	 	     ResultSet rs = null;
	 		try {
	 			rs = st.executeQuery(query0);
	 		    
	 		
	 		} catch (SQLException e1) {
	 			// TODO Auto-generated catch block
	 			e1.printStackTrace();
	 		}
	 		
	 		 try {
	 			while(rs.next()){
	 	    	     
	 				
	 			    productId = rs.getString("ProductId");
	 			    productUrl = rs.getString("ProductUrl"); 	
	 	    	    brand = rs.getString("Brand");
	 			    description = 	rs.getString("Description");
	 			    price = rs.getString("Price");
	 			    category =  rs.getString("EcommerceSegment");
	 			    seller = rs.getString("Seller");
 	 			    productImage = rs.getString("ProductImage");
	 			
	 			   
	 			    product.setSeller(seller);
	 			    product.setCategory(category);
	 			    product.setBrand(brand);
	 			    product.setPrice(price);
	 			    product.setProductImage(productImage);
	 			    product.setProductId(productId);
	 			    product.setProductUrl(productUrl);
	 			    product.setDescription(description);
	 			    
	 			    return product;
	 			    
	 			}    
	 		 }  
	 	      catch(Exception e){
	 	    	  
	 	    	  
	 	      }
	 		 finally {
					
				    DBUtil.close(rs);
					DBUtil.close(st);
					DBUtil.close(con);
				
				}   
	 	         
	 	        
	 		return product;
		 
 } 
   
   
   
   
 
   


}