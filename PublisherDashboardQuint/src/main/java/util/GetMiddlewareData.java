package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.Cookie;

import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.publisherdata.Daos.AggregationModule;
import com.publisherdata.model.Article;
import com.publisherdata.model.DashboardTemplate;
import com.publisherdata.model.Dropdown;
import com.publisherdata.model.DropdownMap;
import com.publisherdata.model.Page;
import com.publisherdata.model.PageMap;
import com.publisherdata.model.Section;
import com.publisherdata.model.Site;
import com.publisherdata.model.TemplateMap;
import com.publisherdata.model.TimeSpan;
import com.publisherdata.model.TimeSpanMap;


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
		//GetMiddlewareData.getDashboardTemplate("1");
	    GetMiddlewareData.getArticleMetaData("http://womansera.com/trending/celerbrity-gossip/deepikas-comment-shahids-picture-will-make-go-aww");
	
	
	}
	
	
	
  

   public static List<Section> getSectionData( String siteid) {
	   
	    String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware";
		String DBUser = "root";
	    String DBPass = "Qwerty12@";
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
	    System.out.println("Getting Section Data");
	    List<Section> section = new ArrayList<Section>();
	    String sectionid = null;
	    String sectionurl = null;
	   
	    query0 = "Select * from Section where siteid="+siteid;
	    
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
	    	
				sectionid = rs.getString("Id");
			    sectionurl = rs.getString("SectionUrl");
			     System.out.println(sectionurl);
	             Section obj = new Section();
	             obj.setSiteId(siteid);
	             obj.setSectionUrl(sectionurl);
	    		 obj.setId(sectionid);
	    	     section.add(obj);
	    	
	    	
			}    
		 }  
	      catch(Exception e){
	      
	    	  
	      }
		 finally {
				
			    DBUtil.close(rs);
				DBUtil.close(st);
				DBUtil.close(con);
			
			}  
	    
		
	        
	    return section;
		 
  }
   
     



   public static List<Article> getArticleData( String siteid ) {
	   String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware";
	 		String DBUser = "root";
	 	    String DBPass = "Qwerty12@";
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
	 	    System.out.println("Getting Article Data");
	 	    List<Article> article = new ArrayList<Article>();
	 	    String articleid = null;
	 	    String articleurl = null;
	 	    String articlename = null;
	 	    query0 = "Select * from Article where siteid="+siteid;
	 	    
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
	 	    	     
	 				articleid = rs.getString("Id");
		 		    articleurl = rs.getString("ArticleUrl");
	 				articlename = rs.getString("ArticleName");
	 				System.out.println(articleurl);
	 	             Article obj = new Article();
	 	             obj.setId(articleid);
	 	             obj.setArticleUrl(articleurl);
	 	    		 obj.setSiteId(siteid);;
	 	    	     obj.setArticleName(articlename);
	 	    		 article.add(obj);
	 	    	
	 	    	
	 			}    
	 		 }  
	 	      catch(Exception e){
	 	    	  
	 	    	  
	 	      }
	 		 finally {
					
				    DBUtil.close(rs);
					DBUtil.close(st);
					DBUtil.close(con);
				
				}   
	 	         
	 	        
	 	    return article;
		 
 }
  
   public static Article getArticleMetaData( String url ) {
	  
	        EhCacheKeyCodeRepository1 ehcache = EhCacheKeyCodeRepository1.getInstance();
	   
	        Article object  = (Article)ehcache.get(url,false);
	        
	        if(object == null || object.equals("")){
	        String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware";
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
	 	    System.out.println("Getting Article Data");
	 	    List<Article> article = new ArrayList<Article>();
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
	 	    String sectionname = null;
	 	    query0 = "Select * from Article where ArticleUrl = '"+url+"'";
	 	    Article obj = new Article();
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
	 	    	     
	 				articleid = rs.getString("Id");
		 		    articleurl = rs.getString("ArticleUrl");
	 				articlename = rs.getString("ArticleName");
	 				siteid = rs.getString("SiteId");
	 			    publishdate = rs.getString("PublishDate");
	 			    articletitle = rs.getString("ArticleTitle");
	 			    author = rs.getString("Author");
	 			    mainimage = rs.getString("MainImage");
	 			    tag =rs.getString("Tags");
	 				authorId = rs.getString("AuthorId");
	 			    sectionid = rs.getString("SectionId");
	 				if(sectionid !=null && !sectionid.isEmpty())
	 			    sectionname = AggregationModule.sectionMap.get(sectionid);
	 			    
	 			   // System.out.println(articleurl);
	 				List<String> tags1 = new ArrayList<String>();
	 				if(tag != null)
	 			    tags1 = Arrays.asList(tag.split("\\s*,\\s*")); 
	 	           
	 				if(articletitle == null || articletitle.isEmpty()) 
	 	            {
	 	            	
	 	            	articletitle = articlename;
	 	            	articletitle = articletitle.replace("-", " ");
	 	            	articletitle = AggregationModule.capitalizeString(articletitle);
	 	            	
	 	            
	 	            
	 	            
	 	            }
	 			     
	 	            List<String> tagIds = new ArrayList<String>();
	 	            
	 	            for(String tagv1:tags1){
	 	            	
	 	            	String tagId = AggregationModule.tagMap3.get(tagv1);
	 	            	tagIds.add(tagId);
	 	            }
	 	            
	 	             if(articleid!=null)
	 	             obj.setId(articleid);
	 	             
	 	             if(articleurl != null)
	 	             obj.setArticleUrl(articleurl);
	 	    		 
	 	             
	 	             if(siteid!=null)
	 	             obj.setSiteId(siteid);
	 	    	     
	 	             if(articlename !=null)
	 	             obj.setArticleName(articlename);
	 	    		 
	 	             if(author !=null && !author.equals("[]"))
	 	             obj.setAuthor(author);
	 	    		 
	 	             if(publishdate != null && !publishdate.equals("None"))
	 	             obj.setPublishdate(publishdate);
	 	    		 
	 	             if(mainimage != null)
	 	             obj.setMainimage(mainimage);
	 	    		 
	 	             
	 	             if(tags1 != null)
	 	             obj.setTags(tags1);
	 	    	    
	 	             if(articletitle != null)
	 	             obj.setArticletitle(articletitle);
	 	    	    
	 	             
	 	             if(sectionid != null)
	 	             obj.setSectionid(sectionid);
	 	    		// article.add(obj);
	 	    	     
	 	             if(authorId !=null)
	 	             obj.setAuthorId(authorId);
	 	    	   
	 	             
	 	             if(sectionname != null)
	 	             obj.setSectionname(sectionname);
	 	    	   
	 	             
	 	             if(tagIds != null)
	 	             obj.setTagIds(tagIds);
	 			}    
	 		 }  
	 	      catch(Exception e){
	 	    	  
	 	    	  
	 	      }
	 		 finally {
					
				    DBUtil.close(rs);
					DBUtil.close(st);
					DBUtil.close(con);
				
				}   
	 		   ehcache.put(url,obj);   
	 		  return obj;
	        
	        }    
	 	    
	        
	        
	       return object; 
	        
 }
  /*
   public String getArticleNamev1( String Id) {
	    
	    String query = "SELECT Entity1,Entity2,Entity3,Entity4,Entity5,Entity6,Entity7,Entity8,Entity9,Entity10,Entity11,Entity12,EntityId FROM entity where EntityId ="+Id+" GROUP BY Entity1,Entity2,Entity3,Entity4,Entity5,Entity6,Entity7,Entity8,Entity9,Entity10,Entity11,Entity12,EntityId";
	    System.out.println(query);
	    CSVResult csvResult = null;
		try {
			csvResult = getCsvResult(false, query);
		} catch (SQLFeatureNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SqlParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvExtractorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    List<String> headers = csvResult.getHeaders();
	    List<String> lines = csvResult.getLines();
	    List<String> articleList = new ArrayList();
	    String articleid = "";
	    String articleurl = "";
	    String articlename = "";
	    String articletitle = "";
	    String siteid = "";
	    String author = "";
	    String publishdate = "";
	    String mainimage = "";
	    String tag = "";
	    String authorId = "";
	    String sectionid= "";
	    String sectionname = null;
	    Article obj = new Article();
	    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
	    {
	     
	 	    
	    	        String[] data = ((String)lines.get(0)).split(",");
	 	    	     
	 			    if(data[0]!=null && !data[0].isEmpty())
		 		    articleurl = data[0];
		 		  
		 		    if(data[1]!=null && !data[1].isEmpty())
		 		    articlename = data[1];
	 				
		 		    if(data[2]!=null && !data[2].isEmpty())
		 		    siteid = data[2];
	 			    
		 		    if(data[4]!=null && !data[4].isEmpty())
		 		    publishdate = data[4];
	 			   
		 		    
		 		    if(data[8]!=null && !data[8].isEmpty())
		 		    articletitle = data[8];
	 			    
		 		    if(data[3]!=null && !data[3].isEmpty()){
		 		    author = data[3];
		 		    authorId = AggregationModule.AuthorMap1.get(author);
	 			    
		 		    }
		 		    
		 		    if(data[6]!=null && !data[6].isEmpty())
		 		    mainimage = data[6];
	 			    
		 		    if(data[5]!=null && !data[5].isEmpty())
		 		    tag = data[5];
	 				
		 		    if(data[11]!=null && !data[11].isEmpty())
		 		    {
		 		    sectionid = data[11];
	 			    sectionname = AggregationModule.sectionMap.get(sectionid);
		 		    }
		 		    
		 		   
		 		    if(data[12]!=null && !data[12].isEmpty())
		 		    {
		 		     articleid=data[12];
		 		    }
		 		    
		 		   
	 			    
	 			   // System.out.println(articleurl);
	 				List<String> tags1 = new ArrayList<String>();
	 				if(tag != null)
	 			    tags1 = Arrays.asList(tag.split("\\s*,\\s*")); 
	 	           
	 				if(articletitle == null || articletitle.isEmpty()) 
	 	            {
	 	            	
	 	            	articletitle = articlename;
	 	            	articletitle = articletitle.replace("-", " ");
	 	            	articletitle = AggregationModule.capitalizeString(articletitle);
	 	            	
	 	            
	 	            
	 	            
	 	            }
	 			     
	 	            List<String> tagIds = new ArrayList<String>();
	 	            
	 	            for(String tagv1:tags1){
	 	            	
	 	            	String tagId = AggregationModule.tagMap3.get(tagv1);
	 	            	if(tagId != null)
	 	            	tagIds.add(tagId);
	 	            }
	 	            
	 	             if(articleid!=null)
	 	             obj.setId(articleid);
	 	             
	 	             if(articleurl != null)
	 	             obj.setArticleUrl(articleurl);
	 	    		 
	 	             
	 	             if(siteid!=null)
	 	             obj.setSiteId(siteid);
	 	    	     
	 	             if(articlename !=null)
	 	             obj.setArticleName(articlename);
	 	    		 
	 	             if(author !=null && !author.equals("[]"))
	 	             obj.setAuthor(author);
	 	    		 
	 	             if(publishdate != null && !publishdate.equals("None"))
	 	             obj.setPublishdate(publishdate);
	 	    		 
	 	             if(mainimage != null)
	 	             obj.setMainimage(mainimage);
	 	    		 
	 	             
	 	             if(tags1 != null && !tags1.isEmpty())
	 	             obj.setTags(tags1);
	 	    	    
	 	             if(articletitle != null)
	 	             obj.setArticletitle(articletitle);
	 	    	    
	 	             
	 	             if(sectionid != null)
	 	             obj.setSectionid(sectionid);
	 	    		// article.add(obj);
	 	    	     
	 	             if(authorId !=null)
	 	             obj.setAuthorId(authorId);
	 	    	   
	 	             
	 	             if(sectionname != null)
	 	             obj.setSectionname(sectionname);
	 	    	   
	 	             
	 	             if(tagIds != null && !tagIds.isEmpty())
	 	             obj.setTagIds(tagIds);
	 			}    
	 		  
	 	     
	 	         
	 	        
	 	    return articletitle;
		 
} 

   */
   
   
   public static Article getArticleName( String articleId ) {
	   String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware";
	 		String DBUser = "root";
	 	    String DBPass = "Qwerty12@";
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
	 	    System.out.println("Getting Article Data");
	 	    List<Article> article = new ArrayList<Article>();
	 	    String articleid = null;
	 	    String articleurl = null;
	 	   
	 	    query0 = "Select * from Article where Id="+articleId;
	 	    
	 	    Statement st = null;
	 		try {
	 			st = con.createStatement();
	 		} catch (SQLException e1) {
	 			// TODO Auto-generated catch block
	 			e1.printStackTrace();
	 		}
	 	      
	 	      // execute the query, and get a java resultset
	 	     ResultSet rs = null;
	 	     Article obj = new Article();
	 	     
	 	     try {
	 			rs = st.executeQuery(query0);
	 		    
	 		
	 		} catch (SQLException e1) {
	 			// TODO Auto-generated catch block
	 			e1.printStackTrace();
	 		}
	 		
	 		 try {
	 			while(rs.next()){
	 	    	     
	 				articleid = rs.getString("Id");
		 		    articleurl = rs.getString("ArticleUrl");
	 				
	 				System.out.println(articleurl);
	 	             
	 	             obj.setId(articleid);
	 	             obj.setArticleUrl(articleurl);
	 	    		 obj.setArticleName(rs.getString("ArticleName"));
	 	    	    
	 	    	
	 	    	
	 			}    
	 		 }  
	 	      catch(Exception e){
	 	    	  
	 	    	  
	 	      }
	 		 finally {
					
				    DBUtil.close(rs);
					DBUtil.close(st);
					DBUtil.close(con);
				
				}    
	 	         
	 	        
	 	    return obj;
		 
 }


   public static Section getSectionName( String sectionId ) {
	   String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware";
		String DBUser = "root";
	    String DBPass = "Qwerty12@";
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
	    System.out.println("Getting Section Data");
	    List<Section> section = new ArrayList<Section>();
	    String sectionid = null;
	    String sectionurl = null;
	    String sectionname = null;
	    query0 = "Select * from Section where Id="+sectionId;
	    
	    Statement st = null;
		try {
			st = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	      
	      // execute the query, and get a java resultset
	     ResultSet rs = null;
	     Section obj = new Section();
	     
	     try {
			rs = st.executeQuery(query0);
		    
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 try {
			while(rs.next()){
	    	
				sectionid = rs.getString("Id");
			    sectionurl = rs.getString("SectionUrl");
			    sectionname = rs.getString("SectionName");
			    System.out.println(sectionurl);
	            
	 
	             obj.setSectionUrl(sectionurl);
	    		 obj.setId(sectionid);
	    	     obj.setSectionName(sectionname);
	    		 section.add(obj);
	    	
	    	
			}    
		 }  
	      catch(Exception e){
	    	  
	    	  
	      }
		 finally {
				
			    DBUtil.close(rs);
				DBUtil.close(st);
				DBUtil.close(con);
			
			}   
	         
	        
	    return obj;
 }

   public static Site getSiteName( String siteId ) {
	   String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware";
		String DBUser = "root";
	    String DBPass = "Qwerty12@";
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
	    System.out.println("Getting Section Data");
	    List<Section> section = new ArrayList<Section>();
	    String siteid = null;
	    String siteurl = null;
	    String sitename = null;
	    query0 = "Select * from Site where Id="+siteId;
	    
	    Statement st = null;
		try {
			st = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	      
	      // execute the query, and get a java resultset
	     ResultSet rs = null;
	     Site obj = new Site();
	     
	     try {
			rs = st.executeQuery(query0);
		    
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 try {
			while(rs.next()){
	    	
				siteid = rs.getString("Id");
			    siteurl = rs.getString("SiteUrl");
			    sitename = rs.getString("SiteName");
			    
	            
	 
	             obj.setSiteId(siteid);
	    		 obj.setSiteName(sitename);
	    	     obj.setSiteurl(siteurl);
	    		
	    	
	    	
			}    
		 }  
	      catch(Exception e){
	    	  
	    	  
	      }
		 finally {
				
			    DBUtil.close(rs);
				DBUtil.close(st);
				DBUtil.close(con);
			
			}  
	         
	        
	    return obj;
 }

   
   public static Site getSiteDetails( String sitename ) {
	   String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware";
		String DBUser = "root";
	    String DBPass = "Qwerty12@";
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
	    System.out.println("Getting Section Data");
	    List<Section> section = new ArrayList<Section>();
	    String siteid = null;
	    String siteurl = null;
//	    String sitename = null;
	    query0 = "Select * from Site where SiteName='"+sitename+"'";
	    
	    Statement st = null;
		try {
			st = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	      
	      // execute the query, and get a java resultset
	     ResultSet rs = null;
	     Site obj = new Site();
	     
	     try {
			rs = st.executeQuery(query0);
		    
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 try {
			while(rs.next()){
	    	
				siteid = rs.getString("Id");
			    siteurl = rs.getString("SiteUrl");
			    sitename = rs.getString("SiteName");
			    
	            
	 
	             obj.setSiteId(siteid);
	    		 obj.setSiteName(sitename);
	    	     obj.setSiteurl(siteurl);
	    		
	    	
	    	
			}    
		 }  
	      catch(Exception e){
	    	  
	    	  
	      }
		 finally {
				
			    DBUtil.close(rs);
				DBUtil.close(st);
				DBUtil.close(con);
			
			}  
	         
	        
	    return obj;
 }

   
   
   
   
   
   
   
   
   
   
   public static List<Site> getSiteData( String userId ) {
	   String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware";
		String DBUser = "root";
	    String DBPass = "Qwerty12@";
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
	    System.out.println("Getting Section Data");
	    List<Site> site = new ArrayList<Site>();
	    String siteid = null;
	    String siteurl = null;
	    String sitename = null;
	    
	    
	    
	    
	    
	    
	    query0 = "Select * from Site where UserId="+userId;
	    
	    Statement st = null;
		try {
			st = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	      
	      // execute the query, and get a java resultset
	     ResultSet rs = null;
	    // Site obj = new Site();
	     
	     try {
			rs = st.executeQuery(query0);
		    
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 try {
			while(rs.next()){
	    	
				siteid = rs.getString("Id");
			    siteurl = rs.getString("SiteUrl");
			    sitename = rs.getString("SiteName");
			    Site obj = new Site();
	            
	 
	             obj.setSiteId(siteid);
	    		 obj.setSiteName(sitename);
	    	     obj.setSiteurl(siteurl);
	    		 site.add(obj);
	    	
	    	
			}    
		 }  
	      catch(Exception e){
	    	  
	    	  
	      }
		 
		 finally {
				
			    DBUtil.close(rs);
				DBUtil.close(st);
				DBUtil.close(con);
			
			}   
	         
	        
	    return site;
 }

   public static DashboardTemplate getDashboardTemplate( String Id ) {
	  
	   EhCacheKeyCodeRepository2 ehcache = EhCacheKeyCodeRepository2.getInstance();
	   DashboardTemplate object  = (DashboardTemplate)ehcache.get(Id,false);
	   if(object == null || object.equals("")){
	   
	   String ServerConnectionURL = "jdbc:mysql://10.100.104.17:3306/middleware1";
		String DBUser = "root";
	    String DBPass = "Qwerty12@";
	    String DBName = "";
		String TABLENAME = "";
		Connection con = null;
		DBConnector connector = new DBConnector();
		con = connector.getPooledConnection(ServerConnectionURL);
	    String status1 = "false";
	    Statement stmt = null;
	    String query0 = null;
	    String query0b = null;
	    String query1 = null;
	    String query2 = null;
	//    dateadded = new Timestamp(System.currentTimeMillis()).toString();
	    System.out.println("Getting Section Data");
	    List<Site> site = new ArrayList<Site>();
	    String id = null;
	    String siteid = null;
	    String siteurl = null;
	    String sitename = null;
	    String dashboardtype= null;
	    String timespan = null;
	    String endpoint = null;
	    String cardTitle =null;
	    String dropdown=null;
	    List<TemplateMap> tempmap = new ArrayList<TemplateMap>();
//	    List<TimeSpanMap> timespanmap = new ArrayList<TimeSpanMap>();
	    List<DropdownMap> dropdownmap = new ArrayList<DropdownMap>();
	    List<Page> pageMap  = new ArrayList<Page>();
	    DashboardTemplate template = new DashboardTemplate();
	    TimeSpanMap timespanmap = new TimeSpanMap();
	    DropdownMap dropdownMap = new DropdownMap();
	    String minutes = null;
	    ResultSet rs = null;
	    ResultSet rs1 = null;
	    ResultSet rs2 = null;
	    ResultSet rs3 = null;
	    Statement st = null;
	    Statement st1 = null;
	    Statement st2 = null;
	    Statement st3 = null;
	    Set<String> set = new HashSet<String>();
	    String pageTitle = null;
	    String pageSubTitle = null;
	    String pageName = null;
	    String parentPageId = null;
	    String pageId = null;
	    String graphTitle= null;
	    String cardImages =null;
	    String APIendpoint=null;
	    String APIfilter = null;
	    
	    try{
		    
			   
			   
			   String query3a = "Select PageId from PageSiteMap where SiteId='"+Id+"'";
				 try {
						st3 = con.createStatement();
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				 
				 
				 try {
						rs3 = st3.executeQuery(query3a);
					    
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				 
				 String value = "";
				 
				 while(rs3.next()){
				  value = rs3.getString("PageId");
				 }
				
				 String Id1 = addCommaString(value);   
			   
			   
			 	   
			   
		    query0b = "Select * from PageMap where PageId in ("+Id1+") And SiteId='"+Id+"'";
		    
		    
			try {
				st = con.createStatement();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		      
		      // execute the query, and get a java resultset
		   
		     
		     try {
				rs = st.executeQuery(query0b);
			    
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			 try {
				while(rs.next()){
					 Page obj = new Page();
					siteid = rs.getString("SiteId");
				    pageName = rs.getString("PageName");
				    pageTitle = rs.getString("PageTitle");
				    pageSubTitle = rs.getString("PageSubTitle");
		            id = rs.getString("PageId");
		            parentPageId = rs.getString("ParentPageId");
		 
		            obj.setPageId(id);
		            obj.setPageName(pageName);
		            obj.setPageSubtitle(pageSubTitle);
		            obj.setPageTitle(pageTitle);
		    		obj.setParentPageId(parentPageId);
		    	    
		    		pageMap.add(obj);
		    	
				}    		 
			 
			 }  
		      catch(Exception e){
		    	  
		    	  
		      }

	    
	    
	    
	    
	    
	    
	    
	  
	    
		   
		   
		   String query0a = "Select TemplateIds from TemplateSiteMap where SiteId='"+Id+"'";
			 try {
					st1 = con.createStatement();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			 
			 
			 try {
					rs1 = st1.executeQuery(query0a);
				    
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			 
			 value = "";
			 
			 while(rs1.next()){
			  value = rs1.getString("TemplateIds");
			 }
			
			 Id1 = addCommaString(value);   
		   
		   
		 	   
		   
	    query0 = "Select * from TemplateMap where Id in ("+Id1+")";
		    
	    
	    
		try {
			st = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	      
	      // execute the query, and get a java resultset
	   
	     
	     try {
			rs = st.executeQuery(query0);
		    
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 try {
			while(rs.next()){
				 TemplateMap obj = new TemplateMap();
				siteid = rs.getString("SiteId");
			    endpoint = rs.getString("endPoint");
			    id = rs.getString("Id");
			    cardTitle = rs.getString("CardTitle");
	            pageId = rs.getString("PageId");
	            graphTitle = rs.getString("GraphTitle");
	            cardImages = rs.getString("CardImages");
	            
	    		 obj.
	    		 setEndpoint(endpoint);
	    		 obj.setDisplay(cardTitle);
	    	     obj.setPageId(pageId);
	    	     obj.setGraphTitle(graphTitle);
	    	     obj.setCardImages(cardImages);
	    	     tempmap.add(obj);
	    	
			}    		 
		 
		 }  
	      catch(Exception e){
	    	  
	    	  
	      }

		 
		 
		 
		 String query1a = "Select TimeSpanIds from TimeSpanSiteMap where SiteId='"+Id+"'";
		 try {
				st1 = con.createStatement();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		 
		 
		 try {
				rs1 = st1.executeQuery(query1a);
			    
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		 
		 while(rs1.next()){
		 value = rs1.getString("TimeSpanIds");
		 }
		 
		 
		 String Id2 = addCommaString(value);
		 
		 
		 query1 = "Select * from TimeSpan where Id in ("+Id2+")";
		    
		    
		
		
		    
		 
		 
			try {
				st1 = con.createStatement();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		      
		      // execute the query, and get a java resultset
		    
			 Map<String, List<TimeSpan>> timespanmap1 = new HashMap<String,List<TimeSpan>>();
			 List<TimeSpanMap> tsmap = new ArrayList<TimeSpanMap>();
		     
		    
		     try {
				rs1 = st1.executeQuery(query1);
			    
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			 try {
				while(rs1.next()){
					
					siteid = rs1.getString("SiteId");
					id = rs1.getString("Id");
				    
				    dashboardtype = rs1.getString("DashBoardType");
				    timespan = rs1.getString("Timespan");
				    minutes = rs1.getString("Minutes");
				 //   set.add(dashboardtype);
				    TimeSpan obj1 = new TimeSpan();
						    
				    obj1.setTimespan(timespan);
				    obj1.setId(id);
				    obj1.setMinutes(minutes);
				    
				        
				    if(timespanmap1.containsKey(dashboardtype.trim())==false)  
				    {
				    	List<TimeSpan> tspan = new ArrayList<TimeSpan>();
					    tspan.add(obj1);
				    	timespanmap1.put(dashboardtype.trim(),tspan);
				    	
				    }
				    else{
				    	
				    	List<TimeSpan> tspan1 =  timespanmap1.get(dashboardtype.trim());
				    	tspan1.add(obj1);
				    	timespanmap1.put(dashboardtype.trim(),tspan1);
				    }
		    		
		    		
		    	
				}    		 
		
				
				
				for (Map.Entry<String, List<TimeSpan>> entry : timespanmap1.entrySet())
				{
				    
					TimeSpanMap tmap1 = new TimeSpanMap();
					tmap1.setDashboardType(entry.getKey());
					tmap1.setTimeSpan(entry.getValue());
					tsmap.add(tmap1);
				}	
				
			 
			   timespanmap.setTimeSpanValues(tsmap);
			   
			 }  
		      catch(Exception e){
		    	  
		    	  
		      }
		 
		 
		 
			 String query2a = "Select DropdownIds from DropdownSiteMap where SiteId='"+Id+"'";
			 try {
					st1 = con.createStatement();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			 
			 
			 try {
					rs1 = st1.executeQuery(query2a);
				    
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			 
			 while(rs1.next()){
			 value = rs1.getString("DropdownIds");
			 }
			 
			 String Id3 = addCommaString(value);
			 
			 
			 query2 = "Select * from DropdownMap where Id in ("+Id3+")";
		    
			    
			   
				try {
					st2= con.createStatement();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			      
			      // execute the query, and get a java resultset
			   
				 Map<String, List<Dropdown>> dropdownmap1 = new HashMap<String,List<Dropdown>>();
				 
			     List<DropdownMap> dropdowns = new ArrayList<DropdownMap>();
			   
			     try {
					rs2 = st2.executeQuery(query2);
				    
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				 try {
					while(rs2.next()){
			    	    Dropdown obj = new Dropdown(); 
						id = rs2.getString("Id");
					    siteid = rs2.getString("siteId");
					    pageId = rs2.getString("pageId");
					    dropdown = rs2.getString("Dropdown");
					    endpoint = rs2.getString("endpoint");
					    APIendpoint = rs2.getString("APIendpoint");
					    APIfilter = rs2.getString("APIfilter"); 
			            obj.setDropdown(dropdown);
			            obj.setId(id);
			            obj.setPageId(pageId);
			            obj.setAPIendpoint(APIendpoint);
			            obj.setAPIfilter(APIfilter);
			            
			            if(dropdownmap1.containsKey(endpoint)==false)  
					    {
					    	List<Dropdown> dropdown1 = new ArrayList<Dropdown>();
						    dropdown1.add(obj);
					    	dropdownmap1.put(endpoint,dropdown1);
					    	
					    }
					    else{
					    	
					    	List<Dropdown> dropdown2 =  dropdownmap1.get(endpoint);
					    	dropdown2.add(obj);
					    	dropdownmap1.put(endpoint,dropdown2);
					    }
			    		
			            
					}    		 
				 
					 for (Map.Entry<String, List<Dropdown>> entry : dropdownmap1.entrySet())
						{
						    
							DropdownMap dmap1 = new DropdownMap();
							dmap1.setEndpoint(entry.getKey());
							dmap1.setDropdown(entry.getValue());
							dropdowns.add(dmap1);
						}	
			            
					
					
					dropdownMap.setDropdownValues(dropdowns);
					
					
				 }  
			      catch(Exception e){
			    	  
			    	  
			      }
		 
		 
		 
		     template.setDropdowntemplate(dropdownMap);
		     template.setCardtitletemplate(tempmap);
		     template.setTimespantemplate(timespanmap);
		     template.setPageMap(pageMap);
	   }
	   catch(Exception e){
		   
		   
	   }
		 
		 
		 finally{
				
			    DBUtil.close(rs);
				DBUtil.close(st);
				
				 DBUtil.close(rs1);
					DBUtil.close(st1);
					 DBUtil.close(rs2);
						DBUtil.close(st2);
						DBUtil.close(con);	
		 }
			   
	   
	    ehcache.put(Id,template);    
	    return template;
	   }
       return object;
   
   }
   
 
   
   public static String getAuthorMap( ) {
	    String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware1";
		String DBUser = "root";
	    String DBPass = "Qwerty12@";
	    String DBName = "";
		String TABLENAME = "";
		Connection con = null;
		DBConnector1 connector1 = new DBConnector1();
		con = connector1.getPooledConnection(ServerConnectionURL);
	    String status1 = "false";
	    Statement stmt = null;
	    String query0 = null;
	    String query1 = null;
	//    dateadded = new Timestamp(System.currentTimeMillis()).toString();
	    System.out.println("Getting Author Data");
	    List<Section> section = new ArrayList<Section>();
	    String authorid = null;
	    String sectionurl = null;
	    String authorname = null;
	    Map<String, String> authorMap1  = new HashMap<String,String>();
	    Map<String, String> authorMap2  = new HashMap<String,String>();
	    
	    query0 = "Select * from Author";
	    
	    Statement st = null;
		try {
			st = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	      
	      // execute the query, and get a java resultset
	     ResultSet rs = null;
	     Section obj = new Section();
	     
	     try {
			rs = st.executeQuery(query0);
		    
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 try {
			while(rs.next()){
	    	
				authorid = rs.getString("Id");
			   
			    authorname = rs.getString("AuthorName");
			    
			 
			      String line5 = "";
			      String cvsSplitBy5 = ",";
			      String key2 = "";
			     
			      authorMap1.put(authorid,authorname);
			      authorMap2.put(authorname,authorid);
			             

			        
			      
			      }

		 }      
			      
			      
			catch(Exception e){
				
				e.printStackTrace();
			} 
	    	
	    	
		AggregationModule.AuthorMap	=   authorMap1;
	    AggregationModule.AuthorMap1 =   authorMap2;
	         
	    if (authorMap1!=null && authorMap2!=null)
	    return "Map Loaded";
	    		
	    		
	    return "Map Not Loaded";
 }
   
   
   
   
   public static String getTagmap( ) {
	   String ServerConnectionURL = "jdbc:mysql://192.168.101.198:3306/middleware1";
		String DBUser = "root";
	    String DBPass = "Qwerty12@";
	    String DBName = "";
		String TABLENAME = "";
		Connection con = null;
		DBConnector1 connector1 = new DBConnector1();
		con = connector1.getPooledConnection(ServerConnectionURL);
	    String status1 = "false";
	    Statement stmt = null;
	    String query0 = null;
	    String query1 = null;
	//    dateadded = new Timestamp(System.currentTimeMillis()).toString();
	    System.out.println("Getting Tag Data");
	    List<Section> section = new ArrayList<Section>();
	    String tagid = null;
	    String sectionurl = null;
	    String tagname = null;
	    Map<String, String> tagMap1  = new HashMap<String,String>();
	    Map<String, String> tagMap2  = new HashMap<String,String>();
	    
	    query0 = "Select * from Tag";
	    
	    Statement st = null;
		try {
			st = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	      
	      // execute the query, and get a java resultset
	     ResultSet rs = null;
	     Section obj = new Section();
	     
	     try {
			rs = st.executeQuery(query0);
		    
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 try {
			while(rs.next()){
	    	
				tagid = rs.getString("Id");
			   
			    tagname = rs.getString("Tag");
			    
			 
			      String line5 = "";
			      String cvsSplitBy5 = ",";
			      String key2 = "";
			     
			      tagMap1.put(tagid,tagname);
			      tagMap2.put(tagname,tagid);
			             

			        
			      
			      }

		 }      
			      
			      
			catch(Exception e){
				
				e.printStackTrace();
			} 
	    	
	    	
		AggregationModule.tagMap	=  tagMap1;
	    AggregationModule.tagMap1 =  tagMap2;
	    AggregationModule.tagMap2	=   tagMap1;
	    AggregationModule.tagMap3 =  tagMap2;  
	    
	    
	    if (tagMap1!=null && tagMap2!=null)
	    return "Map Loaded";
	    		
	    		
	    return "Map Not Loaded";
 }
   
   
   
   
 
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   public static String addCommaString(String value) {
	    String res = "";
	    String [] parts = value.split(",");
	    for(int i =0; i<parts.length; i++){
	    	
	    	res = res+"'"+parts[i]+"'"+",";
	    	
	    }
       res = res.substring(0,res.length()-1);
      return res;
 }
   


}