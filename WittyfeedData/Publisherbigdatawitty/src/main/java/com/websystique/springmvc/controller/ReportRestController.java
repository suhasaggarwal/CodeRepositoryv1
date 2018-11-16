package com.websystique.springmvc.controller;

import java.net.URLDecoder;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import util.EncryptionModule;
import util.GetMiddlewareData;

import com.publisherdata.Daos.AggregationModule;
import com.publisherdata.model.Article;
import com.publisherdata.model.ArticleResponse;
import com.publisherdata.model.DashboardTemplate;
import com.publisherdata.model.LocationResponse;
import com.publisherdata.model.PublisherReport;
import com.publisherdata.model.Response;
import com.publisherdata.model.Section;
import com.publisherdata.model.SectionResponse;
import com.publisherdata.model.Site;
import com.publisherdata.model.SiteResponse;
import com.publisherdata.model.User;
import com.spring.service.UserDetailsDAO;
import com.websystique.springmvc.model.Reports;
import com.websystique.springmvc.service.ReportService;

@RestController
public class ReportRestController {

	@Autowired
	ReportService reportService; // Service which will do all data
									// retrieval/manipulation work

	// -------------------Retrieve Report with
	// Id--------------------------------------------------------
	/*
	 * @RequestMapping(value = "/report/{id}/{dateRange}/{channel_name}", method
	 * = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE) public
	 * ResponseEntity<List<PublisherReport>> getReport(@PathVariable("id") long
	 * id,@PathVariable("dateRange") String
	 * dateRange,@PathVariable("channel_name") String channel_name){
	 * System.out.println("Fetching Report with id " + id);
	 * List<PublisherReport> report =
	 * reportService.extractReportsChannelNames(id,dateRange,channel_name); if
	 * (report == null) { System.out.println("Report with id " + id +
	 * " not found"); return new
	 * ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND); } return new
	 * ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK); }
	 */
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
	          	 siteMap1.put(site.getSiteId(),site.getSiteName());
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

	      
	      siteMap = Collections.unmodifiableMap(siteMap1);  
	  
	      //    System.out.println(citycodeMap);
	  }
	
	  

	  static {
	      Map<String, String> sectionMap1 = new HashMap<String,String>();
	      
	      for (Map.Entry<String, String> entry : siteMap.entrySet())
	      {
	    	  try {
	      List<Section> sectiondata = GetMiddlewareData.getSectionData(entry.getKey());
	     
	      

	         
	         
	        for(Section section: sectiondata){
	             
	        	try{
	          	 sectionMap1.put(section.getId(),section.getSectionName());
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
	   continue;
	} 
	      
	      }       
	      
	      sectionMap = Collections.unmodifiableMap(sectionMap1);  
	  
	      //    System.out.println(citycodeMap);
	  }
	  
	  
	  
	 
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	
	  static {
	      Map<String, String> articleMap1 = new HashMap<String,String>();
	     
	     
	      for (Map.Entry<String, String> entry : siteMap.entrySet())
	      {
	      
	      try {  
	      List<Article> articledata = GetMiddlewareData.getArticleData(entry.getKey());
	     
	      

	         
	         
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
	    continue;
	 
	} 
	      
	      }     
	      
	      articleMap = Collections.unmodifiableMap(articleMap1);  
	  
	      //    System.out.println(citycodeMap);
	  }
	
	*/
	
	
	
	
	
	
	@CrossOrigin
	@RequestMapping(value = "/report/{id}/{dateRange}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReport(
			@PathVariable("id") long id,
			@PathVariable("dateRange") String dateRange,
			HttpServletRequest request) {

		List<PublisherReport> report = null;

		String token;
		String userdetails;
		String[] userinfo;
		String emailId = null;
		String status = null;
		User user = new User();
		Cookie[] cookies = request.getCookies();
		if (cookies != null) {
			for (int i = 0; i < cookies.length; i++) {
				if (cookies[i].getName().equals("AUTHTOKEN")) {
					// Fetch User details and return in json format
					token = cookies[i].getValue();
					userdetails = EncryptionModule.decrypt(null, null,
							URLDecoder.decode(token));
					userinfo = userdetails.split(":");
					emailId = userinfo[0];
				}
			}

		}

		if (emailId.equalsIgnoreCase("vinay.rajput@cuberoot.co")) {

			System.out.println("Fetching Report with id " + id);
			report = reportService.extractReportsChannel(id, dateRange,
					"Adda52");
			if (report == null) {
				System.out.println("Report with id " + id + " not found");
				return new ResponseEntity<List<PublisherReport>>(
						HttpStatus.NOT_FOUND);
			}

		} else {

			System.out.println("Fetching Report with id " + id);
			report = reportService.extractReports(id, dateRange);
			if (report == null) {
				System.out.println("Report with id " + id + " not found");
				return new ResponseEntity<List<PublisherReport>>(
						HttpStatus.NOT_FOUND);

			}
		}

		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	}

	@CrossOrigin
	@RequestMapping(value = "/report/{id}/{dateRange}/{channel}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReport(
			@PathVariable("id") long id,
			@PathVariable("dateRange") String dateRange,
			@PathVariable("channel") String channel_name) {
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report = reportService.extractReportsChannel(id,
				dateRange, channel_name);
		if (report == null) {
			System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(
					HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	}

	@CrossOrigin
	@RequestMapping(value = "/report/{id}/{dateRange}/channelList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Set<String>> getList(@PathVariable("id") long id,
			@PathVariable("dateRange") String dateRange,
			HttpServletRequest request) {

		String token;
		String userdetails;
		String[] userinfo;
		String emailId = null;
		String status = null;
		User user = new User();
		Cookie[] cookies = request.getCookies();
		if (cookies != null) {
			for (int i = 0; i < cookies.length; i++) {
				if (cookies[i].getName().equals("AUTHTOKEN")) {
					// Fetch User details and return in json format
					token = cookies[i].getValue();
					userdetails = EncryptionModule.decrypt(null, null,
							URLDecoder.decode(token));
					userinfo = userdetails.split(":");
					emailId = userinfo[0];
				}
			}

		}

		System.out.println("Fetching Report with id " + id);
		Set<String> list = reportService.extractChannelNames(id, dateRange);
		if (list == null) {
			System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<Set<String>>(HttpStatus.NOT_FOUND);
		}

		if (emailId.equalsIgnoreCase("vinay.rajput@cuberoot.co")) {
			list.clear();
			list.add("momagic");
			list.add("opera");
			list.add("cricbuzz");
			list.add("cricbuzz_mob");
			list.add("taboola");
			list.add("forkmedia");
			list.add("inuxu_native");
			list.add("ixigo");
			list.add("spidio");
			list.add("shopclues");
			list.add("espn");
			list.add("gamooga");

		}

		return new ResponseEntity<Set<String>>(list, HttpStatus.OK);
	}

	/*
	 * 
	 * @RequestMapping(value =
	 * "/report/v1/{id}/ArticleAPIs/{dateRange}/{channel}/{articlename}", method
	 * = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE) public
	 * ResponseEntity<List<PublisherReport>>
	 * getReportArticle(@PathVariable("id") long id,@PathVariable("dateRange")
	 * String dateRange,@PathVariable("channel") String
	 * channel_name,@PathVariable("articlename") String articlename) {
	 * System.out.println("Fetching Report with id " + id);
	 * List<PublisherReport> report =
	 * reportService.extractReportsChannelArticle(
	 * id,dateRange,channel_name,articlename); if (report == null){
	 * System.out.println("Report with id " + id + " not found"); return new
	 * ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND); } return new
	 * ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK); }
	 */
/*
	@CrossOrigin
	@RequestMapping(value = "/report/v1/{QueryField}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getReportqueryField(
			@PathVariable("QueryField") String queryfield,
			@RequestParam("dateRange") String dateRange,
			@RequestParam("siteId") String siteId,
			@RequestParam(value = "sectionid", required = false) String sectionId,
			@RequestParam(value = "articleid", required = false) String articleid,
			@RequestParam(value = "city", required = false) String city,
			@RequestParam(value = "gender", required = false) String gender,
			@RequestParam(value = "agegroup", required = false) String agegroup,
			@RequestParam(value = "audience_segment", required = false) String audience_segment,
			@RequestParam(value = "income_level", required = false) String income_level,
			@RequestParam(value = "device_type", required = false) String device_type,
			@RequestParam(value = "live", required = false) String live,
			@RequestParam(value = "filter", required = false) String filter,
			@RequestParam(value = "Aggregateby", required = false) String group_by) {

		AggregationModule module = AggregationModule.getInstance();
		try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Response response = new Response();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

		Response response1 = new Response();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");

		Site obj = GetMiddlewareData.getSiteName(siteId);
		Section obj1 = null;
		Article obj2 = null;

		if (sectionId != null && !sectionId.isEmpty()) {

			obj1 = GetMiddlewareData.getSectionName(sectionId);
			sectionId = obj1.getSectionName();
		}
		if (articleid != null && !articleid.isEmpty()) {
			obj2 = GetMiddlewareData.getArticleName(articleid);
			articleid = obj2.getArticleName();
		}

		siteId = obj.getSiteName();

		List<PublisherReport> report = null;
		Map<String, String> FilterMap = new HashMap<String, String>();
		List<String> groupby = new ArrayList<String>();
		String[] groupbyparts;
		if (group_by != null && group_by.isEmpty() == false) {

			groupbyparts = group_by.split("~");

			for (int i = 0; i < groupbyparts.length; i++) {

				groupby.add(groupbyparts[i]);

			}
		}

		
		
		if (live!=null && !live.isEmpty() && live.equals("true")) {

			String starttimestamp = "";
			String endtimestamp = "";
			// String dateRange = "";
			if (dateRange.equals("Last_24_Hours")) {

				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				endtimestamp = sdf.format(cal.getTime()).toString();
				Calendar calendar = Calendar.getInstance();
				calendar.add(Calendar.HOUR_OF_DAY, -24);
				starttimestamp = sdf.format(calendar.getTime()).toString();

				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");
			}

			else if (dateRange.equals("Last_15_minutes")) {
				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				endtimestamp = sdf.format(cal.getTime()).toString();

				cal.add(Calendar.MINUTE, -420);
				starttimestamp = sdf.format(cal.getTime()).toString();
				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");

			}

			
			else{
				
			    String [] parts = dateRange.split("_");
			    starttimestamp = parts[0]+" 00:00:01";
			    endtimestamp = parts[1] +" 23:59:59";
				dateRange = starttimestamp + "_" + endtimestamp;		
				
				
			}
			// if((dateRange !=null && dateRange.isEmpty()==false)&& (siteId
			// !=null && siteId.isEmpty()==false) && ((city !=null &&
			// city.isEmpty()==false) || (gender !=null &&
			// gender.isEmpty()==false) || (agegroup !=null &&
			// agegroup.isEmpty()==false) || (audience_segment !=null &&
			// audience_segment.isEmpty()==false) || (income_level !=null &&
			// income_level.isEmpty()==false) || (device_type !=null &&
			// device_type.isEmpty()==false)) )
			if (city != null && !city.isEmpty()) {
				city = AggregationModule.citycodeMap2.get(city);
				FilterMap.put("city", city);
			}

			if (gender != null && !gender.isEmpty())
				FilterMap.put("gender", gender);

			if (agegroup != null && !agegroup.isEmpty())
				FilterMap.put("agegroup", agegroup);

			if (audience_segment != null && !audience_segment.isEmpty()){
				audience_segment = AggregationModule.audienceSegmentMap1.get(audience_segment);
				//Should contain audience Code
				FilterMap.put("audience_segment", audience_segment);

			}	
				
			if (income_level != null && !income_level.isEmpty())
				FilterMap.put("income_level", income_level);

			if (device_type != null && !device_type.isEmpty())
				FilterMap.put("device_type", device_type);

			if (dateRange != null && dateRange.isEmpty() == false) {

				if (siteId != null && siteId.isEmpty() == false) {

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId == null || sectionId.isEmpty()) {
								if (articleid == null || articleid.isEmpty()) {

									String[] dateInterval = dateRange
											.split("_");

									try {
										report = module
												.getQueryFieldChannelLive(
														queryfield,
														dateInterval[0],
														dateInterval[1],
														siteId,filter);
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

									response.setData(report);
									if(report == null || report.isEmpty())
									{
										if(queryfield.equals("city"))
										response.setMessage("Location Not available");
										
										if(queryfield.equals("audience_segment"))
										response.setMessage("Segment Not available");
									
									
									
									}
									return new ResponseEntity<Response>(
											response, HttpStatus.OK);

								}

							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId != null && !sectionId.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSection(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() > 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId != null && !sectionId.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSectionFilter(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId, FilterMap);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticle(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() > 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticleFilter(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid, FilterMap);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);
							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId == null || sectionId.isEmpty()) {

								if (articleid == null || articleid.isEmpty()) {

									String[] dateInterval = dateRange
											.split("_");

									try {
										report = module
												.getQueryFieldChannelLive(
														queryfield,
														dateInterval[0],
														dateInterval[1],
														siteId,filter);
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

									response.setData(report);
									if(report == null || report.isEmpty())
									{
										if(queryfield.equals("city"))
										response.setMessage("Location Not available");
										
										if(queryfield.equals("audience_segment"))
										response.setMessage("Segment Not available");
									
									
									
									}
									return new ResponseEntity<Response>(
											response, HttpStatus.OK);

								}

							}

						}
					}

					if (FilterMap.size() > 0) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelFilterLive(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													FilterMap,filter);
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
								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelGroupByLive(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													groupby,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId != null && !sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSectionGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId, groupby,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticleGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid, groupby, filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

				}

			}
		}

		else {

			// if((dateRange !=null && dateRange.isEmpty()==false)&& (siteId
			// !=null && siteId.isEmpty()==false) && ((city !=null &&
			// city.isEmpty()==false) || (gender !=null &&
			// gender.isEmpty()==false) || (agegroup !=null &&
			// agegroup.isEmpty()==false) || (audience_segment !=null &&
			// audience_segment.isEmpty()==false) || (income_level !=null &&
			// income_level.isEmpty()==false) || (device_type !=null &&
			// device_type.isEmpty()==false)) )

			if (city != null && !city.isEmpty()) {
				city = AggregationModule.citycodeMap2.get(city);
				FilterMap.put("city", city);
			}

			if (gender != null && !gender.isEmpty())
				FilterMap.put("gender", gender);

			if (agegroup != null && !agegroup.isEmpty())
				FilterMap.put("agegroup", agegroup);

			if (audience_segment != null && !audience_segment.isEmpty()){
				audience_segment = AggregationModule.audienceSegmentMap1.get(audience_segment);
				//Should contain audience Code
				FilterMap.put("audience_segment", audience_segment);

			}
				
			if (income_level != null && !income_level.isEmpty())
				FilterMap.put("income_level", income_level);

			if (device_type != null && !device_type.isEmpty())
				FilterMap.put("device_type", device_type);

			if (dateRange != null && dateRange.isEmpty() == false) {

				if (siteId != null && siteId.isEmpty() == false) {

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId == null || sectionId.isEmpty()) {
								if (articleid == null || articleid.isEmpty()) {

									String[] dateInterval = dateRange
											.split("_");

									try {
										report = module.getQueryFieldChannel(
												queryfield, dateInterval[0],
												dateInterval[1],
												siteId,filter);
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

									response.setData(report);
									if(report == null || report.isEmpty())
									{
										if(queryfield.equals("city"))
										response.setMessage("Location Not available");
										
										if(queryfield.equals("audience_segment"))
										response.setMessage("Segment Not available");
									
									
									
									}
									return new ResponseEntity<Response>(
											response, HttpStatus.OK);

								}

							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId != null && !sectionId.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSection(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() > 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId != null && !sectionId.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSectionFilter(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId, FilterMap);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticle(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() > 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticleFilter(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid, FilterMap);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);
							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId == null || sectionId.isEmpty()) {

								if (articleid == null || articleid.isEmpty()) {

									String[] dateInterval = dateRange
											.split("_");

									try {
										report = module.getQueryFieldChannel(
												queryfield, dateInterval[0],
												dateInterval[1],
												siteId,filter);
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

									response.setData(report);
									if(report == null || report.isEmpty())
									{
										if(queryfield.equals("city"))
										response.setMessage("Location Not available");
										
										if(queryfield.equals("audience_segment"))
										response.setMessage("Segment Not available");
									
									
									
									}
									return new ResponseEntity<Response>(
											response, HttpStatus.OK);

								}

							}

						}
					}

					if (FilterMap.size() > 0) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module.getQueryFieldChannelFilter(
											queryfield, dateInterval[0],
											dateInterval[1],
											siteId,
											FilterMap);
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
								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													groupby,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId != null && !sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSectionGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId, groupby,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticleGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid, groupby, filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

				}

			}

		}

		if (report == null) {
			response1.setData(report);
			return new ResponseEntity<Response>(response1, HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity<Response>(response, HttpStatus.OK);
	}

	*/
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/{QueryField}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Response> getReportqueryField(
			@PathVariable("QueryField") String queryfield,
			@RequestParam("dateRange") String dateRange,
			@RequestParam("siteId") String siteId,
			@RequestParam(value = "sectionid", required = false) String sectionId,
			@RequestParam(value = "articleid", required = false) String articleid,
			@RequestParam(value = "city", required = false) String city,
			@RequestParam(value = "gender", required = false) String gender,
			@RequestParam(value = "agegroup", required = false) String agegroup,
			@RequestParam(value = "audience_segment", required = false) String audience_segment,
			@RequestParam(value = "incomelevel", required = false) String income_level,
			@RequestParam(value = "devicetype", required = false) String device_type,
			@RequestParam(value = "tag", required = false) String tag,
			@RequestParam(value = "authorId", required = false) String authorName,
			@RequestParam(value = "referrerType", required = false) String referrerType,
			@RequestParam(value = "subcategory", required = false) String subcategory,
			@RequestParam(value = "cookie_id", required = false) String cookie_id,
			@RequestParam(value = "sourceUrl", required = false) String sourceUrl,
			@RequestParam(value = "state", required = false) String state,
			@RequestParam(value = "country", required = false) String country,
			@RequestParam(value = "live", required = false) String live,
			@RequestParam(value = "filter", required = false) String filter,
			@RequestParam(value = "limit", required = false) String limit,
			@RequestParam(value = "filtertype", required = false) String filtertype,
			@RequestParam(value = "engagementType", required = false) String engagementType,
			@RequestParam(value = "Aggregateby", required = false) String group_by) {

		AggregationModule module = AggregationModule.getInstance();
		
		
		try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Response response = new Response();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

		Response response1 = new Response();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");

		//Site obj = GetMiddlewareData.getSiteName(siteId);
		Section obj1 = null;
		Article obj2 = null;

		String citylist = "";
		String statelist = "";
		String sourceurlList = "";
		String parts[];
		String countrylist = "";
		String genderlist = "";
		String agelist = "";
		String audiencesegmentlist = "";
		String subcategorylist = "";
		String cookielist= "";
		String incomelist = "";
		String referrerTypelist = "";
		String devicelist = "";
		String authorlist= "";
        String taglist = "";
		String [] dateRange1;
		
		if (sectionId != null && !sectionId.isEmpty()) {

			sectionId = AggregationModule.sectionMap.get(sectionId);
			
		}
		if (articleid != null && !articleid.isEmpty()) {
			articleid = AggregationModule.getArticleName(articleid);
		//	articleid = obj2.getArticleName();
		}

	//	siteId = obj.getSiteName();
		
		if(siteId.equals("2"))
		siteId = "wittyfeed";
		
		if(siteId.equals("3"))
	    siteId = "duniadigest";
		
		if(siteId.equals("4"))
			siteId = "geeksmate";
		
		if(siteId.equals("5"))
			siteId = "foodmate";
		
		if(siteId.equals("6"))
			siteId = "innervoice";
		
		if(siteId.equals("1"))
			siteId = "wittyfeed_india";
		
		List<PublisherReport> report = null;
		Map<String, String> FilterMap = new HashMap<String, String>();
		List<String> groupby = new ArrayList<String>();
		String[] groupbyparts;
		if (group_by != null && group_by.isEmpty() == false) {

			groupbyparts = group_by.split("~");

			for (int i = 0; i < groupbyparts.length; i++) {

				groupby.add(groupbyparts[i]);

			}
		}

		
		
		if (live!=null && !live.isEmpty() && live.equals("true")) {

			String starttimestamp = "";
			String endtimestamp = "";
			// String dateRange = "";
			
			if (dateRange.equals("Last_24_Hours")) {

				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				endtimestamp = sdf.format(cal.getTime()).toString();
				Calendar calendar = Calendar.getInstance();
				calendar.add(Calendar.HOUR_OF_DAY, -24);
				starttimestamp = sdf.format(calendar.getTime()).toString();

				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");
			}

			else if (dateRange.equals("Last_15_minutes")) {
				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				endtimestamp = sdf.format(cal.getTime()).toString();

				cal.add(Calendar.MINUTE, -360);
				starttimestamp = sdf.format(cal.getTime()).toString();
				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");

			}
			
			else if (dateRange.equals("10")) {

				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				endtimestamp = sdf.format(cal.getTime()).toString();
				Calendar calendar = Calendar.getInstance();
				calendar.add(Calendar.HOUR_OF_DAY, -24);
				starttimestamp = sdf.format(calendar.getTime()).toString();

				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");
			}

			else if (dateRange.equals("7")) {
				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				endtimestamp = sdf.format(cal.getTime()).toString();

				cal.add(Calendar.MINUTE, -420);
				starttimestamp = sdf.format(cal.getTime()).toString();
				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");

			}

			
			else if (dateRange.equals("8")) {
				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				endtimestamp = sdf.format(cal.getTime()).toString();

				cal.add(Calendar.MINUTE, -360);
				starttimestamp = sdf.format(cal.getTime()).toString();
				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");

			}
			
				
			else if (dateRange.equals("9")) {
				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				endtimestamp = sdf.format(cal.getTime()).toString();

				cal.add(Calendar.MINUTE, -420);
				starttimestamp = sdf.format(cal.getTime()).toString();
				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");

			}
			
			else if (dateRange.equals("4")){
				
				    Date date = new Date();
				    Calendar c = Calendar.getInstance();
				    c.setTime(date);
				    int i = c.get(Calendar.DAY_OF_WEEK) - c.getFirstDayOfWeek();
				    c.add(Calendar.DATE, -i - 7);
				    DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				    
				    starttimestamp = sdf.format(c.getTime()).toString();
				   
				    
				    c.add(Calendar.DATE, 6);
				    endtimestamp = sdf.format(c.getTime()).toString();
				    dateRange = starttimestamp + "_" + endtimestamp;
					dateRange = dateRange.replace("/", "-");
				
				
				
				
			}
			
			else if (dateRange.equals("3")){
				
				
				
				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
				
				
				Calendar cal = Calendar.getInstance();
			    cal.add(Calendar.DATE, -1);
			   
			    endtimestamp = sdf.format(cal.getTime()).toString();
			    
			    starttimestamp = endtimestamp;
			    
			    dateRange = starttimestamp +" 00:00:01" + "_" + endtimestamp+" 23:59:59";
				dateRange = dateRange.replace("/", "-");
			
			
			
			
		}
		
			else if (dateRange.equals("5")){
				
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.MONTH, -1);
				cal.set(Calendar.DATE, 1);
				DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			    
				starttimestamp = sdf.format(cal.getTime()).toString();
				   

				cal.set(Calendar.DATE, cal.getActualMaximum(Calendar.DATE)); // changed calendar to cal

				endtimestamp = sdf.format(cal.getTime()).toString();
				dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");
				
			
			
			
		}
		
	         else if (dateRange.equals("1")){
				
	        	 Calendar cal = Calendar.getInstance();
	             cal.add(Calendar.DATE, -7);
	        			 
				 DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			    
				 starttimestamp = sdf.format(cal.getTime()).toString();


			     Calendar cal1 = Calendar.getInstance();

				 endtimestamp = sdf.format(cal1.getTime()).toString();
				
				 dateRange = starttimestamp + "_" + endtimestamp;
				 dateRange = dateRange.replace("/", "-");
				
			
			
			
		   }


	         else if (dateRange.equals("6")){
					
	        	 Calendar cal = Calendar.getInstance();
	             cal.add(Calendar.DATE, -30);
	        			 
				 DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			    
				 starttimestamp = sdf.format(cal.getTime()).toString();


			     Calendar cal1 = Calendar.getInstance();

				 endtimestamp = sdf.format(cal1.getTime()).toString();
				
				 dateRange = starttimestamp + "_" + endtimestamp;
				 dateRange = dateRange.replace("/", "-");
					
				
				
				
			   }
			
	         else if (dateRange.equals("2")){
					
	        	 Calendar cal = Calendar.getInstance();
	          
				 DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			    
				 endtimestamp = sdf.format(cal.getTime()).toString();


				 DateFormat sdf1 =  new SimpleDateFormat("yyyy/MM/dd");
				
				 starttimestamp = sdf1.format(cal.getTime()).toString();
				 starttimestamp = starttimestamp  + " 00:00:01";
				 
				 dateRange = starttimestamp + "_" + endtimestamp;
				 dateRange = dateRange.replace("/", "-");
					
				
				
				
			   }
			
	         else if (dateRange.equals("11")){
					
	        	 Calendar cal = Calendar.getInstance();
	          
				 DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			    
				 endtimestamp = sdf.format(cal.getTime()).toString();


				 DateFormat sdf1 =  new SimpleDateFormat("yyyy/MM/dd");
				
				 starttimestamp = sdf1.format(cal.getTime()).toString();
				 starttimestamp = starttimestamp  + " 00:00:01";
				 
				 dateRange = starttimestamp + "_" + endtimestamp;
				 dateRange = dateRange.replace("/", "-");
					
				
				
				
			   }
						
			
			else{
				
			    String [] dateparts = dateRange.split("_");
			    starttimestamp = dateparts[0]+" 00:00:01";
			    endtimestamp = dateparts[1] +" 23:59:59";
				dateRange = starttimestamp + "_" + endtimestamp;		
				
				
			}
			// if((dateRange !=null && dateRange.isEmpty()==false)&& (siteId
			// !=null && siteId.isEmpty()==false) && ((city !=null &&
			// city.isEmpty()==false) || (gender !=null &&
			// gender.isEmpty()==false) || (agegroup !=null &&
			// agegroup.isEmpty()==false) || (audience_segment !=null &&
			// audience_segment.isEmpty()==false) || (income_level !=null &&
			// income_level.isEmpty()==false) || (device_type !=null &&
			// device_type.isEmpty()==false)) )
		
		
			
			
			if (city != null && !city.isEmpty()) {
				
				parts = city.split("~");
				for (int i = 0; i < parts.length; i++) {

					parts[i]=AggregationModule.citycodeMap2.get(parts[i]);
					citylist = citylist + parts[i]+"~";

				}
				
				citylist = citylist.substring(0, citylist.length()-1);
			//	city = AggregationModule.citycodeMap2.get(city);
				FilterMap.put("city", citylist);
			}

			
			if (state != null && !state.isEmpty()) {
				
				parts = state.split("~");
				for(int i=0; i<parts.length; i++){
				parts[i] = AggregationModule.StateMap.get(parts[i]);
				parts[i] = parts[i].replace(" ","_");
				statelist = statelist + parts[i]+"~";
				
				}
				
				statelist = statelist.substring(0,statelist.length()-1);
				FilterMap.put("state", statelist);
			}
			
			if (sourceUrl != null && !sourceUrl.isEmpty()) {
				
				parts = sourceUrl.split("~");
				for(int i=0; i<parts.length; i++){
				
				  parts[i] = AggregationModule.UrlMap.get(parts[i]);
				  sourceurlList = sourceurlList + parts[i]+"~";
				  
				  
				}
			
				sourceurlList = sourceurlList.substring(0,sourceurlList.length()-1);
				
				FilterMap.put("sourceUrl", sourceurlList);
			}
			
			
			
			if (country != null && !country.isEmpty()) {
				
				parts = country.split("~");
				
				for(int i=0; i< parts.length; i++){
				parts[i] = AggregationModule.CountryMap.get(parts[i]);
				parts[i] = parts[i].replace(" ","_");
			    countrylist = countrylist +parts[i]+"~";
				}	
				
				countrylist = countrylist.substring(0,countrylist.length()-1);
				
				FilterMap.put("country", countrylist);
			
			}
			
			if (gender != null && !gender.isEmpty()){
			
				parts = gender.split("~");
				for(int i=0;i< parts.length; i++){
				parts[i] = AggregationModule.GenderMap.get(parts[i]);
				genderlist = genderlist + parts[i]+"~";
				
				}
			
				genderlist = genderlist.substring(0,genderlist.length()-1);
				FilterMap.put("gender", genderlist);
			}
			
			if (agegroup != null && !agegroup.isEmpty()){
				parts = agegroup.split("~");
				for(int i=0;i< parts.length; i++){
				parts[i] = AggregationModule.AgeMap.get(parts[i]);
				agelist = agelist+parts[i]+"~";
				}				
				
				agelist = agelist.substring(0,agelist.length()-1);
				FilterMap.put("agegroup", agelist);
       
			}
				
			if (audience_segment != null && !audience_segment.isEmpty()){
				parts = audience_segment.split("~");
				for(int i=0; i < parts.length; i++){
				
				parts[i] = AggregationModule.audienceSegmentMap1.get(parts[i]);
				audiencesegmentlist = audiencesegmentlist+parts[i]+"~";
				
				//Should contain audience Code
				
				
				}
				
				audiencesegmentlist = audiencesegmentlist.substring(0,audiencesegmentlist.length()-1);
				FilterMap.put("audience_segment", audiencesegmentlist);

			}	
				
			
			if (subcategory != null && !subcategory.isEmpty()){
				parts = subcategory.split("~");
				for(int i=0; i < parts.length; i++){
			
				parts[i] = AggregationModule.audienceSegmentMap1.get(parts[i]);
				
				subcategorylist = subcategorylist+parts[i]+"~";
				
				
				//Should contain audience Code
				
				}
				
				subcategorylist = subcategorylist.substring(0,subcategorylist.length()-1);
				FilterMap.put("subcategory", subcategorylist);

			}	
			
			
			
			
			
			if(cookie_id != null && !cookie_id.isEmpty()){
			    parts = cookie_id.split("~");
				for(int i=0; i<parts.length; i++){
					
					cookielist = cookielist+parts[i]+"~";
				}
				cookielist = cookielist.substring(0,cookielist.length()-1);
				FilterMap.put("cookie_id", cookielist);

			}
			
			
			if (referrerType != null && !referrerType.isEmpty()){
				
				parts = referrerType.split("~");
				for(int i=0;i<parts.length;i++){
				//if(isInteger(referrerType)){
				parts[i] = AggregationModule.referrerTypeMap.get(parts[i]);
				referrerTypelist = referrerTypelist+parts[i]+"~";
				//}
				}
				
				referrerTypelist = referrerTypelist.substring(0,referrerTypelist.length()-1);
				FilterMap.put("referrerType", referrerTypelist);
			}
			
			
			if (income_level != null && !income_level.isEmpty()){
				parts = income_level.split("~");
				
				for(int i=0;i<parts.length;i++){
				parts[i] = AggregationModule.IncomeMap.get(parts[i]);
				incomelist = incomelist + parts[i]+ "~";
				}
				
				incomelist = incomelist.substring(0,incomelist.length()-1);
				FilterMap.put("incomelevel", incomelist);
			}
			
			if (device_type != null && !device_type.isEmpty()){
			    parts = device_type.split("~");
			    for(int i=0;i<parts.length;i++){
				parts[i] = AggregationModule.deviceMap.get(parts[i]);
				devicelist = devicelist + parts[i]+"~";
				
			    }
			    devicelist = devicelist.substring(0, devicelist.length()-1);
				FilterMap.put("device", devicelist);
			}
			
			if (tag != null && !tag.isEmpty())
				{
			
				parts =tag.split("~");
				
				for(int i=0;i<parts.length;i++){
				//	if(isInteger(tag)){
				parts[i] = AggregationModule.tagMap.get(parts[i]);		
			//	}
				 taglist = taglist+parts[i]+"~";
				
				 }
				
				taglist = taglist.substring(0,taglist.length()-1);
				
				FilterMap.put("tag", taglist);
				}

			if (authorName != null && !authorName.isEmpty()){
			
				parts = authorName.split("~");
				for(int i=0;i<parts.length;i++){
				parts[i] = AggregationModule.AuthorMap.get(parts[i]);
			    authorlist = authorlist+parts[i]+"~";
				
				}
				
				authorlist = authorlist.substring(0,authorlist.length()-1);
				FilterMap.put("authorName", authorlist);

			}

			/*   
			dateRange1= dateRange.split(" ");
			    
			    if(dateRange1[0].compareTo("2018-05-08")!=1){
			    	for(int i=0;i<dateRange1.length;i++){
			    	    if(i>0)
			    		dateRange = "2018-05-09_"+dateRange1[i];
				
			    	}
			    	}
			*/
			if (dateRange != null && dateRange.isEmpty() == false) {

				if (siteId != null && siteId.isEmpty() == false) {

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId == null || sectionId.isEmpty()) {
								if (articleid == null || articleid.isEmpty()) {

									String[] dateInterval = dateRange
											.split("_");

									try {
										report = module
												.getQueryFieldChannelLive(
														queryfield,
														dateInterval[0],
														dateInterval[1],
														siteId,filter);
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

									response.setData(report);
									if(report == null || report.isEmpty())
									{
										if(queryfield.equals("city"))
										response.setMessage("Location Not available");
										
										else if(queryfield.equals("audience_segment"))
										response.setMessage("Segment Not available");
									
										else 
										response.setMessage("Data Not available");
									
									
									}
									return new ResponseEntity<Response>(
											response, HttpStatus.OK);

								}

							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId != null && !sectionId.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSection(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
								     response.setMessage("Data Not available");
								}
								
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() > 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId != null && !sectionId.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSectionFilter(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId, FilterMap, filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
									response.setMessage("Data Not available");
								}
								
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticle(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid,filter,filtertype);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
										response.setMessage("Data Not available");
								}
								
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() > 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticleFilter(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid, FilterMap, filter, filtertype);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
										response.setMessage("Data Not available");
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);
							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId == null || sectionId.isEmpty()) {

								if (articleid == null || articleid.isEmpty()) {

									String[] dateInterval = dateRange
											.split("_");

									try {
										report = module
												.getQueryFieldChannelLive(
														queryfield,
														dateInterval[0],
														dateInterval[1],
														siteId,filter);
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

									response.setData(report);
									if(report == null || report.isEmpty())
									{
										if(queryfield.equals("city"))
										response.setMessage("Location Not available");
										
										else if(queryfield.equals("audience_segment"))
										response.setMessage("Segment Not available");
									
										else
										response.setMessage("Data Not available");
									}
									return new ResponseEntity<Response>(
											response, HttpStatus.OK);

								}

							}

						}
					}

					if (FilterMap.size() > 0) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelFilterLive(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													FilterMap,filter);
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
								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
									response.setMessage("Data Not available");
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelGroupByLive(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													groupby,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
									response.setMessage("Data Not available");
									
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId != null && !sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSectionGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId, groupby,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
									response.setMessage("Data Not available");
									
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticleGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid, groupby, filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
										response.setMessage("Data Not available");
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

				}

			}
		}

		else {

			// if((dateRange !=null && dateRange.isEmpty()==false)&& (siteId
			// !=null && siteId.isEmpty()==false) && ((city !=null &&
			// city.isEmpty()==false) || (gender !=null &&
			// gender.isEmpty()==false) || (agegroup !=null &&
			// agegroup.isEmpty()==false) || (audience_segment !=null &&
			// audience_segment.isEmpty()==false) || (income_level !=null &&
			// income_level.isEmpty()==false) || (device_type !=null &&
			// device_type.isEmpty()==false)) )
			String starttimestamp = "";
			String endtimestamp = "";
			
			
		     if (dateRange.equals("4")){
				
			    Date date = new Date();
			    Calendar c = Calendar.getInstance();
			    c.setTime(date);
			    int i = c.get(Calendar.DAY_OF_WEEK) - c.getFirstDayOfWeek();
			    c.add(Calendar.DATE, -i - 7);
			    DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
			    
			    starttimestamp = sdf.format(c.getTime()).toString();
			   
			    
			    c.add(Calendar.DATE, 6);
			    endtimestamp = sdf.format(c.getTime()).toString();
			    dateRange = starttimestamp + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");
			
			
			
			
		}
		
		 if (dateRange.equals("3")){
			
			    DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
				
				
				Calendar cal = Calendar.getInstance();
			    cal.add(Calendar.DATE, -1);
			   
			    endtimestamp = sdf.format(cal.getTime()).toString();
			    
			    starttimestamp = endtimestamp;
			    
			    dateRange = starttimestamp  + "_" + endtimestamp;
				dateRange = dateRange.replace("/", "-");
		
		
		
		
	}
	
		if (dateRange.equals("5")){
			
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MONTH, -1);
			cal.set(Calendar.DATE, 1);
			DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		    
			starttimestamp = sdf.format(cal.getTime()).toString();
			   

			cal.set(Calendar.DATE, cal.getActualMaximum(Calendar.DATE)); // changed calendar to cal

			endtimestamp = sdf.format(cal.getTime()).toString();
			dateRange = starttimestamp + "_" + endtimestamp;
			dateRange = dateRange.replace("/", "-");
			
		
		
		
	}
	
          if (dateRange.equals("1")){
			
        	 Calendar cal = Calendar.getInstance();
             cal.add(Calendar.DATE, -7);
        			 
			 DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		    
			 starttimestamp = sdf.format(cal.getTime()).toString();


		     Calendar cal1 = Calendar.getInstance();

			 endtimestamp = sdf.format(cal1.getTime()).toString();
			
			 dateRange = starttimestamp + "_" + endtimestamp;
			 dateRange = dateRange.replace("/", "-");
			
		
		
		
	   }


          if (dateRange.equals("6")){
				
        	 Calendar cal = Calendar.getInstance();
             cal.add(Calendar.DATE, -30);
        			 
			 DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		    
			 starttimestamp = sdf.format(cal.getTime()).toString();


		     Calendar cal1 = Calendar.getInstance();

			 endtimestamp = sdf.format(cal1.getTime()).toString();
			
			 dateRange = starttimestamp + "_" + endtimestamp;
			 dateRange = dateRange.replace("/", "-");
				
			
			
			
		   }
		
          if (dateRange.equals("2")){
				
        	 Calendar cal = Calendar.getInstance();
          
			 DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		    
			 endtimestamp = sdf.format(cal.getTime()).toString();


			 DateFormat sdf1 =  new SimpleDateFormat("yyyy/MM/dd");
			
			 starttimestamp = sdf1.format(cal.getTime()).toString();
			 starttimestamp = starttimestamp;
			 
			 dateRange = starttimestamp + "_" + endtimestamp;
			 dateRange = dateRange.replace("/", "-");
				
			
			
			
		   }
			
			
			
			
			
			
			
			
			
			
			
			
		    if(engagementType !=null && !engagementType.isEmpty())
		    {
		    	
		    	FilterMap.put("engagementType", engagementType);
		    }
			
			
			if (city != null && !city.isEmpty()) {
				
				parts = city.split("~");
				for (int i = 0; i < parts.length; i++) {

					parts[i]=AggregationModule.citycodeMap2.get(parts[i]);
					citylist = citylist + parts[i]+"~";

				}
				
				citylist = citylist.substring(0, citylist.length()-1);
			//	city = AggregationModule.citycodeMap2.get(city);
				FilterMap.put("city", citylist);
			}

			
			if (state != null && !state.isEmpty()) {
				
				parts = state.split("~");
				for(int i=0; i<parts.length; i++){
				parts[i] = AggregationModule.StateMap.get(parts[i]);
				parts[i] = parts[i].replace(" ","_");
				statelist = statelist + parts[i]+"~";
				
				}
				
				statelist = statelist.substring(0,statelist.length()-1);
				FilterMap.put("state", statelist);
			}
			
			if (sourceUrl != null && !sourceUrl.isEmpty()) {
				
				parts = sourceUrl.split("~");
				for(int i=0; i<parts.length; i++){
				
				  parts[i] = AggregationModule.UrlMap.get(parts[i]);
				  sourceurlList = sourceurlList + parts[i]+"~";
				  
				  
				}
			
				sourceurlList = sourceurlList.substring(0,sourceurlList.length()-1);
				
				FilterMap.put("sourceUrl", sourceurlList);
			}
			
			
			
			if (country != null && !country.isEmpty()) {
				
				parts = country.split("~");
				
				for(int i=0; i< parts.length; i++){
				parts[i] = AggregationModule.CountryMap.get(parts[i]);
				parts[i] = parts[i].replace(" ","_");
			    countrylist = countrylist +parts[i]+"~";
				}	
				
				countrylist = countrylist.substring(0,countrylist.length()-1);
				
				FilterMap.put("country", countrylist);
			
			}
			
			if (gender != null && !gender.isEmpty()){
			
				parts = gender.split("~");
				for(int i=0;i< parts.length; i++){
				parts[i] = AggregationModule.GenderMap.get(parts[i]);
				genderlist = genderlist + parts[i]+"~";
				
				}
			
				genderlist = genderlist.substring(0,genderlist.length()-1);
				FilterMap.put("gender", genderlist);
			}
			
			if (agegroup != null && !agegroup.isEmpty()){
				parts = agegroup.split("~");
				for(int i=0;i< parts.length; i++){
				parts[i] = AggregationModule.AgeMap.get(parts[i]);
				agelist = agelist+parts[i]+"~";
				}				
				
				agelist = agelist.substring(0,agelist.length()-1);
				FilterMap.put("agegroup", agelist);
       
			}
				
			if (audience_segment != null && !audience_segment.isEmpty()){
				parts = audience_segment.split("~");
				for(int i=0; i < parts.length; i++){
				
				parts[i] = AggregationModule.audienceSegmentMap1.get(parts[i]);
				audiencesegmentlist = audiencesegmentlist+parts[i]+"~";
				
				//Should contain audience Code
				
				
				}
				
				audiencesegmentlist = audiencesegmentlist.substring(0,audiencesegmentlist.length()-1);
				FilterMap.put("audience_segment", audiencesegmentlist);

			}	
				
			
			if (subcategory != null && !subcategory.isEmpty()){
				parts = subcategory.split("~");
				for(int i=0; i < parts.length; i++){
			
				parts[i] = AggregationModule.audienceSegmentMap1.get(parts[i]);
				
				subcategorylist = subcategorylist+parts[i]+"~";
				
				
				//Should contain audience Code
				
				}
				
				subcategorylist = subcategorylist.substring(0,subcategorylist.length()-1);
				FilterMap.put("subcategory", subcategorylist);

			}	
			
			
			
			
			
			if(cookie_id != null && !cookie_id.isEmpty()){
			    parts = cookie_id.split("~");
				for(int i=0; i<parts.length; i++){
					
					cookielist = cookielist+parts[i]+"~";
				}
				cookielist = cookielist.substring(0,cookielist.length()-1);
				FilterMap.put("cookie_id", cookielist);

			}
			
			
			if (referrerType != null && !referrerType.isEmpty()){
				
				parts = referrerType.split("~");
				for(int i=0;i<parts.length;i++){
				//if(isInteger(referrerType)){
				parts[i] = AggregationModule.referrerTypeMap.get(parts[i]);
				referrerTypelist = referrerTypelist+parts[i]+"~";
				//}
				}
				
				referrerTypelist = referrerTypelist.substring(0,referrerTypelist.length()-1);
				FilterMap.put("referrerType", referrerTypelist);
			}
			
			
			if (income_level != null && !income_level.isEmpty()){
				parts = income_level.split("~");
				
				for(int i=0;i<parts.length;i++){
				parts[i] = AggregationModule.IncomeMap.get(parts[i]);
				incomelist = incomelist + parts[i]+ "~";
				}
				
				incomelist = incomelist.substring(0,incomelist.length()-1);
				FilterMap.put("incomelevel", incomelist);
			}
			
			if (device_type != null && !device_type.isEmpty()){
			    parts = device_type.split("~");
			    for(int i=0;i<parts.length;i++){
				parts[i] = AggregationModule.deviceMap.get(parts[i]);
				devicelist = devicelist + parts[i]+"~";
				
			    }
			    devicelist = devicelist.substring(0, devicelist.length()-1);
				FilterMap.put("device", devicelist);
			}
			
			if (tag != null && !tag.isEmpty())
				{
			
				parts =tag.split("~");
				
				for(int i=0;i<parts.length;i++){
				//	if(isInteger(tag)){
				parts[i] = AggregationModule.tagMap.get(parts[i]);		
			//	}
				 taglist = taglist+parts[i]+"~";
				
				 }
				
				taglist = taglist.substring(0,taglist.length()-1);
				
				FilterMap.put("tag", taglist);
				}

			if (authorName != null && !authorName.isEmpty()){
			
				parts = authorName.split("~");
				for(int i=0;i<parts.length;i++){
				parts[i] = AggregationModule.AuthorMap.get(parts[i]);
			    authorlist = authorlist+parts[i]+"~";
				
				}
				
				authorlist = authorlist.substring(0,authorlist.length()-1);
				FilterMap.put("authorName", authorlist);

			}

            dateRange1= dateRange.split("_");
		    
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

            Date date1 = null;
            Date date2 = null;
            try {
				 date1 = format.parse(dateRange1[0]);
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
            try {
			    date2 = format.parse("2018-05-08");
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

            
		    if(date1.compareTo(date2)!=1)
		    	dateRange = "2018-05-09_"+dateRange1[1];
			
			
			if (dateRange != null && dateRange.isEmpty() == false) {

				if (siteId != null && siteId.isEmpty() == false) {

					
					
                        if (FilterMap.size() > 0) {
						
						if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelFilterGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													groupby,filter,FilterMap);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						  }
						}
}	
					
					

        			

        					
        					
                                if (FilterMap.size() > 0) {
        						
        						if (group_by != null && !group_by.isEmpty()) {

        						if (sectionId != null && !sectionId.isEmpty()) {

        							if (articleid == null || articleid.isEmpty()) {

        								String[] dateInterval = dateRange.split("_");

        								try {
        									report = module
        											.getQueryFieldChannelSectionFilterGroupBy(
        													queryfield,
        													dateInterval[0],
        													dateInterval[1],
        													siteId,sectionId,
        													groupby,filter,FilterMap);
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

        								response.setData(report);
        								if(report == null || report.isEmpty())
        								{
        									if(queryfield.equals("city"))
        									response.setMessage("Location Not available");
        									
        									if(queryfield.equals("audience_segment"))
        									response.setMessage("Segment Not available");
        								
        								
        								
        								}
        								return new ResponseEntity<Response>(response,
        										HttpStatus.OK);

        							}

        						  }
        						}	
                                }
					
					
                                
                                if (FilterMap.size() > 0) {
            						
            						if (group_by != null && !group_by.isEmpty()) {

            						if (sectionId == null || sectionId.isEmpty()) {

            							if (articleid != null && !articleid.isEmpty()) {

            								String[] dateInterval = dateRange.split("_");

            								try {
            									report = module
            											.getQueryFieldChannelArticleFilterGroupBy(
            													queryfield,
            													dateInterval[0],
            													dateInterval[1],
            													siteId,articleid,
            													groupby,filter,FilterMap);
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

            								response.setData(report);
            								if(report == null || report.isEmpty())
            								{
            									if(queryfield.equals("city"))
            									response.setMessage("Location Not available");
            									
            									if(queryfield.equals("audience_segment"))
            									response.setMessage("Segment Not available");
            								
            								
            								
            								}
            								return new ResponseEntity<Response>(response,
            										HttpStatus.OK);

            							}

            						  }
            						}	
                                    }
    					
                                
                                
					
					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId == null || sectionId.isEmpty()) {
								if (articleid == null || articleid.isEmpty()) {

									String[] dateInterval = dateRange
											.split("_");

									try {
										report = module.getQueryFieldChannel(
												queryfield, dateInterval[0],
												dateInterval[1],
												siteId,filter,limit);
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

									response.setData(report);
									if(report == null || report.isEmpty())
									{
										if(queryfield.equals("city"))
										response.setMessage("Location Not available");
										
										else if(queryfield.equals("audience_segment"))
										response.setMessage("Segment Not available");
									  
										else 
											response.setMessage("Data Not available");
									
									}
									return new ResponseEntity<Response>(
											response, HttpStatus.OK);

								}

							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId != null && !sectionId.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSection(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
										response.setMessage("Data Not available");
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() > 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId != null && !sectionId.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSectionFilter(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId, FilterMap, filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else 
										response.setMessage("Data Not available");
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticle(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid,filter,filtertype);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else 
										response.setMessage("Data Not available");
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (FilterMap.size() > 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticleFilter(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid, FilterMap, filter, filtertype);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								   
									else 
										response.setMessage("Data Not available");
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);
							}

						}

					}

					if (FilterMap.size() == 0) {

						if (group_by == null || group_by.isEmpty()) {

							if (sectionId == null || sectionId.isEmpty()) {

								if (articleid == null || articleid.isEmpty()) {

									String[] dateInterval = dateRange
											.split("_");

									try {
										report = module.getQueryFieldChannel(
												queryfield, dateInterval[0],
												dateInterval[1],
												siteId,filter,limit);
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

									response.setData(report);
									if(report == null || report.isEmpty())
									{
										if(queryfield.equals("city"))
										response.setMessage("Location Not available");
										
										else if(queryfield.equals("audience_segment"))
										response.setMessage("Segment Not available");
									
										else 
											response.setMessage("Data Not available");
									
									}
									return new ResponseEntity<Response>(
											response, HttpStatus.OK);

								}

							}

						}
					}

					if (FilterMap.size() > 0) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module.getQueryFieldChannelFilter(
											queryfield, dateInterval[0],
											dateInterval[1],
											siteId,
											FilterMap,filter,limit);
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
								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else 
										response.setMessage("Data Not available");
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													groupby,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
										response.setMessage("Data Not available");
								 
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId != null && !sectionId.isEmpty()) {

							if (articleid == null || articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelSectionGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													sectionId, groupby,filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else 
										response.setMessage("Data Not available");
								
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

					if (group_by != null && !group_by.isEmpty()) {

						if (sectionId == null || sectionId.isEmpty()) {

							if (articleid != null && !articleid.isEmpty()) {

								String[] dateInterval = dateRange.split("_");

								try {
									report = module
											.getQueryFieldChannelArticleGroupBy(
													queryfield,
													dateInterval[0],
													dateInterval[1],
													siteId,
													articleid, groupby, filter);
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

								response.setData(report);
								if(report == null || report.isEmpty())
								{
									if(queryfield.equals("city"))
									response.setMessage("Location Not available");
									
									else if(queryfield.equals("audience_segment"))
									response.setMessage("Segment Not available");
								
									else
										response.setMessage("Data Not available");
								
								}
								return new ResponseEntity<Response>(response,
										HttpStatus.OK);

							}

						}

					}

				}

			}

		}

		if (report == null) {
			response1.setData(report);
			return new ResponseEntity<Response>(response1, HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity<Response>(response, HttpStatus.OK);
	}

	
	
	
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/TemplateData", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<DashboardTemplate> getTemplatedata(
			@RequestParam("siteid") String siteid) {
	//	System.out.println("ArticleUrl " + articleurl);
		
        siteid="2";
		DashboardTemplate obj = reportService.getTemplatedata(siteid);
		if(obj==null)
			return new ResponseEntity<DashboardTemplate>(obj, HttpStatus.NOT_FOUND);	
			
		return new ResponseEntity<DashboardTemplate>(obj, HttpStatus.OK);
	    
	
	}
	
	
	

	@CrossOrigin
	@RequestMapping(value = "/report/v1/AuthorData", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String>  getAuthordata() {
	
      AggregationModule module = AggregationModule.getInstance();
		
		
		try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	
     String output = module.getAuthorMap();
     
	
	return new ResponseEntity<String>(output, HttpStatus.OK);
	    
	    
	
	}
	
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/TagData", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getTagdata() {
		
		
       AggregationModule module = AggregationModule.getInstance();
		
		
		try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		
		String output = module.getTagmap();
	     
		
		return new ResponseEntity<String>(output, HttpStatus.OK);
	    
	
	}
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/getLatestData", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public String getLatestDatadata() {
		
		
		CacheManager manager = CacheManager.getInstance();
		Cache cache = manager.getCache("cache2");
		cache.removeAll();
	    
	    return "Data Reloaded! Please note there will be 5 minute latencies in Reports are compared to real time";
	}
	
	
	
	public static boolean isInteger(String s) {
	    try { 
	        Integer.parseInt(s); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    } catch(NullPointerException e) {
	        return false;
	    }
	    // only got here if we didn't return false
	    return true;
	}
	
	
	
	
	
	
	
	
	
	
	

	@CrossOrigin
	@RequestMapping(value = "/report/v1/SectionList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<SectionResponse> getSectionList(
			@RequestParam("siteid") String siteid) {
		System.out.println("SiteID Section List " + siteid);
		SectionResponse response = new SectionResponse();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

		SectionResponse response1 = new SectionResponse();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");

		List<Section> sectionList = reportService.getSectionList(siteid);
		if (sectionList == null) {
			System.out.println("Report with siteid " + siteid + " not found");
			response1.setData(sectionList);
			return new ResponseEntity<SectionResponse>(response1,
					HttpStatus.NOT_FOUND);
		}
		response.setData(sectionList);
		return new ResponseEntity<SectionResponse>(response, HttpStatus.OK);
	}

	@CrossOrigin
	@RequestMapping(value = "/report/v1/ArticleList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<ArticleResponse> getArticleList(
			@RequestParam("siteid") String siteid) {
		System.out.println("SiteID Section List " + siteid);
		ArticleResponse response = new ArticleResponse();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

		ArticleResponse response1 = new ArticleResponse();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");

		List<Article> articleList = reportService.getArticleList(siteid);
		if (articleList == null) {
			System.out.println("Report with siteid " + siteid + " not found");
			response1.setData(articleList);
			return new ResponseEntity<ArticleResponse>(response1,
					HttpStatus.NOT_FOUND);
		}
		response.setData(articleList);
		return new ResponseEntity<ArticleResponse>(response, HttpStatus.OK);
	}

	@CrossOrigin
	@RequestMapping(value = "/report/v1/ArticleMetadata", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<ArticleResponse> getArticleMetadata(
			@RequestParam("ArticleUrl") String articleurl) {
		System.out.println("ArticleUrl " + articleurl);
		ArticleResponse response = new ArticleResponse();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

		ArticleResponse response1 = new ArticleResponse();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");
        List<Article> articlelist = new ArrayList<Article>();
		Article article = reportService.getArticleMetadata(articleurl);
		if (article == null) {
			//System.out.println("Report with siteid " + siteid + " not found");
			response1.setData(articlelist);
			return new ResponseEntity<ArticleResponse>(response1,
					HttpStatus.NOT_FOUND);
		}
		articlelist.add(article);
		response.setData(articlelist);
		return new ResponseEntity<ArticleResponse>(response, HttpStatus.OK);
	}
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/SiteList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<SiteResponse> getSiteList(
			@RequestParam("userid") String userid, HttpServletRequest request) {
		System.out.println("SiteID List " + userid);
		SiteResponse response = new SiteResponse();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

		SiteResponse response1 = new SiteResponse();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");
		List<Site> siteList = new ArrayList<Site>();
		 String token;	
	     String userdetails;
	     String [] userinfo;
	     String emailId = null;
		 User user = new User();
	     Cookie[] cookies = request.getCookies();
 	            if(cookies != null){
	     for(int i = 0; i < cookies.length ; i++){
 	            if(cookies[i].getName().equals("AUTHTOKEN")){
             //Fetch User details and return in json format
	              token = cookies[i].getValue();
	              userdetails=EncryptionModule.decrypt(null, null,URLDecoder.decode(token));
 	              userinfo = userdetails.split(":");
                  emailId = userinfo[0];
 	            }
 	        }
 	      }
 	            
 	    if(emailId != null)
 	    user=UserDetailsDAO.GetUserDetails(emailId);
		
		if(emailId == null){
			userid="1";
		}
		else{
		userid = user.getUserId();
		}
		
		//if(userid.equals("3")  || || ){
		
		
		
		
		String siteid = "2";
	    String siteurl = "www.wittyfeed.com";
	    String sitename = "wittyfeed";
	
	    String siteid1 = "3";
	    String siteurl1 = "www.wittyfeed.com";
	    String sitename1 = "duniadigest";
	    
	    String siteid2 = "4";
	    String siteurl2 = "www.wittyfeed.com";
	    String sitename2 = "geeksmate";
	   
	    String siteid3 = "5";
	    String siteurl3 = "www.wittyfeed.com";
	    String sitename3 = "foodmate";
	    
	    String siteid4 = "6";
	    String siteurl4 = "www.wittyfeed.com";
	    String sitename4 = "innervoice";
	    
	    String siteid5 = "1";
	    String siteurl5 = "www.wittyfeed.tv";
	    String sitename5 = "wittyfeed_india";
	    
	    Site obj = new Site();
        

         obj.setSiteId(siteid);
		 obj.setSiteName(sitename);
	     obj.setSiteurl(siteurl);
		
		
	     
		    Site obj1 = new Site();
	        

	         obj1.setSiteId(siteid1);
			 obj1.setSiteName(sitename1);
		     obj1.setSiteurl(siteurl1);
	     
		     Site obj2 = new Site();
		        

	         obj2.setSiteId(siteid2);
			 obj2.setSiteName(sitename2);
		     obj2.setSiteurl(siteurl2);
	     
		     Site obj3 = new Site();
		        

	         obj3.setSiteId(siteid3);
			 obj3.setSiteName(sitename3);
		     obj3.setSiteurl(siteurl3);
		
		
		     Site obj4 = new Site();
		        

	         obj4.setSiteId(siteid4);
			 obj4.setSiteName(sitename4);
		     obj4.setSiteurl(siteurl4);
		     
		     Site obj5 = new Site();
		        

	         obj5.setSiteId(siteid5);
			 obj5.setSiteName(sitename5);
		     obj5.setSiteurl(siteurl5);
		     
		     
		     
		siteList.add(obj);
		siteList.add(obj1);
		siteList.add(obj2);
		siteList.add(obj3);
		siteList.add(obj4);
		siteList.add(obj5);
	//	}
		
		if (siteList == null) {
			System.out.println("Report with userid " + userid + " not found");
			response1.setData(siteList);
			return new ResponseEntity<SiteResponse>(response1,
					HttpStatus.NOT_FOUND);
		}
		response.setData(siteList);
		return new ResponseEntity<SiteResponse>(response, HttpStatus.OK);
	}


	@CrossOrigin
	@RequestMapping(value = "/report/v1/countryList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<LocationResponse> getCountryList() {
		System.out.println("Country List ");
		LocationResponse response = new LocationResponse();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

	    LocationResponse response1 = new LocationResponse();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");

		response1.setMessage("Not Found");

		AggregationModule module = AggregationModule.getInstance();
		try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		List<String> countryList = new ArrayList<String>();
		countryList = module.getcountryNames();
		if (countryList == null) {
		
			response1.setData(countryList);
			return new ResponseEntity<LocationResponse>(response1,
					HttpStatus.NOT_FOUND);
		}
		response.setData(countryList);
		return new ResponseEntity<LocationResponse>(response, HttpStatus.OK);
	}
	
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/stateList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<LocationResponse> getStateList(@RequestParam("countryCode") String countryCode) {
		System.out.println("State List ");
		LocationResponse response = new LocationResponse();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

	    LocationResponse response1 = new LocationResponse();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");

		AggregationModule module = AggregationModule.getInstance();
		try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		List<String> stateList = new ArrayList<String>();
		stateList = module.getcountryStateNames(countryCode);
		if (stateList == null) {
			
			response1.setData(stateList);
			return new ResponseEntity<LocationResponse>(response1,
					HttpStatus.NOT_FOUND);
		}
		response.setData(stateList);
		return new ResponseEntity<LocationResponse>(response, HttpStatus.OK);
	}
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/cityList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<LocationResponse> getStateList(@RequestParam("countryCode") String countryCode,@RequestParam("stateCode") String stateCode) {
		System.out.println("City List ");
		LocationResponse response = new LocationResponse();
		response.setCode("200");
		response.setStatus("Success");
		response.setMessage("API Successful");

	    LocationResponse response1 = new LocationResponse();
		response1.setCode("404");
		response1.setStatus("Error");
		response1.setMessage("Not Found");

		AggregationModule module = AggregationModule.getInstance();
		try {
			module.setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		List<String> cityList = new ArrayList<String>();
	    cityList = module.getcountryCityNames(countryCode, stateCode);
        if (cityList == null) {
			
			response1.setData(cityList);
			return new ResponseEntity<LocationResponse>(response1,
					HttpStatus.NOT_FOUND);
		}
		response.setData(cityList);
		return new ResponseEntity<LocationResponse>(response, HttpStatus.OK);
	}
	
	
	
	
	@CrossOrigin
	@RequestMapping(value = "/report/{id}/SectionAPIs/{dateRange}/{channel}/{sectionname}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReportSection(
			@PathVariable("id") long id,
			@PathVariable("dateRange") String dateRange,
			@PathVariable("channel") String channel_name,
			@PathVariable("sectionname") String sectionname) {
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report = reportService
				.extractReportsChannelSection(id, dateRange, channel_name,
						sectionname);
		if (report == null) {
			System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(
					HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	}

	@CrossOrigin
	@RequestMapping(value = "/report/{id}/LiveAPIs/{channel}/{timeduration}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReportLiveData(
			@PathVariable("id") long id,
			@PathVariable("channel") String channel_name,
			@PathVariable("timeduration") String timeduration) {
		System.out.println("Fetching Report with id " + id);

		String starttimestamp = "";
		String endtimestamp = "";
		String dateRange = "";
		if (timeduration.equals("Last_24_Hours")) {

			DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
			endtimestamp = sdf.format(cal.getTime()).toString();
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.HOUR_OF_DAY, -24);
			starttimestamp = sdf.format(calendar.getTime()).toString();

			dateRange = starttimestamp + "," + endtimestamp;
			dateRange = dateRange.replace("/", "-");
		}

		if (timeduration.equals("Last_15_minutes")) {
			DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
			endtimestamp = sdf.format(cal.getTime()).toString();

			cal.add(Calendar.MINUTE, -15);
			starttimestamp = sdf.format(cal.getTime()).toString();
			dateRange = starttimestamp + "," + endtimestamp;
			dateRange = dateRange.replace("/", "-");

		}

		List<PublisherReport> report = reportService.extractReportsChannelLive(
				id, dateRange, channel_name);
		if (report == null) {
			System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(
					HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	}

}
