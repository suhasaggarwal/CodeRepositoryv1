package com.spidio.UserSegmenter;



import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;




















import com.spidio.dataModel.DeviceObject;
import com.spidio.dataModel.LocationObject;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

	public class WorkerThread implements Runnable {
		
		SearchHit hit;
		Client client;
		BulkProcessor bulkProcessor;
		String keywords = null;
		String description = null;
		String[] searchkeyword = null;
		String[] finalsearchkeyword = null;
		SearchHit[] matchingsegmentrecords = null;
		String category = null;
		String subcategory = null;
		DeviceObject deviceProperties = null;
		LocationObject locationProperties = null;
		
		// SearchHit[] results =
		// IndexCategoriesData.searchEntireUserData(client,
		// "dmpuserdatabase","core2");

		// System.setOut(new PrintStream(new BufferedOutputStream(new
		// FileOutputStream("log.txt"))));

		// Scans quarterly data of the day, enhances its data points and store
		// it in new index
		String refurl;
		String refcurrent;
		String clickedurl;
		Set<String> lines;
		//String reforiginal = (String) result.get("reforiginal");
	   // String refcuroriginal = (String) result.get("refcurrentoriginal");
	   // String clickedurloriginal = (String) result.get("clickurloriginal");
		String localstorageId;
		String page_title;
		String documentId;
		String ip;
		String browser;
		String userAgent;
		String channel_name;
		String subcategorydata;
		String localstorageid;
		String fingerprint_id;
		String cookie_id;	
		String session_id;
		String master_id;
		String plugin_data;
		String audience_segment;
		String request_time;
		String system_os;
		String date;
	
		
		
		
		String brand_name = null;
		String browserversion = null;
		String model_name = null;
		String device_os = null;
		String browser1 = null;
		String picture_gif = null;				
		String picture_jpg = null;
		String picture_png = null;
		String gif_animated = null;
		String streaming_video = null;
	    String streaming_3gpp = null;
		String streaming_mp4 = null;
		String streaming_mov = null;
		String colors = null;
		String dual_orientation = null;
		String ux_full_desktop = null;
		String city = null;
		String state = null;
		String org = null;
		String isp = null;
		String gender[] = { "male", "female" };
		String agegroup[] = { "13_17", "18_24", "25_34", "35_44",
				"45_54", "55_64", "65_more" };
		String agegroup1[] = {"25_34","35_44","45_54"};
		String gender1 = "male";
		String incomelevel[] = {"high","medium","medium","medium","low"};
		String incomelevel1[] = {"high","high","medium","medium","medium","low"};
		String subcategory_entertainment[] = {
				"_entertainment_bollywood", "_entertainment_music",
				"_entertainment_tv_shows", "_entertainment_games" };
		String subcategory_lifestyle[] = { "_lifestyle_food",
				"_lifestyle_health_fitness", "_lifestyle_fashion" };
		String subcategory_news[] = { "_news_india_news",
				"_news_world_news" };
		String subcategory_education[] = { "_education_mba",
				"_education_mca", "_education_btech",
				"_education_online_courses" };
		String subcategory_technology[] = { "_technology_mobile",
				"_technology_computing", "_technology_apps",
				"_technology_gadgets" };
		String subcategory_business[] = { "_business_india_business",
				"_business_international_business",
				"_business_markets", "_business_mf_simplified",
				"_business_startups" };
		String QuarterValue = null;
		
		
		Map<String, Object> result0;
		Map<String, Object> result;
		Map<String, Object> result1;
		Map<String, Object> result2;
		
		Integer delta=0;
		
	    public WorkerThread(SearchHit hit, Client client,Set<String> lines1){
	        this.hit = hit;
	        this.client = client;
	       // this.bulkProcessor = BulkProcessorModule.GetInstance(client);
			this.lines = lines1; 
	    }

	    //Data points enhancement per record
	    @Override
	    public void run() {
	    	//Handle the hit...
	    	
	    	String srcIP = "";
	    	String ES_ID = this.hit.getId();
	    	String ES_INDEX = "enhanceduserdatabeta1";
	    	try {

	    		 
	    		


					System.out.println("------------------------------");
					result = hit.getSource();
					refurl = (String) result.get("referrer");
					refcurrent = (String) result.get("refcurrent");
					clickedurl =  (String) result.get("clickedurl");
					System.out.println(refurl);
					//String reforiginal = (String) result.get("reforiginal");
				   // String refcuroriginal = (String) result.get("refcurrentoriginal");
				   // String clickedurloriginal = (String) result.get("clickurloriginal");
					localstorageId = (String)result.get("localStorageid");
					page_title = (String) result.get("page_title");
					documentId = (String) result.get("_id");
					ip = (String) result.get("ip");
					browser = (String) result.get("browser_name");
					userAgent = (String) result.get("br_user_agent");
					channel_name = (String) result.get("channel_name");
					subcategorydata = (String) result.get("subcategory");
					localstorageid = (String) result.get("localStorageid");
					fingerprint_id =(String) result.get("fingerprint_id");
					cookie_id = (String) result.get("cookie_id");	
					session_id = (String)result.get("session_id");
					master_id = (String) result.get("mastercookie_id");
					plugin_data = (String) result.get("plugin");
				    audience_segment = (String) result.get("audience_segment");
				    request_time = (String) result.get("request_time");
					system_os = (String) result.get("system_os");
					
					 system_os = system_os.replace(" ", "_").replace("-", "_");
					
					 date = request_time.substring(0, 10);
					 System.out.println(userAgent);
					 brand_name = null;
					
					
			        QuarterValue = null;
					int hour = Integer.parseInt(request_time.substring(11, 13)
							.trim());
					System.out.println(request_time);
					System.out.println(hour);
					// This Quartely data of the day is aggregated later on and used
					// for time of the day reports

					if (page_title != null) {
                    //All below special characters are replaced with underscore 
					//This helps prevent string split at the time of indexing, this will lead to data point corruption	
				    //This should be taken care of if new data point is to be added in enhanced section and mapping of string is set to analyzed
						page_title = page_title.replace("-", "_").replace(".", "_")
								.replace(":", "_").replace(" ", "_")
								.replace("/", "_").replace("//", "_");
						System.out.println(page_title);
					}

					if (refurl != null) {

						result.put("reforiginal",refurl);
						refurl = refurl.replace("-", "_").replace(".", "_")
								.replace(":", "_").replace(" ", "_")
								.replace("/", "_").replace("//", "_");
						result.put("referrer", refurl);
						
						System.out.println(refurl);
					}

					
					if (refcurrent != null) {

						
						result.put("refcurrentoriginal",refcurrent);
						refcurrent = refcurrent.replace("-", "_").replace(".", "_")
								.replace(":", "_").replace(" ", "_")
								.replace("/", "_").replace("//", "_");
						result.put("refcurrent", refcurrent);
						
						System.out.println(refcurrent);
					}
					
					
					
					if (clickedurl != null) {
                    //Click Url enhancement 
						result.put("clickurloriginal",clickedurl);
						clickedurl = clickedurl.replace("-", "_").replace(".", "_")
								.replace(":", "_").replace(" ", "_")
								.replace("/", "_").replace("//", "_");
						result.put("clickedurl", clickedurl);
						
						System.out.println(clickedurl);
					}
					
					
					 Random random = new Random();
				      
				     Integer idx2 = random.nextInt(7) + 1;
					
				     result.put("engagementTime", idx2);
				     
				     
				    
				  //Determine Quarter of the day of ES record, Used for generating specific reports   
				     
					if (hour < 4)
						QuarterValue = "Quarter1";

					if (hour >= 4 && hour < 8)
						QuarterValue = "Quarter2";

					if (hour >= 8 && hour < 12)
						QuarterValue = "Quarter3";

					if (hour >= 12 && hour < 16)
						QuarterValue = "Quarter4";

					if (hour >= 16 && hour < 20)
						QuarterValue = "Quarter5";

					if (hour >= 20 && hour < 24)
						QuarterValue = "Quarter6";

					System.out.println(QuarterValue);

					int idx;
					result.put("date", date);
			   //Derives device properties using userAgent passed to Wurfl		
					deviceProperties = ProcessDeviceData
							.getDeviceDetails(userAgent);

					if (deviceProperties != null) {
						brand_name = deviceProperties.getBrandName();
						if (brand_name != null)
							brand_name = deviceProperties.getBrandName()
									.replace(" ", "_").replace("-", "_");

						model_name = deviceProperties.getModel_name();
						
						browserversion = deviceProperties.getBrowserversion();
						
						
						if(browserversion != null && !browserversion.isEmpty())
						result.put("browserversion", browserversion);
						
						String release_date = deviceProperties.getRelease_date();

						if (model_name != null)
							model_name = deviceProperties.getModel_name().replace("-", "_").replace(":", "_").replace(" ", "_")
						  			.replace("/", "_").replace("//", "_").replace("+", "_").replace(")","_").replace("(", "_");

						if (release_date != null)
							release_date = deviceProperties.getRelease_date()
									.replace(" ", "_").replace("-", "_");

						model_name = brand_name + "_" + model_name + "_"
								+ release_date;

						
						browser1 = deviceProperties.getBrowser();
					
						browser1 =  browser1.replace("-", "_").replace(":", "_").replace(" ", "_")
					  			.replace("/", "_").replace("//", "_").replace(")","_").replace("(", "_").replace("+", "_");
						
						browser =  browser.replace("-", "_").replace(":", "_").replace(" ", "_")
					  			.replace("/", "_").replace("//", "_").replace(")","_").replace("(", "_").replace("+", "_");
						
						if(browser.trim().toLowerCase().contains("handheld")==true)
						    result.put("browser_name", browser1);
						else	
						    result.put("browser_name", browser);
						
						
						device_os = deviceProperties.getDeviceOs();
						String device_os_version = deviceProperties
								.getDevice_os_version();

						if (device_os != null)
							device_os = deviceProperties.getDeviceOs()
									.replace(" ", "_").replace("-", "_");

						if (device_os_version != null)
							device_os_version = deviceProperties
									.getDevice_os_version().replace(" ", "_")
									.replace("-", "_");

						device_os = device_os + "_" + device_os_version;
					}

					if (channel_name != null)
						channel_name = channel_name.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");

					if (fingerprint_id != null)
						fingerprint_id = fingerprint_id.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");

					if (cookie_id != null)
						cookie_id = cookie_id.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");

				
					
					if (session_id != null)
						session_id = session_id.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");
					
					
					
					
					
					if (localstorageid != null)
						localstorageid = localstorageid.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");

					if (plugin_data != null)
						plugin_data = plugin_data.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");

					
					if(master_id != null)
						master_id = master_id.replace("-", "_")
						.replace(".", "_").replace(" ", "_")
						.replace("#", "_");
					
					if (browser != null)
						browser = browser.replace(" ", "_").replace("-", "_");

					// String userAgent =
					// "Mozilla/5.0 (Linux; Android 4.4.2; SGH-I257M Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.89 Mobile Safari/537.36";
					if (audience_segment != null) {
						audience_segment = audience_segment.replace("-", "_")
								.replace(" ", "_").replace("#", "").toLowerCase()
								.replace("technologynology", "technology")
								.replace("food&beverages", "food_beverages")
								.replace("life_style", "lifestyle")
								.replace("india_news", "news")
								.replace("accessories", "clothes")
								.replace("indianexpress.com", "news")
								.replace("wolifestyle", "women_lifestyle");

					}

					// deviceProperties =
					// ProcessDeviceData.getDeviceDetails(userAgent);
					// String channelName = (String)result.get("channel_name");

					// deviceProperties =
					// ProcessDeviceData.getDeviceDetails(userAgent);

					if (channel_name != null) {
						/*
						 * if (channel_name.contains("Mumbai")) { result.put("city",
						 * "Mumbai"); result.put("country", "India");
						 * result.put("latitude_longitude", "Mumbai_19_72.8");
						 * result.put("postalcode", "Mumbai_400099");
						 * 
						 * } else {
						 */
						if (ip != null) {
							//Derives location properties using IPAddress from Maxmind
							locationProperties = ProcessIPAddress.getIPDetails(ip);
							if (locationProperties != null) {
								city = locationProperties.getCity();
								if (city != null) {
									city = locationProperties.getCity()
											.replace(" ", "_").replace("-", "_");
									result.put("city", city);
									result.put("country",
											locationProperties.getCountry().replace(" ", "_").replace("-", "_"));
									if (locationProperties.getLatittude() != null)
										result.put("latitude_longitude", city
												+ "_"
												+ locationProperties.getLatittude()
														.toString()
												+ "_"
												+ locationProperties.getLongitude()
														.toString());
									if (locationProperties.getPostalCode() != null)
										result.put(
												"postalcode",
												city
														+ "_"
														+ locationProperties
																.getPostalCode());
								
								
								  if(locationProperties.getState()!=null)
								  {
									  state = locationProperties.getState().replace(" ", "_").replace("-", "_");
									  result.put("state",state);
								  }
								}
							}
						}
						// }
					}

					if (ip != null) {
                        //Derives Organisation data from IP Address - Maxmind
						org = ProcessOrganisationData.getOrgDetails(ip);
						if (org != null)
							org = org.replace("-", "_").replace(":", "_").replace(" ", "_")
						  			.replace("/", "_").replace("//", "_").replace(")","_").replace("(", "_").replace("+", "_").replace("&", "_");
                        //Derives ISP data from IP Address - Maxmind
						isp = ProcessISPData.getISPDetails(ip);
						if (isp != null)
							isp = isp.replace("-", "_").replace(":", "_").replace(" ", "_")
			    			.replace("/", "_").replace("//", "_").replace(")","_").replace("(", "_").replace("+", "_").replace("&", "_");
					      
					}

					result.put("brandName", brand_name);
					result.put("modelName", model_name);
					if (device_os.contains("Desktop") == false)
						result.put("system_os", device_os);
					else
						result.put("system_os", system_os);
					
					result.put("channel_name", channel_name);
					
					
					if (cookie_id ==null || cookie_id.isEmpty())
			          { 
			    		
			    		if (localstorageId !=null && !localstorageId.isEmpty()){
			    		cookie_id = localstorageId;  
			        	cookie_id = cookie_id.replace("-", "_");
			    		}
			          }
					
					result.put("cookie_id", cookie_id);
					
					 final BytesRef seedAsString = new BytesRef(cookie_id);
				     long nodeIdSeed = MurmurHash3.hash128(seedAsString.bytes, seedAsString.offset, seedAsString.length, 0, new MurmurHash3.Hash128()).h1;
				   //Uses Murmer Hash for cookie hash (Use it for Distinct Count - Speeds up HyperLogLog++ Algorithm used in ES)
				     result.put("cookiehash", nodeIdSeed);
					
					
					
					result.put("session_id", session_id);
					
					
					 final BytesRef seedAsString1 = new BytesRef(session_id);
				     long nodeIdSeed1 = MurmurHash3.hash128(seedAsString1.bytes, seedAsString1.offset, seedAsString1.length, 0, new MurmurHash3.Hash128()).h1;
				   //Uses Murmer Hash for session hash (Use it for Distinct Count - Speeds up HyperLogLog++ Algorithm used in ES)
				     result.put("sessionhash", nodeIdSeed1);
					
					
					
					
					result.put("fingerprint_id", fingerprint_id);
					result.put("plugin",plugin_data);
					result.put("mastercookie_id", master_id);
					result.put("audience_segment", audience_segment);
					result.put("referrer", refurl);
					result.put("page_title", page_title);
					result.put("organisation", org);
					result.put("ISP", isp);
					result.put("QuarterValue", QuarterValue);
					idx = new Random().nextInt(gender.length);

					result.put("gender", gender[idx]);
					idx = new Random().nextInt(agegroup.length);
					int idx1 = new Random().nextInt(agegroup1.length);
				
					idx = new Random().nextInt(incomelevel.length);
					result.put("incomelevel",incomelevel[idx]);
					
	                int idx1a = new Random().nextInt(incomelevel1.length);
					
					if(channel_name.trim().toLowerCase().equals("cuberoot")){
					result.put("agegroup",agegroup1[idx1] );
	                result.put("gender", gender1);
	                result.put("incomelevel",incomelevel1[idx1a]);
					}
					else
					result.put("agegroup", agegroup[idx]);

	                
	                if(brand_name.equals("OnePlus")){
	                	result.put("agegroup", "35_44");
	                    result.put("gender","male");
	                } 
	                
	                if(brand_name.equals("Samsung")){
	                	result.put("agegroup", "35_44");
	                    result.put("gender","male");
	                }
	                
	                
	                
					if (audience_segment != null) {
						if (audience_segment.trim().toLowerCase().equals("entertainment")) {
							
							result.put("subcategory",
									"_entertainment_"+subcategorydata);
						}

						if (audience_segment.trim().toLowerCase().equals("news")) {
							
							result.put("subcategory","_news_"+subcategorydata);
						}

						if (audience_segment.trim().toLowerCase().equals("lifestyle")) {
							
							result.put("subcategory","_lifestyle_"+subcategorydata);
						}
						
	                    if (audience_segment.trim().toLowerCase().equals("sports")) {
							
							result.put("subcategory","_sports_"+subcategorydata);
						}
						
						
	                    if (audience_segment.trim().toLowerCase().equals("travel")) {
							
							result.put("subcategory","_travel_"+subcategorydata);
						}
	                    
	                   
					//	if (audience_segment.trim().toLowerCase().equals("education")) {
							
					//		result.put("subcategory", "_education_"+subcategorydata);
					//	}

						if (audience_segment.trim().toLowerCase().equals("technology")) {
							
							result.put("subcategory","_technology_"+subcategorydata);
						}

						if (audience_segment.trim().toLowerCase().equals("business")) {
							
							result.put("subcategory","_business_"+subcategorydata);
						}
						
						
	                    if (audience_segment.trim().toLowerCase().equals("foodbeverages")) {
							
							result.put("subcategory","_foodbeverages_"+subcategorydata);
						}
					
	                    
	                    if (audience_segment.trim().toLowerCase().equals("food_beverages")) {
							
	                    	result.put("audience_segment","foodbeverages");
	                    	result.put("subcategory","_foodbeverages_"+subcategorydata);
						}
	                    
	                    
	                    if(subcategorydata == null || subcategorydata.isEmpty() == true )
	                    {
	                    	result.put("subcategory","");
	                    	
	                    	
	                    }
	                    
	                    
					    if(channel_name.trim().toLowerCase().equals("cuberoot"))
						   result.put("subcategory","_business_startups");
					
					}

					if (deviceProperties != null) {
						result.put(
								"screen_properties",
								deviceProperties.getPhysical_screen_width()
										+ "_"
										+ deviceProperties
												.getPhysical_screen_height());
						result.put("resolution_properties",
								deviceProperties.getResolution_width() + "_"
										+ deviceProperties.getResolution_height());
						result.put("isWireless",
								deviceProperties.getWireless_device());
						
						
					    result.put("picture_gif", deviceProperties.getPicture_gif());
					    result.put("picture_jpg", deviceProperties.getPicture_jpg());
					    result.put("picture_png", deviceProperties.getPicture_png());
					    result.put("gif_animated", deviceProperties.getGif_animated());
					    result.put("streaming_video", deviceProperties.getStreaming_video()) ;
					    result.put("streaming_mp4", deviceProperties.getStreaming_mp4());
					    result.put("streaming_3gpp", deviceProperties.getStreaming_3gpp());
					  //  result.put("streaming_video", deviceProperties.getStreaming_video());
					  //  result.put("streaming_mp4", deviceProperties.getStreaming_mp4());
					    result.put("streaming_mov", deviceProperties.getStreaming_mov());
					    result.put("colors", deviceProperties.getColors());
					    result.put("dual_orientation", deviceProperties.getDual_orientation());
					    result.put("ux_full_desktop", deviceProperties.getUx_full_desktop());
					  
					    //Identifies device Type
					    if(deviceProperties.getWireless_device().equals("false"))
							result.put("device","Computer");
							
					    if(deviceProperties.getIs_tablet().equals("true"))
								result.put("device","Tablet");
						
					    if(deviceProperties.getIs_tablet().equals("false") && deviceProperties.getWireless_device().equals("true"))
					            result.put("device","Mobile");
					  
					    //Below code does mobile device properties enhancement using scraped 91 mobiles data
					    //This code is commented because at the time of Enhancement fetching from Mysql is slow and does not scale well (possible for low volume clients), possible enhancement is to cache in memory, use Elasticsearch for storing the data
					    //Index can be created in ES very similar to Entity index and these Entities (extra device properties can be fetched from ES and embedded as data points as below)
					    //Relevant module - get91mobilesData method in getWurflData
					    /*
					    String mobilesId =  deviceProperties.getBrandName()+" "+deviceProperties.getModel_name()+" "+deviceProperties.getMarketing_name()+" "+deviceProperties.getRelease_date();
					    
					    String mobileproperties = GetWurflData.get91mobilesData(mobilesId.trim());
					    
					    String [] otherProperties = mobileproperties.split(":"); 
					    
					        if( otherProperties[0]!= null)
						    otherProperties[0]= otherProperties[0].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[1]!= null)
						    otherProperties[1]= otherProperties[1].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[2]!= null)
						    otherProperties[2]=	otherProperties[2].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[3]!= null)
						    otherProperties[3]= otherProperties[3].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[4]!= null)
						    otherProperties[4]= otherProperties[4].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[5]!= null)
						    otherProperties[5]= otherProperties[5].replace(" ", "_").replace("-", "_").replace(",", "_");
						    		
					    		
					    		
					    		
						result.put("price_range",otherProperties[0]);
						result.put("processor",otherProperties[1]);
					    result.put("display",otherProperties[2]);
					    result.put("primarycamera",otherProperties[3]);
					    result.put("battery",otherProperties[4]);
					    result.put("frontcamera",otherProperties[5]);
					    */
					} 	    
				     
					
					
					  String referrer = (String)result.get("referrer");
			          
			          String referrer1 = (String)result.get("refcurrent");
			          
			          String referrer2 = (String)result.get("reforiginal");
			          
			          String referrer3 = (String)result.get("refcurrentoriginal");
			          
			          String source = "";
			          
			          String s = referrer3.trim().toLowerCase();
			          boolean isWeb = (s.startsWith("http://")) || (s.startsWith("https://"));
			          if (isWeb) {
			            source = EnhanceUserDataDaily.getHostName(referrer3);
			          }
			          s = referrer2.trim().toLowerCase();
			          isWeb = (s.startsWith("http://")) || (s.startsWith("https://"));
			          if (isWeb) {
			            source = EnhanceUserDataDaily.getHostName(referrer2);
			          }
			          if (referrer.toLowerCase().contains("m_facebook")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("lm_facebook")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("instagram")) {
			            source = "instagram.com";
			          }
			          if (referrer.toLowerCase().contains("l_facebook")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("www_facebook")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("googlesyndication")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("lm_facebook")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("web_facebook")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("mobile_facebook")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("touch_facebook")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("google_co_in")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("news_google")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("google_co_in_url")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("google_co_in_search")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("www_google_com")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("www_google_com_url")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("www_google_com_search")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("www_google_co_uk")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("www_google_co_es")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("www_google_ca")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("www_google_c")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("search_yahoo")) {
			            source = "search.yahoo.com";
			          }
			          if (referrer.toLowerCase().contains("news_google_co")) {
			            source = referrer2;
			          }
			          if (referrer.toLowerCase().contains("linkedin_co")) {
			            source = "linkedin.com";
			          }
			          if (referrer.toLowerCase().contains("stumbleupon_co")) {
			            source = "stumbleupon.com";
			          }
			          if (referrer.toLowerCase().contains("pinterest_co")) {
			            source = "pinterest.com";
			          }
			          if (referrer.toLowerCase().contains("zapmeta_co")) {
			            source = "zapmeta.com";
			          }
			          if (referrer.toLowerCase().contains("bing_co")) {
			            source = "bing.com";
			          }
			          if (referrer.toLowerCase().contains("twitter_co")) {
			            source = "twitter.com";
			          }
			          if (referrer.toLowerCase().contains("android_app___com_google_android_googlequicksearchbox")) {
			            source = referrer2;
			          }
			          if ((referrer.toLowerCase().contains("android")) && (referrer.toLowerCase().contains("facebook"))) {
			            source = referrer2;
			          }
			          if ((referrer.toLowerCase().contains("android")) && (referrer.toLowerCase().contains("linkedin"))) {
			            source = referrer2;
			          }
			          if (referrer1.toLowerCase().contains("m_facebook")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("instagram")) {
			            source = "instagram.com";
			          }
			          if (referrer1.toLowerCase().contains("lm_facebook")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("l_facebook")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("www_facebook")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("googlesyndication")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("lm_facebook")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("web_facebook")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("mobile_facebook")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("touch_facebook")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("google_co_in")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("news_google")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("google_co_in_url")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("google_co_in_search")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("www_google_com")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("www_google_com_url")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("www_google_com_search")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("www_google_co_uk")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("www_google_es")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("www_google_ca")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("search_yahoo")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("news_google_co")) {
			            source = referrer3;
			          }
			          if (referrer1.toLowerCase().contains("linkedin_co")) {
			            source = "linkedin.com";
			          }
			          if (referrer1.toLowerCase().contains("stumbleupon_co")) {
			            source = "stumbleupon.com";
			          }
			          if (referrer1.toLowerCase().contains("pinterest_co")) {
			            source = "pinterest.com";
			          }
			          if (referrer1.toLowerCase().contains("zapmeta_co")) {
			            source = "zapmeta.com";
			          }
			          if (referrer1.toLowerCase().contains("bing_co")) {
			            source = "bing.com";
			          }
			          if (referrer1.toLowerCase().contains("twitter_co")) {
			            source = "twitter.com";
			          }
			          if (referrer1.toLowerCase().contains("wikipedia_co")) {
			            source = "wikipedia.com";
			          }
			          if (referrer1.toLowerCase().contains("android_app___com_google_android_googlequicksearchbox")) {
			            source = referrer3;
			          }
			          if ((referrer1.toLowerCase().contains("android")) && (referrer1.toLowerCase().contains("facebook"))) {
			            source = referrer3;
			          }
			          if ((referrer1.toLowerCase().contains("android")) && (referrer1.toLowerCase().contains("linkedin"))) {
			            source = referrer3;
			          }
					
					
					//Does Source Url Enhancement
			          result.put("sourceUrl", source);
					
			          String referrerv1 = (String)result.get("referrer");
			          //  String []parts = subcategory.split("_");
			      	//  String subcategory1 = parts[parts.length -1];
			            String referrerv2 = (String)result.get("refcurrent");
			            
			            String sourcev1 = "Others";
			            
			        
			            for (String line : lines){    
			            
			            	if(line !=null){
			            	line =  EnhanceUserDataDaily.getHostName(line);
			            	
			      	        if(referrerv1.toLowerCase().contains(line) && referrerv2.toLowerCase().contains(line)) 	
			                  sourcev1="Direct";
			            	}
			            	
			            	}
			      	        if(referrerv1.toLowerCase().contains("google") || referrerv2.toLowerCase().contains("google"))
			          	          sourcev1="Search";
			          	  
			          	    else if(referrerv1.toLowerCase().contains("facebook") || referrerv2.toLowerCase().contains("facebook"))
			          	      	  sourcev1="Social";
			          	    
			          	    else  if(referrerv1.toLowerCase().contains("linkedin") || referrerv2.toLowerCase().contains("linkedin"))
			          	          sourcev1="Linkedin";
			          	    
			          	    else if(referrerv1.toLowerCase().contains("bing") || referrerv2.toLowerCase().contains("bing"))
			          	    	  sourcev1="Search";	    	  
			          	    	
			          	    else if(referrerv1.toLowerCase().contains("pinterest") || referrerv2.toLowerCase().contains("pinterest")) 	
			          	          sourcev1="Pinterest";
			            	    
			          	    else if(referrerv1.toLowerCase().contains("stumble") || referrerv2.toLowerCase().contains("stumble")) 	
			            	          sourcev1="StumbleUpon";
			            	    
			          	    else if(referrerv1.toLowerCase().contains("yahoo") || referrerv2.toLowerCase().contains("yahoo")) 	
			          	          sourcev1="Search";
			          	    
			          	    else if(referrerv1.toLowerCase().contains("duckduckgo") || referrerv2.toLowerCase().contains("duckduckgo")) 	
			        	              sourcev1="Search";
			          	    
			          	    else if(referrerv1.toLowerCase().contains("zapmeta") || referrerv2.toLowerCase().contains("zapmeta")) 	
			        	              sourcev1="Search";
			          	    
			          	    
			          	    else if(referrerv1.toLowerCase().contains("wikipedia")|| referrerv2.toLowerCase().contains("wikipedia")) 	
			      	              sourcev1="Wikipedia";
			          	    
			          	    else if(referrerv1.toLowerCase().contains("twitter")|| referrerv2.toLowerCase().contains("twitter")) 	
			      	              sourcev1="Twitter";
			        	    
			          	    else if(referrerv1.toLowerCase().contains("instagram")|| referrerv2.toLowerCase().contains("instagram")) 	
			      	              sourcev1="Instagram";
			        	    
			      	      //Does Referrer Type Enhancement   

			      	      result.put("referrerType", sourcev1);
			      	        
			      	        
				   //  bulkProcessor.add(new IndexRequest().index(ES_INDEX).type(this.hit.getType()).source(result)
					 //       );
	    	
			      	    IndexCategoriesData.doIndex(client,
								"enhanceduserdatabeta1", "core2", hit.getId(),result);
	    	
				
	    	
	    	}
			
			 catch (Exception e) {
                //Bulk Process was not used submitting index request for bulk indexing
				//Though it speeds up module but leads to data loss
				//Some more configuration trials can be experimented but it is slighly risky, it is available in BulkProcessorModule 
				//bulkProcessor.add(new IndexRequest().index(ES_INDEX).type(this.hit.getType()).source(result)
				//	        ); 
				//In case any Exception occurs Index partial data points Enhancement
				 IndexCategoriesData.doIndex(client,
							"enhanceduserdatabeta1", "core2", hit.getId(),result);
				 e.printStackTrace();
				

			}
 
	    	  finally {
	    	//  bulkProcessor.close();
	          }
	   
        }
		
	 
	
	
}
