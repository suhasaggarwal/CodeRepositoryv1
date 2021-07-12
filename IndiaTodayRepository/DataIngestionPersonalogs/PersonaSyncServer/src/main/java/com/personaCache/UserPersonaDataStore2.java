package com.personaCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.PriorityQueue;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.gson.Gson;

class UserPersonaDataStore2 {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		String LogFolderPathArg = "I:\\LogData";
		String accessLogPrefix = "EhcacheUploadFilepart";
		String accessLogFileExt = "txt";
		
		
		RestHighLevelClient client = new RestHighLevelClient(
			RestClient.builder(new HttpHost("192.168.103.138", 9200, "http")));

		String logEntry;

		if (args[0] == null) {
			System.err.println("Enter log folder path in args");
			System.exit(1);
		}
		String logFolderPathArg = args[0];
		
		BufferedReader br1 = null;
		BufferedReader br2 = null;
		BufferedReader br3 = null;
		BufferedReader br4 = null;
		BufferedReader br5 = null;
		BufferedReader br6 = null;
		List<String> segmentlist;
		List<String> emotionlist;
		List<String> genderlist;
		List<String> incomelist;
		List<String> agegrouplist;
		String segmentsDatabase = logFolderPathArg+"/Segments.txt";
        String emotionsDatabase = logFolderPathArg+"/emotions.txt";
        String genderDatabase = logFolderPathArg+"/gender.txt";
        String incomeDatabase = logFolderPathArg+"/income.txt";
        String agegroupDatabase = logFolderPathArg+"/agegroup.txt";
		try (Stream<String> lines = Files.lines(Paths.get(segmentsDatabase))) {
			segmentlist = lines.collect(Collectors.toList());
		}
		try (Stream<String> lines = Files.lines(Paths.get(emotionsDatabase))) {
			emotionlist = lines.collect(Collectors.toList());
		}
		try (Stream<String> lines = Files.lines(Paths.get(genderDatabase ))) {
			genderlist = lines.collect(Collectors.toList());
		}
		try (Stream<String> lines = Files.lines(Paths.get(incomeDatabase))) {
			incomelist = lines.collect(Collectors.toList());
		}
		try (Stream<String> lines = Files.lines(Paths.get(agegroupDatabase))) {
			agegrouplist = lines.collect(Collectors.toList());
		}
		
		try {
			br1 = new BufferedReader(new FileReader(logFolderPathArg+"/Segments.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			br2 = new BufferedReader(new FileReader(logFolderPathArg+"/emotions.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			br3 = new BufferedReader(new FileReader(logFolderPathArg+"/gender.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			br4 = new BufferedReader(new FileReader(logFolderPathArg+"/income.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			br5 = new BufferedReader(new FileReader(logFolderPathArg+"/agegroup.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
		
		File logFolder = new File(logFolderPathArg);
		
		if (logFolder.isDirectory()) {

			String[] logFileList = logFolder.list();
			try {
				XContentBuilder builder = XContentFactory.jsonBuilder();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			Map<String, String> map = new HashMap<String, String>();
			
			for (String lf : logFileList) {
				if (lf.startsWith(accessLogPrefix)) {
					BufferedReader br = null;
					try {
						br = new BufferedReader(new FileReader(logFolderPathArg + "/" + lf));
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

					while ((logEntry = br.readLine()) != null) {

						ArrayList<String> topicList = new ArrayList<String>();
						Set<String> latestFrequentCategories = new LinkedHashSet<String>();
						ArrayList<String> latestFrequentCategories1 = new ArrayList<String>();
						ArrayList<String> mostFrequentCategories = new ArrayList<String>();
						ArrayList<String> mostFrequentCategories1 = new ArrayList<String>();
						ArrayList<String> taglist = new ArrayList<String>();
						Set<String> redundantChecker = new LinkedHashSet<String>();
						ArrayList<String> latestFrequentSection = new ArrayList<String>();
						ArrayList<String> latestFrequentSection1 = new ArrayList<String>();
						ArrayList<String> mostFrequentSection = new ArrayList<String>();
						ArrayList<String> mostFrequentSection1 = new ArrayList<String>();
						ArrayList<String> genderList = new ArrayList<String>();
						ArrayList<String> ageList = new ArrayList<String>();
						ArrayList<String> incomelevelList = new ArrayList<String>();
						ArrayList<String> sectionlist = new ArrayList<String>();
						ArrayList<String> sectionengagementlist = new ArrayList<String>();
						ArrayList<String> segmentengagementlist = new ArrayList<String>();
						ArrayList<String> topicengagementlist = new ArrayList<String>();
						ArrayList<String> affinitysegmentlist = new ArrayList<String>();
						ArrayList<String> sectiondatalist = new ArrayList<String>();
						ArrayList<String> topiclist = new ArrayList<String>();
						ArrayList<String> affinitysegmentetlist = new ArrayList<String>();
						ArrayList<String> sectiondataetlist = new ArrayList<String>();
						ArrayList<String> topicetlist = new ArrayList<String>();
						ArrayList<String> mlsignalslist = new ArrayList<String>();
						ArrayList<String> finalsectionlist = new ArrayList<String>();
						ArrayList<String> topicpaddedlist = new ArrayList<String>();
						ArrayList<String> segmentpaddedlist = new ArrayList<String>();
						ArrayList<String> sectionpaddedlist = new ArrayList<String>();
						ArrayList<String> affinitylistwithoutcodes = new ArrayList<String>();
						ArrayList<String> extradataList = new ArrayList<String>();
						Map affinitylistwithoutcodesmap = new HashMap<String, String>();
						PriorityQueue<AffinityPair> queue = new PriorityQueue<AffinityPair>(100,
								new Comparator<AffinityPair>() {

									public int compare(AffinityPair a, AffinityPair b) {
										return b.count - a.count;
									}
								});

						PriorityQueue<TopicPair> queue1 = new PriorityQueue<TopicPair>(100,
								new Comparator<TopicPair>() {

									public int compare(TopicPair a, TopicPair b) {
										return b.count - a.count;
									}
								});

						PriorityQueue<SectionPair> queue2 = new PriorityQueue<SectionPair>(100,
								new Comparator<SectionPair>() {

									public int compare(SectionPair a, SectionPair b) {
										return b.count - a.count;
									}
								});

						if (logEntry.contains("@@")) {
							String[] personaData = logEntry.split(":@");
							String localstorageID = personaData[0];
							String[] personaParts = logEntry.split("@@");
							UserProfile profile = new UserProfile();
							if (personaParts.length > 4 && personaParts[4] != null && !personaParts[4].equals("##")) {
								String[] affinityParts = personaParts[4].split(",");
								String code = null;
								int i = 1;
								for (String part : affinityParts) {
									mostFrequentCategories1.add(part.toLowerCase());
									if (i % 2 == 0 && !part.equals("0")) {
										affinitylistwithoutcodes.add(part);
										affinitylistwithoutcodesmap.put(part, code);
									} else {
										if (!part.equals("0"))
											code = part;
									}

									i++;
								}
							}

							if (personaParts.length > 9 && personaParts[9] != null && !personaParts[9].equals("##")) {
								String[] topicParts = personaParts[9].split(",");
								for (String part : topicParts) {
									if (!part.equals("0"))
										topicList.add(part.toLowerCase());
								}
							}

							if (personaParts.length > 5 && personaParts[5] != null && !personaParts[5].equals("##")) {
								String[] sectionsegments = personaParts[5].split(",");
								Integer marker0 = 30;
								if (sectionsegments.length < 30) {
									marker0 = sectionsegments.length;
								}
								int j = 0;
								boolean remainingdataTracker = false;
								for (int i = 0; i < marker0; i++) {
									if (!sectionsegments[i].equals("itweben") && !sectionsegments[i].equals("ajtk")
											&& remainingdataTracker == false)
										sectionlist.add(sectionsegments[i]);

									if (sectionsegments[i].contains("Quarter")) {
										extradataList.add(sectionsegments[i]);
										break;
									}

									if (remainingdataTracker == true) {
										extradataList.add(sectionsegments[i]);
									}

									if (sectionsegments[i].equals("itweben") || sectionsegments[i].equals("ajtk")) {
										remainingdataTracker = true;
										extradataList.add(sectionsegments[i]);
									}

								}

								boolean sectionTracker = false;
								Integer start = sectionlist.size() + extradataList.size();
								if (start <= 15)
									start = 15;
								Integer marker1 = start + personaParts[9].split(",").length;
								Integer marker2 = marker1 + personaParts[4].split(",").length
										- affinitylistwithoutcodes.size();
								Integer marker3 = marker2 + 15;

								for (int i = start; i < sectionsegments.length; i++) {
									if (i < marker1) {
										if (!sectionsegments[i].equals("0"))
											topicengagementlist.add(sectionsegments[i]);
									}
									if (i >= marker1 && i < marker2) {
										if (!sectionsegments[i].equals("0"))
											segmentengagementlist.add(sectionsegments[i]);
									}
									if (i >= marker2 && i < marker2 + sectionlist.size()) {
										sectionengagementlist.add(sectionsegments[i]);
									}
									if (i >= marker3) {
										mlsignalslist.add(sectionsegments[i]);
									}

								}
							}

							Stream<String> sectionStream = sectionlist.stream();
							Stream<String> sectionEngagementStream = sectionengagementlist.stream();
							Stream<String> segmentStream = affinitylistwithoutcodes.stream();
							Stream<String> segmentEngagementStream = segmentengagementlist.stream();
							Stream<String> topicStream = topicList.stream();
							Stream<String> topicEngagementStream = topicengagementlist.stream();
							String entity = "";
							Integer metric = 0;
							SectionPair spair = null;
							AffinityPair apair = null;
							TopicPair tpair = null;
							Integer optimumPersonaSize = 30;
							if (affinitylistwithoutcodes.size() < optimumPersonaSize) {
								for (int i = 0; i < 30 - affinitylistwithoutcodes.size(); i++) {
									segmentpaddedlist.add("0");
								}
							} else {
								for (int i = 0; i < 30 - optimumPersonaSize; i++) {
									segmentpaddedlist.add("0");
								}
							}

							if (topicList.size() < optimumPersonaSize) {
								for (int i = 0; i < 30 - topicList.size(); i++) {
									topicpaddedlist.add("0");
								}
							} else {
								for (int i = 0; i < 30 - optimumPersonaSize; i++) {
									topicpaddedlist.add("0");
								}
							}

							for (int i = sectionlist.size(); i < 30; i++) {
								sectionpaddedlist.add("0");
							}

							List<SectionPair> sectionpairList = Streams
									.zip(sectionStream, sectionEngagementStream, SectionPair::new)
									.collect(Collectors.toList());

							List<AffinityPair> segmentpairList = Streams
									.zip(segmentStream, segmentEngagementStream, AffinityPair::new)
									.collect(Collectors.toList());

							List<TopicPair> topicpairList = Streams
									.zip(topicStream, topicEngagementStream, TopicPair::new)
									.collect(Collectors.toList());

							for (SectionPair sp : sectionpairList) {
								queue2.offer(sp);
							}

							for (AffinityPair ap : segmentpairList) {
								queue.offer(ap);
							}

							for (TopicPair tp : topicpairList) {
								queue1.offer(tp);
							}

							while (queue.size() > 0) {
								apair = queue.poll();
								entity = apair.affinitysegment;
								metric = apair.count;
								affinitysegmentlist.add(affinitylistwithoutcodesmap.get(entity).toString());
								affinitysegmentlist.add(entity);
								affinitysegmentetlist.add(metric.toString());
								if (affinitysegmentetlist.size() == optimumPersonaSize) {
									break;
								}
							}

							while (queue1.size() > 0) {
								tpair = queue1.poll();
								entity = tpair.topic;
								metric = tpair.count;
								topiclist.add(entity);
								topicetlist.add(metric.toString());
								if (topicetlist.size() == optimumPersonaSize) {
									break;
								}
							}

							while (queue2.size() > 0) {
								spair = queue2.poll();
								entity = spair.sectionsegment;
								metric = spair.count;
								sectiondatalist.add(entity);
								sectiondataetlist.add(metric.toString());
							}

							sectiondatalist.addAll(extradataList);
							for (int i = sectiondatalist.size(); i < 30; i++) {
								sectiondatalist.add("0");
							}

							sectiondataetlist.addAll(sectionpaddedlist);
							topiclist.addAll(topicpaddedlist);
							topicetlist.addAll(topicpaddedlist);
							affinitysegmentlist.addAll(segmentpaddedlist);
							affinitysegmentetlist.addAll(segmentpaddedlist);

							finalsectionlist = Lists.newArrayList(Iterables.concat(sectiondatalist, topicetlist,
									affinitysegmentetlist, sectiondataetlist, mlsignalslist));

							XContentBuilder builder = XContentFactory.jsonBuilder();
							
							try {

								if (personaParts.length > 0 && personaParts[0] != null && !personaParts[0].equals("##"))
									profile.setCity(personaParts[0].toLowerCase());

								if (personaParts.length > 1 && personaParts[1] != null && !personaParts[1].equals("##"))
									profile.setCountry(personaParts[1].toLowerCase());

								if (personaParts.length > 2 && personaParts[2] != null && !personaParts[2].equals("##"))
									profile.setMobileDevice(personaParts[2].toLowerCase());

								if (personaParts.length > 3 && personaParts[3] != null
										&& !personaParts[3].equals("##")) {
									String[] parts = personaParts[3].split(",");
									for (String part : parts) {
										if (part.contains("Seg") == false) {
											latestFrequentCategories1.add(part.toLowerCase());
										} else {
											latestFrequentCategories1.add(part);
										}
									}
									profile.setInMarketSegments(latestFrequentCategories1);
								}
								if (personaParts.length > 4 && personaParts[4] != null
										&& !personaParts[4].equals("##")) {

									profile.setAffinitySegments(affinitysegmentlist);
								}
								if (personaParts.length > 5 && personaParts[5] != null
										&& !personaParts[5].equals("##")) {
									profile.setSection(finalsectionlist);
								}
								if (personaParts.length > 6 && personaParts[6] != null
										&& !personaParts[6].equals("##")) {
									String[] parts = personaParts[6].split(",");
									for (String part : parts) {
										genderList.add(part.toLowerCase());
									}
									profile.setGender(genderList);
								}
								if (personaParts.length > 7 && personaParts[7] != null
										&& !personaParts[7].equals("##")) {
									String[] parts = personaParts[7].split(",");
									for (String part : parts) {
										ageList.add(part.toLowerCase());
									}
									profile.setAge(ageList);
								}
								if (personaParts.length > 8 && personaParts[8] != null
										&& !personaParts[8].equals("##")) {
									String[] parts = personaParts[8].split(",");
									for (String part : parts) {
										incomelevelList.add(part.toLowerCase());
									}
									profile.setIncomelevel(incomelevelList);
								}
								if (personaParts.length > 9 && personaParts[9] != null
										&& !personaParts[9].equals("##")) {
									profile.setTags(topiclist);
								}

							} catch (Exception e) {
								e.printStackTrace();
							}

							Map<String,String> persona = new HashMap<String,String>();
						    Integer topiccounter = 1;
							Integer segmentcounter = 1;
							Integer tracker = 1;
							Integer sectioncounter = 1;
							Integer mlsignalscounter = 1;
						    for (String topic :topiclist) {
									persona.put("topic"+topiccounter, topic);
							        topiccounter++;
						    }
						    topiccounter=1;
						    for (String topicet :topicetlist) {
								persona.put("topic"+topiccounter+"engagementtime", topicet);
								topiccounter++;
						    }		 						    						    
						    for (String affinitysegment :affinitysegmentlist) {
								if(affinitysegment.contains("Seg"))
						    	persona.put("affinitysegmentcode"+segmentcounter, affinitysegment);
								if(tracker %2 == 0 || affinitysegment.equals("0") ) {
									persona.put("affinitysegment"+segmentcounter, affinitysegment);
									segmentcounter++;
								}
								tracker++;
						    }
						    segmentcounter=1;
					        for (String affinitysegmentet :affinitysegmentetlist) {
							persona.put("affinitysegment"+segmentcounter+"engagementtime", affinitysegmentet);
							segmentcounter++;
					        }		 
					        for (String sectionsegment :sectiondatalist) {
                               
					        	if(sectionsegment.toLowerCase().contains("ajtk")) {
                                	persona.put("aajtakchannel", "true");
								}                 
                                
					        	if(sectionsegment.toLowerCase().contains("itweben")) {
                                	persona.put("indiatodaychannel", "true");
								} 
                               
					        	if(sectionsegment.toLowerCase().contains("quarter")) {
                                	persona.put("quarter", sectionsegment);
								} 
					        	
								if(!sectionsegment.toLowerCase().contains("ajtk") && !sectionsegment.toLowerCase().contains("itweben") && !sectionsegment.toLowerCase().contains("quarter")) {
									persona.put("section"+sectioncounter, sectionsegment);
								}
								sectioncounter++;
						    }
						    sectioncounter=1;
					        for (String sectionsegmentet :sectiondataetlist) {
						       persona.put("section"+segmentcounter+"engagementtime", sectionsegmentet);
						       sectioncounter++;
					        }
					        mlsignalscounter = 1;
					        Integer emotioncounter = 0;
					        segmentcounter = 0;
					        Integer creativecounter = 0;
	                        Integer colorcounter = 0;
	                        Integer creativeentitycounter = 0;
					        for (String mlsegments :mlsignalslist) {
						       if(mlsignalscounter == 1)
	                           persona.put("totalengagementtime", mlsegments);
						       if(mlsignalscounter == 2)
		                           persona.put("totalsessiontime", mlsegments);
						       if(mlsignalscounter == 3)
		                           persona.put("timesincelastsession", mlsegments);
						       if(mlsignalscounter == 4)
		                           persona.put("loyaltytype", mlsegments);
						       if(mlsignalscounter == 5)
		                           persona.put("totalnumberofvisits", mlsegments);
						       
						       if(mlsignalscounter > 5 && mlsignalscounter < 34)
						       {
						    	   
						    	  persona.put(emotionlist.get(emotioncounter).toLowerCase(), mlsegments); 
						    	  emotioncounter++; 
						    	   
						       }
						       if(mlsignalscounter > 33 && mlsignalscounter < 46)
						       {
						    	   
						    	  persona.put("Color"+colorcounter, mlsegments); 
						    	  colorcounter++; 
						    	   
						       }
						       if(mlsignalscounter > 45 && mlsignalscounter < 146)
						       {
						    	   
						    	 persona.put(segmentlist.get(segmentcounter).replace(" ","").toLowerCase(), mlsegments); 
							     segmentcounter++;
						    	   
						       }
						       if(mlsignalscounter > 245 && mlsignalscounter < mlsignalslist.size())
						       {
						    	   
						    	 persona.put("CreativeKnowledgeGraphEntity"+creativeentitycounter, mlsegments); 
						    	 creativeentitycounter++;
						    	   
						       }
						      				       
						       
						       mlsignalscounter++;
					        }		  
	                     /* 
					        
					        Integer malecounter = 0;
	                        Integer femalecounter = 0;
	                        Integer babycounter = 0;
	                        Integer teencounter = 0;
	                        Integer youthcounter = 0;
	                        Integer adultcounter = 0;
	                        Integer middlecounter = 0;
	                        Integer oldcounter = 0;
	                        Integer poorcounter = 0;
	                        Integer middleincomecounter = 0;
	                        Integer richcounter = 0;
					        Integer counter = 1;
	                        for (String gender : genderList) {
	                         	
					        	
					        	if(gender.contains("female")) {
	                        		  femalecounter++;
	                        	}
	                        		
	                            if(femalecounter == 1 && counter == 4)
	                        	 persona.put("female"+ "wiki'", gender); 
	                        	
	                        	if(femalecounter == 2 && counter == 8)
	                        		persona.put("female" + "twitter", gender); 
	                        		
	                       
	                            if(femalecounter == 3 && counter == 12)
	                            	persona.put("female" + "commonsense", gender); 
	                        	
	                             
					        	
	                        	if(!gender.contains("female") && gender.contains("male")) {	                        		 
	                        		   malecounter++;
	                        	}
	                        		if(malecounter == 1 && counter == 2)
		                        	
		                        	if(malecounter == 2 && counter == 4)
		                        		persona.put("male"+"twitter", gender); 
		                        				                       
		                            if(malecounter == 3 && counter == 6) 
		                            	persona.put("male"+"commonsense", gender); 
	                        	                        	
		                         
	                        	
	                        	counter++;
	                        }
	                        
	                        counter =1;
                            for (String agegroup : agegrouplist) {
                            	if(agegroup.contains("baby")) {

 	                               babycounter++;
 	                        	}
                            		
                            		if(babycounter == 1 && counter == 2)
    	                        	 persona.put("baby"+ "wiki", agegroup); 
    	                        	
    	                        	if(babycounter == 2 && counter == 14)
    	                        		persona.put("baby" + "twitter", agegroup); 
    	                        		
     	                            if(babycounter == 3 && counter == 26)
    	                            	persona.put("baby" + "commonsense", agegroup); 
    	                        	
    					        	
                            	if(agegroup.contains("teen")) {
                            	    teencounter++;
	                        	}
                            		
                            		if(teencounter == 1 && counter == 4)
    	                        	 persona.put("childrenteens"+ "wiki", agegroup); 
    	                        	
    	                        	if(teencounter == 2 && counter == 16)
    	                             persona.put("childrenteens" + "twitter", agegroup); 
    	                        		
    	                       
    	                            if(teencounter == 3 && counter == 28)
    	                             persona.put("childrenteens" + "commonsense", agegroup); 
    	                        	
    	                            
    					        	
                            	if(agegroup.contains("youth")) {

	                                youthcounter++;
	                        	}
                            		
                            		if(youthcounter == 1 && counter == 6)
    	                        	 persona.put("youthadolescent"+ "wiki", agegroup); 
    	                        	
    	                        	if(youthcounter == 2 && counter == 18)
    	                        		persona.put("youthadolescent" + "twitter", agegroup); 
    	                        		
    	                       
    	                            if(youthcounter == 3 && counter == 30)
    	                            	persona.put("youthadolescent" + "commonsense", agegroup); 
    	                        	
                            	
                                                       	
                            	if(agegroup.contains("adult")) {

	                                adultcounter++;
	                        	}
                            		if(adultcounter == 1 && counter == 8)
    	                        	 persona.put("adultmillennials"+ "wiki", agegroup); 
    	                        	
    	                        	if(adultcounter == 2 && counter == 20) 
    	                        		persona.put("adultmillennials" + "twitter", agegroup); 
    	                        		
    	                       
    	                            if(adultcounter == 3 && counter == 32)
    	                            	persona.put("adultmillennials" + "commonsense", agegroup); 
    	                        	
    					        
                            	
                            	if(agegroup.contains("middle")) {
                            	     middlecounter++;
	                        	}
                            		
                            		if(middlecounter == 1 && counter == 10)
    	                        	 persona.put("middleage"+ "wiki", agegroup); 
    	                        	
    	                        	if(middlecounter == 2 && counter == 22)
    	                        		persona.put("middleage" + "twitter", agegroup); 
    	                        		
    	                       
    	                            if(middlecounter == 3 && counter == 34)
    	                            	persona.put("middleage" + "commonsense", agegroup); 
    	                        	
    	                           
                            	
                            	
                            	
                            	if(agegroup.contains("old")) {
                            	    oldcounter++;
                            	}
                            		
                            		
                            		if(oldcounter == 1 && counter == 12)
    	                                persona.put("old"+ "wiki", agegroup); 
    	                        	
    	                        	if(oldcounter == 2 && counter == 24)
    	                        		persona.put("old" + "twitter", agegroup); 
    	                        		    	                       
    	                            if(oldcounter == 3 && counter == 36)
    	                            	persona.put("old" + "commonsense", agegroup); 
    	                        	
    	                         counter++;   
    					        	
                            }
                            
                            
                            counter = 1;
                            for (String income : incomelevelList) {
                            	if(income.contains("poor")) {
                            	    poorcounter++;
	                        	}
                            		
                            		if(poorcounter == 1 && counter == 2)
    	                        	 persona.put("poor"+ "wiki", income); 
    	                        	
    	                        	if(poorcounter == 2 && counter == 8)
    	                        		persona.put("poor" + "twitter", income); 
    	                        		
    	                       
    	                            if(poorcounter == 3 && counter == 14)
    	                            	persona.put("poor" + "commonsense", income); 
    	                        	
    	                           
    					        	
    	                        	if(income.contains("middle")) {	                        		 
    	                               	
    		                            middleincomecounter++;
    	                        	}
                            	
    	                        		if(middleincomecounter == 1 && counter == 4)
    		                        	    persona.put("middleage"+"wiki", income); 
    		                        	
    		                        	if(middleincomecounter == 2 && counter == 10)
    		                        		persona.put("middleage"+"twitter", income); 
    		                        				                       
    		                            if(middleincomecounter == 3 && counter == 16)	
    		                            	persona.put("middleage"+"commonsense", income); 
    	                        	 
    	                        	if(income.contains("rich") ) {	                        		 
    	                              	
    		                            richcounter++;
    	                        	}
                            	
    	                        		if(richcounter == 1 && counter == 6)
    		                        	    persona.put("rich"+"wiki", income); 
    		                        	
    		                        	if(richcounter == 2 && counter == 12)
    		                        		persona.put("rich"+"twitter", income); 
    		                        				                       
    		                            if(richcounter == 3 && counter == 18)
    		                            	persona.put("rich"+"commonsense", income); 
    	                        	  
    	                        	
    	                        
	                        }
					       */ 
					        persona.put("userid",localstorageID);
							
					        persona.put("timestamp", new Timestamp(System.currentTimeMillis()).toString());
					        //System.out.println(persona.toString());
							builder.map(persona);
							IndexRequest indexRequest = new IndexRequest("personadatapoints");
					    	indexRequest.source(builder);
							IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
						   
							
							
							
							
							
							
							
							
							
							
							
							// System.out.println("Cookie_id1:" + value + "Persona1:" + json);
							// if(flag == true){
							// Later on - Aggregated record will override real time persona entries
							// if(json.equals("{}") == false)
							// EhcacheKeyCodeRepository.ehcache.basicCache.put(value,json);
							// }

						}

							
					}
					
					 br.close();
				}
			}

		}
	
	
		 br2.close();
		 br3.close();
		 br4.close();
		 br5.close();
	
	
	
	}
	
	
	public Entry<String, Integer> getMaxEntry(Map<String, Integer> map) {
		Entry<String, Integer> maxEntry = null;
		Integer max = Collections.max(map.values());

		for (Entry<String, Integer> entry : map.entrySet()) {
			Integer value = entry.getValue();
			if (null != value && max == value) {
				maxEntry = entry;
			}
		}
		return maxEntry;
	}
	

}
