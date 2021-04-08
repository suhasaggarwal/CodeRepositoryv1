package com.personaCache;

import java.awt.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import io.netty.util.internal.PriorityQueue;

class UserPersonaDataStore {

	    public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		
	    String LogFolderPathArg="I:\\LogData";
	    String accessLogPrefix="EhcacheUploadFilepart";
        String accessLogFileExt="txt";
		
		
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("localhost", 9200, "http")));
    
		
		
		String logEntry;
        
        if(args[0]==null){
            System.err.println("Enter log folder path in args");
            System.exit(1);
        }
        String logFolderPathArg=args[0];
      
        File logFolder=new File(logFolderPathArg);
        
        if(logFolder.isDirectory()) {
            
        String[] logFileList=logFolder.list();
        try {
			XContentBuilder builder = XContentFactory.jsonBuilder();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        Map<String,String> map = new HashMap<String,String>();
        map.put("timestamp",new Timestamp(System.currentTimeMillis()).toString());
        for(String lf : logFileList){
           if(lf.startsWith(accessLogPrefix)){
              BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(logFolderPathArg+lf));
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
              logEntry=br.readLine();
                   
        while(logEntry!=null){       
		
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
	    UserProfile profile = new UserProfile();
		String[] personaParts = logEntry.split("@@");
		
		try {

			if (personaParts.length > 0 && personaParts[0] != null && !personaParts[0].equals("##"))
				profile.setCity(personaParts[0].toLowerCase());

			if (personaParts.length > 1 && personaParts[1] != null && !personaParts[1].equals("##"))
				profile.setCountry(personaParts[1].toLowerCase());

			if (personaParts.length > 2 && personaParts[2] != null && !personaParts[2].equals("##"))
				profile.setMobileDevice(personaParts[2].toLowerCase());

			if (personaParts.length > 3 && personaParts[3] != null && !personaParts[3].equals("##")) {
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
			if (personaParts.length > 4 && personaParts[4] != null && !personaParts[4].equals("##")) {

				profile.setAffinitySegments(affinitysegmentlist);
			}
			if (personaParts.length > 5 && personaParts[5] != null && !personaParts[5].equals("##")) {
				String section = personaParts[5];
				String []parts = section.split("@@");
			    String sectionList = parts[0];
			    
			
			}
			if (personaParts.length > 6 && personaParts[6] != null && !personaParts[6].equals("##")) {
				String[] parts = personaParts[6].split(",");
				for (String part : parts) {
					genderList.add(part.toLowerCase());
				}
				profile.setGender(genderList);
			}
			if (personaParts.length > 7 && personaParts[7] != null && !personaParts[7].equals("##")) {
				String[] parts = personaParts[7].split(",");
				for (String part : parts) {
					ageList.add(part.toLowerCase());
				}
				profile.setAge(ageList);
			}
			if (personaParts.length > 8 && personaParts[8] != null && !personaParts[8].equals("##")) {
				String[] parts = personaParts[8].split(",");
				for (String part : parts) {
					incomelevelList.add(part.toLowerCase());
				}
				profile.setIncomelevel(incomelevelList);
			}
			if (personaParts.length > 9 && personaParts[9] != null && !personaParts[9].equals("##")) {
				profile.setTags(topiclist);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}		
      }	
   }
      
        }  
        }

	    }    
	    
	    }
