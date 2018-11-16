package com.cuberoot.SBanner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;



public class TestUtil {

	public static void main(String[] args) throws IOException, ScriptException {
		// TODO Auto-generated method stub
/*
		
		String[][] allcampinfo = new String [100][100];
		
		DemographicsCalc calc = new DemographicsCalc();
	    calc.calcList(allcampinfo, "1016", "1034", "3530912729.2438031570.1960223871");
	
	    SegmentCalc calc1 = new SegmentCalc();
	    calc1.calcList(allcampinfo, "1016", "1034", "575962882.967132425.3646610088");
	*/
		
		
		  URL url;

	            // get URL content
	        	String USER_AGENT = "Mozilla/5.0";
	            String a="http://lightning.dc.cuberoot.co/tbnr/calcutta.html";
	            String out = new Scanner(new URL(a).openStream(), "UTF-8").useDelimiter("\\A").next();
	            
	            ScriptEngineManager manager = new ScriptEngineManager();
        	    ScriptEngine engine = manager.getEngineByName("nashorn");
        	    engine.eval(out);
        	    
        	    URL obj = null;
				try {
					obj = new URL(a);
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                // 	    System.out.println("PublisherClickUrl:" + pub_click_url);
                     	
                     	HttpURLConnection con = null;
						try {
							con = (HttpURLConnection) obj.openConnection();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

                 		// optional default is GET
                 		con.setRequestMethod("GET");

                 		//add request header
                 		con.setRequestProperty("User-Agent", USER_AGENT);

                 		int responseCode = con.getResponseCode();
                 		//String content = con.getContent().
                 		 
                 		//con.disconnect();
	        

	}
}
