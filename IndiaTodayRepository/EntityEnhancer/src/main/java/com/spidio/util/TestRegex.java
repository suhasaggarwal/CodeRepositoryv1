package com.spidio.util;

import java.io.File;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class TestRegex {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/*
		String categoryv1 ="galaxy's\\\\da///ta" .replaceAll("\\s+", "\\.").replaceAll(",", "\\.")
				.replaceAll(";", "\\.").replace("'","").replace("\\","").replace("/","");
		System.out.println("Category"+categoryv1);
        */
	
		try {
			System.setOut(new PrintStream(new File("CommandLogsAggregationv1.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		List<String> days = new ArrayList<String>();
	//	days.add("2020-03-23 ");
	//	days.add("2020-03-24 ");
		days.add("2020-03-25 ");
		days.add("2020-03-26 ");
		days.add("2020-03-27 ");
		days.add("2020-03-28 ");
		days.add("2020-03-29 ");
		days.add("2020-03-30 ");
		for (String dayvalue : days) {
	    String baseCommand = "java -jar -Xms16g -Xmx16g -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:GCPauseIntervalMillis=1000 -XX:+UseStringDeduplication ";
	    String day = dayvalue;
	    String jar = " I:///contentenhancer.jar ";
	    String jar1 = " I:///contentenhancer.jar"; 
	    String jarv1 = " I:///entityenhancer.jar ";
	    String jarv11 = " I:///entityenhancer.jar";
	    String jarv2 = " I:///etcomputeenhancer.jar ";
	    String jarv21 = " I:///etcomputeenhancer.jar";
	    String jarv3 = " I:///demographyenhancer.jar ";
	    String jarv31 = " I:///demographyenhancer.jar";
        String dateString;
	    StringBuilder finalcommand = new StringBuilder();
	    finalcommand.append("timeout 15m " + baseCommand);
	    finalcommand.append(jar);
	    StringBuilder finalcommand1 = new StringBuilder();
	    finalcommand1.append("timeout 15m " + baseCommand);
	    finalcommand1.append(jarv1);
	    StringBuilder finalcommand2 = new StringBuilder();
	    finalcommand2.append("timeout 15m " + baseCommand);
	    finalcommand2.append(jarv2);
	    StringBuilder finalcommand3 = new StringBuilder();
	    finalcommand3.append("timeout 20m " + baseCommand);
	    finalcommand3.append(jarv3);
	    DateFormat df = new SimpleDateFormat("HH:mm:ss");
	    Calendar cal = Calendar.getInstance();
	    cal.set(Calendar.HOUR_OF_DAY, 0);
	    cal.set(Calendar.MINUTE, 0);
	    cal.set(Calendar.SECOND, 0);
	    int startDate = cal.get(Calendar.DATE);
	    Integer i = 1;
	    while (cal.get(Calendar.DATE) == startDate) {
	        if (i==1) {
	    	finalcommand.append("'" + day + df.format(cal.getTime())+"'");
	    	finalcommand1.append("'" + day + df.format(cal.getTime())+"'");
	    	finalcommand2.append("'" + day + df.format(cal.getTime())+"'");
	    	finalcommand3.append("'" + day + df.format(cal.getTime())+"'");
	        }
	    	cal.add(Calendar.MINUTE, 60);
	       
	    	dateString = " '" + day + df.format(cal.getTime())+"'";
	    	finalcommand.append(dateString);
	    	finalcommand1.append(dateString);
	    	finalcommand2.append(dateString);
	    	finalcommand3.append(dateString);
	    	String command1 = finalcommand.toString().concat(" 'AJTK'");
	    	String command2 = finalcommand1.toString().concat(" 'AJTK'");
	    	String command3 = finalcommand2.toString().concat(" 'AJTK'");
	    	String command4 = finalcommand3.toString().concat(" 'AJTK'");
	    	System.out.println(command1);
	    	System.out.println("sleep 10");
	    	System.out.println(command1.replaceAll(" 'AJTK'", " 'ITWEBEN'"));
	    	System.out.println("sleep 10");
	    	System.out.println(command2);
	    	System.out.println("sleep 10");
	    	System.out.println(command2.replaceAll(" 'AJTK'", " 'ITWEBEN'"));
	    	System.out.println("sleep 10");
	    	System.out.println(command3);
	    	System.out.println("sleep 10");
	    	System.out.println(command3.replaceAll(" 'AJTK'", " 'ITWEBEN'"));
	    	System.out.println("sleep 10");
	    	System.out.println(command4);
	    	System.out.println("sleep 10");
	    	System.out.println(command4.replaceAll(" 'AJTK'", " 'ITWEBEN'"));
		    System.out.println("sleep 5m");
	    	finalcommand1.append(dateString);
	    	finalcommand2.append(dateString);
	    	finalcommand3.append(dateString);
	    	finalcommand = new StringBuilder();
            finalcommand.append("timeout 15m " + baseCommand);
            finalcommand.append(jar1);
	    	finalcommand.append(dateString);
	    	finalcommand1 = new StringBuilder();
            finalcommand1.append("timeout 15m " +baseCommand);
            finalcommand1.append(jarv11);
	    	finalcommand1.append(dateString);
	    	finalcommand2 = new StringBuilder();
            finalcommand2.append("timeout 15m " +baseCommand);
            finalcommand2.append(jarv21);
	    	finalcommand2.append(dateString);
	    	finalcommand3 = new StringBuilder();
            finalcommand3.append("timeout 20m " +baseCommand);
            finalcommand3.append(jarv31);
	    	finalcommand3.append(dateString);
	        i=0;
	    }
	        /* if (i % 2 == 0) {
	        	System.out.println(finalcommand.toString());
	            finalcommand = new StringBuilder();
	            finalcommand.append(baseCommand);
	            i=0;
	        }
	       
	        i++;
	     */
	    }
	
	}

}
