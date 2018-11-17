package com.spidio.UserSegmenter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

import javax.xml.parsers.*;

import org.w3c.dom.*;
 
public class DeltaUrlsv5 
{ 
    public static void main(String[] args) throws IOException
    {
        String url = "https://www.digit.in/single-sitemap/157";   
        FileWriter fw = new FileWriter("digiturlfulllist.txt", true);
    	BufferedWriter bw = new BufferedWriter(fw);
    	PrintWriter writer = new PrintWriter("digitfeedurllist.txt", "UTF-8");
    	BufferedWriter bw1 = new BufferedWriter(new FileWriter("digitfeedurl.txt"));
    	
    	Scanner s = new Scanner(new File("digiturlfulllist.txt"));
    	ArrayList<String> list = new ArrayList<String>();
    	while (s.hasNext()){
    	    list.add(s.next());
    	}
    	
    	Node dateNode = null;
        Node titleNode = null;
        try
        {
            DocumentBuilderFactory f = 
                    DocumentBuilderFactory.newInstance();
            DocumentBuilder b = f.newDocumentBuilder();
            Document doc = b.parse(url);
 
            doc.getDocumentElement().normalize();
            System.out.println ("Root element: " + 
                        doc.getDocumentElement().getNodeName());
       
            // loop through each item
            NodeList items = doc.getElementsByTagName("url");
            for (int i = 0; i < items.getLength(); i++)
            {
                Node n = items.item(i);
                if (n.getNodeType() != Node.ELEMENT_NODE)
                    continue;
                Element e = (Element) n;
 
                // get the "title elem" in this item (only one)
                NodeList titleList = 
                                e.getElementsByTagName("loc");
                Element titleElem = (Element) titleList.item(0);
 
                // get the "text node" in the title (only one)
                if(titleElem!=null)
                titleNode = titleElem.getChildNodes().item(0);
            
                NodeList dateList = 
                        e.getElementsByTagName("lastmod");
                Element dateElem = (Element)dateList.item(0);

        // get the "text node" in the title (only one)
                if(dateElem!=null)
                dateNode = dateElem.getChildNodes().item(0);
                
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
                Date date = new Date();  
             //   String dateData = formatter.format(date);  
               // String dateData = getYesterdayDateString();
                if(dateNode!=null && titleNode!=null && dateNode.getNodeValue().split("T")[0].compareTo("2018-05-08")==1){
                if(list.contains(titleNode.getNodeValue())==false){
                	if(!titleNode.getNodeValue().equals("https://www.digit.com"))
                {
                    bw.write(titleNode.getNodeValue()+"\n");
                	bw1.write(titleNode.getNodeValue()+"\n");
                } 
                	
                	}
                }
            
            
            
            
            
            }
       
        
        
        
        bw.close();
        bw1.close();
        
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    
    
    
    }



    public static Date yesterday() {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return cal.getTime();
    }
   
    public static String getYesterdayDateString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(yesterday());
}







}