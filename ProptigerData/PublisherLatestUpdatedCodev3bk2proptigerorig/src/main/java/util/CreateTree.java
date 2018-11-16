package util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.publisherdata.model.PublisherReport;

public class CreateTree {

	public static PublisherReport deserialize(String PublisherReport)  
            throws UnsupportedEncodingException {  
       PublisherReport result = null;  
       Stack<PublisherReport> stack = new Stack<PublisherReport>();  
       boolean isData = false;  
       StringBuilder data = null;  
       List<PublisherReport> pubreport = new ArrayList<PublisherReport>();
       for (int i = 0; i < PublisherReport.length(); i++) {  
            if (PublisherReport.charAt(i) == '.') {  
                 isData = !isData;  
                 if (isData) {  
                      data = new StringBuilder();  
                 } else {  
                      PublisherReport child = new PublisherReport();  
                      if (!stack.isEmpty()) {  
                           PublisherReport parent = stack.peek();  
                           parent.setChildren(pubreport);  
                      } else {  
                           result = child;  
                      }  
                      stack.push(child);  
                 }  
            } else {  
                 if (isData) {  
                      data.append(PublisherReport.charAt(i));  
                 } else if (PublisherReport.charAt(i) == ')') {  
                      stack.pop();  
                 } else {  
                      throw new UnsupportedEncodingException(  
                                "Format not recognized.");  
                 }  
            }  
       }  
       return result;  
  }  










}
