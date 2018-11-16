package com.cuberoot.SBanner;


/** USE OF THIS 
 * 1. GETTING REQUEST FOR TRACKERS
 * 2. INSERTING THEM USING DBUTILITY CLASS
 *
 * @author RAJ
 */

import java.sql.*;

import javax.servlet.*;
import javax.servlet.http.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class ResponseTracking extends HttpServlet
{
	
	 public void init(ServletConfig sconf)
	    {
	         sc = sconf.getServletContext();
	         
	    }
	    
	 ServletContext sc; 
 
 protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException 
 {
        DBUtility dbUtil = new DBUtility();
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        java.util.Enumeration e = request.getParameterNames();       
        int count = 0;
        int mcicnt1 = 0;
        String campinfo[][];   
        String mycampinfo[][];
        String targetcmpid = null;
        String blockedCampaigns = null;
        campinfo = new String[1000][45];
        mycampinfo = new String[1000][45];
        campinfo = (String[][]) sc.getAttribute("campinfo");
        while (e.hasMoreElements()) 
        {
            count++;
            e.nextElement();
        }
        
        int tr =0;
        String clkurl="NA";
        
        if(request.getParameter("tr")==null)
        {    
            tr=0;
        }
        else
        {
          tr=Integer.parseInt(request.getParameter("tr"));
        }
        if(request.getParameter("clickThrough")==null)
        {    
            clkurl="NA";            
        }
        else
        {
          clkurl=request.getParameter("clickThrough");
        }
        
        
        String sURL = request.getParameter("sURL");
        String uuid = request.getParameter("uuid");
        String adId = request.getParameter("adId");        
        String cmpId = request.getParameter("cmpId");
        String pubId = request.getParameter("pubId");
        String chnlId = request.getParameter("chnlId");
        String advId = request.getParameter("advId");
        String crId = request.getParameter("crId");
        String ipadd = request.getHeader("X-FORWARDED-FOR"); 
        String pub_click_url = request.getParameter("pubClickUrl");
        String responseValue = request.getParameter("rv");
        String width = request.getParameter("width");
        String height = request.getParameter("height");
        String USER_AGENT = "Mozilla/5.0";
        String os = request.getParameter("os");
        String refurl = request.getParameter("refurl");
        String browser =  request.getParameter("browserinfo");
        String cookie_id = request.getParameter("cookieId");
        //      if(pub_click_url != null & !pub_click_url.equals("NA")){
  //          pub_click_url = URLDecoder.decode(pub_click_url);
   //     	pub_click_url = pub_click_url.replace("flag","&");
   //     }
        
       int outerloop = 1;
        if(ipadd == null)  
        {  
               ipadd = request.getRemoteAddr();  
        } 
        
        /*System.out.println("tr "+request.getParameter("tr"));
        System.out.println("uuid "+request.getParameter("uuid"));
        System.out.println("pubId "+request.getParameter("pubId"));
        System.out.println("cmpId "+request.getParameter("cmpId"));
        */
        
        try 
        {
                if (uuid==null || adId==null || cmpId==null || pubId==null)
                {                
                    out.println("");
                }                
                else if (uuid.equals("") || adId.equals("") || tr==0 || cmpId.equals("") || pubId.equals(""))
                {                
                    out.println("");
                }
                else
                {
                                               
                        int adID=Integer.parseInt(adId);                        
                        //int tracking=Integer.parseInt(tr);
                        int crID=0;
                        int chnlID=0;
                        int pubID=0;
                        int advID=0;
                        if(pubId==null) pubID=0;
                        else pubID=Integer.parseInt(pubId);                         
                        int cmpID=0;
                        if(cmpId==null) cmpID=0;
                        else cmpID=Integer.parseInt(cmpId);                         
                        if(crId==null) crID=0;
                        else crID=Integer.parseInt(crId); 
                        if(chnlId==null) chnlID=0;
                        else chnlID=Integer.parseInt(chnlId); 
                        if(advId==null) advID=0;
                        else advID=Integer.parseInt(advId); 
                        //registering the tracking============
                        ElasticUtil.registerResponseTrackings(uuid,adID,pubID,chnlID,cmpID,advID,crID,ipadd,tr,responseValue,width,height,os,refurl,browser,cookie_id); 
                        if(tr==101)
                        {   
                        	
                        	
                        	 for (int x1 = 0; x1 < campinfo.length; x1++) {

                        		 if(cmpId.equals(campinfo[x1][1])){
                        	         if(responseValue.equals(campinfo[x1][38]))
                        			 targetcmpid = campinfo[x1][39];
                        	
                        	        
                        	         if(responseValue.equals(campinfo[x1][40])){
                            			 targetcmpid = campinfo[x1][41];
                            			
                            			 Cookie ck[]=request.getCookies();  
                            			 for(int i=0;i<ck.length;i++){  
                            			 if(ck[i].getName().equals("BlockedCampaign")) 
                            			 {
                            			  Cookie myCookie = new Cookie("BlockedCampaign", cmpId+";"+ck[i].getValue());
                            		      blockedCampaigns = cmpId+";"+ck[i].getValue();
                            			  response.addCookie(myCookie);
                        	              outerloop = 2;
                            			  break;
                            			 
                            			 }
                        		 
                            	}		 
                            			 }
                        	
                        	         
                        	         if(outerloop ==2 )
                        	         break;
                        	         
                        	         if(responseValue.equals(campinfo[x1][42]))
                            			 targetcmpid = campinfo[x1][43];
                        		 
                        		 
                        		 
                        		 
                        		 
                        		 }	
                        	 } 
                        	     if (targetcmpid != null){
                        		 
                        		 for ( int x1 = 0; x1 < campinfo.length; x1++) {

                 
                     				 if(targetcmpid.contains(campinfo[x1][1])){
                     					
                     					    if(blockedCampaigns == null || (blockedCampaigns!=null && !blockedCampaigns.contains(targetcmpid)))
                     					 
                     					    mycampinfo[mcicnt1][0] = campinfo[x1][0];
                     			            mycampinfo[mcicnt1][1] = campinfo[x1][1];                 
                     			            mycampinfo[mcicnt1][2] = campinfo[x1][2];                   
                     			            mycampinfo[mcicnt1][3] = campinfo[x1][3];                   
                     			            mycampinfo[mcicnt1][4] = campinfo[x1][4];
                     			            mycampinfo[mcicnt1][5] = campinfo[x1][5]; 
                     			            mycampinfo[mcicnt1][6] = campinfo[x1][6];
                     			            mycampinfo[mcicnt1][7] = campinfo[x1][7]; 
                     			            mycampinfo[mcicnt1][8] = campinfo[x1][8]; 
                     			            mycampinfo[mcicnt1][9] = campinfo[x1][9]; 
                     			            mycampinfo[mcicnt1][10] = campinfo[x1][10];
                     			            mycampinfo[mcicnt1][11] = campinfo[x1][11];
                     			            
                     			            mycampinfo[mcicnt1][12] = campinfo[x1][12];
                     			            mycampinfo[mcicnt1][13] = campinfo[x1][13];
                     			            mycampinfo[mcicnt1][14] = campinfo[x1][14];
                     			            mycampinfo[mcicnt1][15] = campinfo[x1][15];
                     			            mycampinfo[mcicnt1][16] = campinfo[x1][16];
                     			            mycampinfo[mcicnt1][18] = campinfo[x1][18];
                     			            mycampinfo[mcicnt1][19] = campinfo[x1][19];
                     			            mycampinfo[mcicnt1][20] = campinfo[x1][20];
                     				        mycampinfo[mcicnt1][21] = campinfo[x1][21];
                     				        mycampinfo[mcicnt1][22] = campinfo[x1][22];
                     				        mycampinfo[mcicnt1][23] = campinfo[x1][23];
                     				        mycampinfo[mcicnt1][24] = campinfo[x1][24];
                     				        mycampinfo[mcicnt1][25] = campinfo[x1][25];
                     				        mycampinfo[mcicnt1][26] = campinfo[x1][26];
                     				        mycampinfo[mcicnt1][27] = campinfo[x1][27];
                     				        mycampinfo[mcicnt1][28] = campinfo[x1][28];
                     				        mycampinfo[mcicnt1][29] = campinfo[x1][29];
                     				        mycampinfo[mcicnt1][30] = campinfo[x1][30];
                     				        mycampinfo[mcicnt1][31] = campinfo[x1][31];
                     				        mycampinfo[mcicnt1][32] = campinfo[x1][32];
                     				        mycampinfo[mcicnt1][33] = campinfo[x1][33];
                     				        mycampinfo[mcicnt1][34] = campinfo[x1][34];
                     				        mycampinfo[mcicnt1][35] = campinfo[x1][35];
                     					
                     					    mcicnt1++;
                     					
                     			
                     				}
                     			
                     					
                             }		 
                 			
                        	     }			
                         		 
                        	
                        	
                        	
                        	
                        	
                        	/*
                        	if(pub_click_url != null && !"NA".equals(pub_click_url))
                        	{
                        	pub_click_url = URLDecoder.decode(pub_click_url);
                        	URL obj = new URL(pub_click_url);
                   // 	    System.out.println("PublisherClickUrl:" + pub_click_url);
                        	
                        	HttpURLConnection con = (HttpURLConnection) obj.openConnection();

                    		// optional default is GET
                    		con.setRequestMethod("GET");

                    		//add request header
                    		con.setRequestProperty("User-Agent", USER_AGENT);

                    		int responseCode = con.getResponseCode();
                    		
                    		 
                    		con.disconnect();
                        	}
                                                 	
                          String clickURL=request.getParameter("CTU");
                          //System.out.println(clickURL);
                          response.sendRedirect(clickURL);
                        */
                        	if(targetcmpid !=null){
                        	ProcessRequest rq = new ProcessRequest(pubId,chnlId,"",mycampinfo,ipadd,"",sURL,width,height,"","","","","","","","",uuid);             
                            String rp1 = rq.doProcess().replace("document.write(\"<div id='storyad' style='display:block;'>\");","");
                        	rp1 = rp1.replace("document.write(\"</div>\");", "");
                            rp1=rp1.replace("document.write(\"", "");
                            rp1=rp1.replace("\");","");
                        	rp1=rp1.replace("<script language='JavaScript' src='https://dcpub.cuberoot.co/dcode1/get.js'></script>", "");
                        	rp1=rp1.replace("<script language='JavaScript' src='https://dcpub.cuberoot.co/dcode1/replacecontent.js'></script>", "");
                            rp1=rp1.replace("<script language='JavaScript' src='https://dcpub.cuberoot.co/dcode1/GenerateResponseUrl.js'></script>","");
                        	rp1=rp1.replace("<script>","");
                        	rp1=rp1.replace("</script>","");
                            out.print(rp1);
                        	}
                        	else{
                        		 out.println("");
                        	}
                        
                        }    
                 }
                 //System.out.println("passed url: "+request.getParameter("fsrc"));
                 //out.println(request.getParameter("fsrc"));
                 out.println("");
                 dbUtil=null;
                  
            
        } 
        catch (Exception e1) 
        {
         //   e1.printStackTrace();
            System.out.println("Error returning callback xml");
        }
        finally 
        {
            if (dbUtil!=null)dbUtil=null;
            out.close();
        }
 }

 
 
 
 // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /** Handles the HTTP <code>GET</code> method.
     * @param request servlet request
     * @param response servlet response
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /** Handles the HTTP <code>POST</code> method.
     * @param request servlet request
     * @param response servlet response
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /** Returns a short description of the servlet.
     */
    public String getServletInfo() {
        return "Short description";
    }
    // </editor-fold>
}
