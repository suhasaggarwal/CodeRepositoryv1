package com.cuberoot.SBanner;
/*

 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/** USE OF THIS 
 * 1. LOADING THE REQUIRED INFORMATION INTO CONTEXT FOR FUTURE USE
 * 2. CLASSGETTING REQUEST FROM PUBLISHERS AND SEND BACK THE RESPONSES
 * 3. THE BASE CLASS TO CALL
 *
 * @author 
 */
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Locale;
import java.util.StringTokenizer;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sourceforge.wurfl.core.Device;

public class Controller extends HttpServlet 
{
	private static final long serialVersionUID = 1L;
	ServletContext sc;
	int finalcamplen = 0;
   //public static MemcachedKeyCodeRepository sc = MemcachedKeyCodeRepository.getInstance();
	
    
	
	
	
    public void init(ServletConfig sconf) throws ServletException
    {
        String[] siteList;
        super.init(sconf); // would set: this.config = config;
       
        sc = sconf.getServletContext();
        DBConnector db = new DBConnector();
        Connection con = null;
        Statement st = null, st1 = null, st2=null, st3=null,st4=null,st5=null,st6=null,st7=null,st8=null,st9=null,st10=null;
        ResultSet rs = null, rs1 = null, rs2 =null, rs3=null,rs4=null,rs5=null,rs6=null,rs7=null,rs8=null,rs9=null,rs10=null;
       
        
        
        
        System.out.println("The Ad serving platform is started and intialize At: "+ new java.util.Date());
        try 
        {
            String host="localhost";   //FROM LOCALHOST
	        host="101.53.137.116";//"10.12.3.125";      //from server (private ip)
            Class.forName("com.mysql.jdbc.Driver").newInstance();          
            con = DriverManager.getConnection("jdbc:mysql://"+host+":3306/lyxel_AD_Server", "wifi_spd_user", "aeiou20142015");   //connect to localhost
            //con = DriverManager.getConnection("jdbc:mysql://"+host+":3306/lyxel_AD_Server", "adserver", "q1w2e3r4t5y6");   //connect to server
            st = con.createStatement();
            /**=================getting all active publisher n channel list and save it in context---------------------**/
            String sql = "SELECT `publisher_id` FROM `publisher_detail` WHERE `status`='1'";
            //System.out.println("Servlet initialization query #1: " + sql);
            rs = st.executeQuery(sql);
            rs.last();
            int rcount = rs.getRow();
            int count = 0;
            siteList = new String[rcount];
            rs.beforeFirst();
            while (rs.next())
            {
                siteList[count] = rs.getString("publisher_id");
                count++;
            }
           
            //sc.deletesiteInfo("siteList");
            //sc.putsiteInfo("siteList", siteList);
            System.out.println("Saving siteList!!");
            sc.setAttribute("siteList", siteList);
            
            /***================================================================publisher list saved=============**/
            
            /*****===========================================Campaign mapping detail=======================***/
            String campinfo[][];            
             String sqlquery="SELECT cpbd.`campaign_id`,cpbd.`publisher_id`,cpbd.`channel_id`,C.`campaign_name`,C.`campaign_type`, cpbd.`cap`, C.`geo_targeted`, cpbd.`cpm-cpc`, C.`geo_target_type`, cpbd.`campaign_start_date`, cpbd.`campaign_end_date`, cpbd.`start_time`, cpbd.`end_time`, C.`advertiser_id` FROM `campaign_detail` C, `campaign_publisher-channel_target` cpbd,`creative_detail` ad WHERE C.`campaign_id`=cpbd.`campaign_id` AND cpbd.`status`='1' AND cpbd.`campaign_start_date`<=NOW() AND cpbd.`campaign_end_date`>=NOW() AND ad.`status`=1 GROUP BY cpbd.`channel_id`,cpbd.`campaign_id` ORDER BY cpbd.`publisher_id`";
         //   String sqlquery="SELECT cpbd.`campaign_id`,cpbd.`publisher_id`,cpbd.`channel_id`,C.`campaign_name`,C.`campaign_type`, cpbd.`cap`, C.`geo_targeted`, cpbd.`cpm-cpc`, C.`geo_target_type`, cpbd.`campaign_start_date`, cpbd.`campaign_end_date`, cpbd.`start_time`, cpbd.`end_time`, C.`advertiser_id` FROM `campaign_detail` C, `campaign_publisher-channel_target` cpbd,`creative_detail` ad WHERE C.`campaign_id`=cpbd.`campaign_id` AND cpbd.`status`='1' AND cpbd.`campaign_start_date`<=NOW() AND cpbd.`campaign_end_date`>=NOW() AND ad.`campaign_id`=C.`campaign_id` AND ad.`status`=1 GROUP BY cpbd.`publisher_id`,cpbd.`campaign_id` ORDER BY cpbd.`publisher_id`";
            //System.out.println("Servlet initialization query #2: " + sqlquery);
            st2 = con.createStatement();
            st3 = con.createStatement();
            st4 = con.createStatement();
            st5 = con.createStatement();
            st6 = con.createStatement();
            st7= con.createStatement();
            st8= con.createStatement();
            st9 = con.createStatement();
            st10 = con.createStatement();
            rs2 = st2.executeQuery(sqlquery);
            rs2.last();
            
            campinfo = new String[rs2.getRow()][45];
            rs2.beforeFirst();
            int cnter = 0;
            while (rs2.next()) 
            {
                int cmpid=rs2.getInt("campaign_id");                
                campinfo[cnter][0] = rs2.getString("publisher_id");                
                campinfo[cnter][1] = ""+cmpid; //rs2.getString("campaign_id");
                campinfo[cnter][2] = rs2.getString("campaign_name");                
                campinfo[cnter][3] = rs2.getString("geo_targeted");
                campinfo[cnter][4] = rs2.getString("channel_id");                 
                campinfo[cnter][5] = ""+rs2.getFloat("cpm-cpc");
                campinfo[cnter][6] = rs2.getString("advertiser_id");
                
                String ccsql="SELECT * FROM `campaign_country` WHERE `campaign_id`="+cmpid;
                rs3 = st3.executeQuery(ccsql);
                campinfo[cnter][12] = "";
                campinfo[cnter][13] = "";                
                campinfo[cnter][14] = "";
                while (rs3.next()) 
                {
                    campinfo[cnter][12] = rs3.getString("country_code")+"="+rs3.getString("state")+"="+rs3.getString("city")+"||"+campinfo[cnter][12]; 
                    campinfo[cnter][13] =rs3.getString("country_name")+"="+rs3.getString("state")+"="+rs3.getString("city")+"||"+campinfo[cnter][13]; 
                    
                    //System.out.println("Matching geo property for campinfo : " + cmpid);
                }
                
                campinfo[cnter][7] = ""+rs2.getInt("geo_target_type");
                if(rs2.getString("campaign_start_date").equals("0000-00-00"))
                {
                    java.text.SimpleDateFormat hdf= new java.text.SimpleDateFormat("yyyy-MM-dd");
                    campinfo[cnter][8]=hdf.format(Calendar.getInstance(Locale.US).getTime());
                
                } 
                else
                {
                campinfo[cnter][8] = ""+rs2.getString("campaign_start_date");
                }
                if(rs2.getString("campaign_end_date").equals("0000-00-00"))
                {
                    java.text.SimpleDateFormat hdf= new java.text.SimpleDateFormat("yyyy-MM-dd");
                    campinfo[cnter][9]=hdf.format(Calendar.getInstance(Locale.US).getTime());
                
                } 
                else
                {
                campinfo[cnter][9] = ""+rs2.getString("campaign_end_date");
                }
                //campinfo[cnter][9] = ""+rs2.getString("campaign_end_date");
                campinfo[cnter][10] = ""+rs2.getString("start_time");
                campinfo[cnter][11] = ""+rs2.getString("end_time");
                
                String dtsql="SELECT * FROM `campaign_device_detail` WHERE `campaign_id`="+cmpid;
                //System.out.println("CampaignId:"+ cmpid + "\n");
                
                campinfo[cnter][15] = "";
                campinfo[cnter][16] = "";
                campinfo[cnter][18] = "";
                campinfo[cnter][19] = "";
                campinfo[cnter][17] = "0";
                campinfo[cnter][20] = "";
                campinfo[cnter][21] = "";
                campinfo[cnter][22] = "";
                campinfo[cnter][23] = "";
                
                rs4 = st4.executeQuery(dtsql);
                while (rs4.next()) 
                {
                    campinfo[cnter][15] =rs4.getString("mobile_os");
                    campinfo[cnter][16] =rs4.getString("mobile_browser");
                    campinfo[cnter][18] =rs4.getString("os_targeting");
                    campinfo[cnter][19] =rs4.getString("browser_targeting");
                    campinfo[cnter][20] =rs4.getString("screen_targeting");
                    campinfo[cnter][21] =rs4.getString("screen_size");
                    campinfo[cnter][22] =rs4.getString("screen_resolution");
                    campinfo[cnter][23] =rs4.getString("screen_resolution_targeting");
                    campinfo[cnter][17] =rs4.getString("is_enabled");
                    //System.out.println("Matching device property campinfo : " + cmpid);

                } 
                
               
                String dtsql1="SELECT * FROM `campaign_demographics_detail` WHERE `campaign_id`="+cmpid;
                System.out.println("CampaignId:"+ cmpid + "\n");
                rs5 = st5.executeQuery(dtsql1);
              
                
                
                campinfo[cnter][24] = "";
                campinfo[cnter][25] = "";
                campinfo[cnter][26] = "";
                campinfo[cnter][27] = "0";
                campinfo[cnter][28] = "";
                campinfo[cnter][29] = "";
                campinfo[cnter][30] = "";
               
                
                
                            
                
                while (rs5.next()) 
                {
                	if(rs5.getString("gender")!=null)
                    campinfo[cnter][24] = rs5.getString("gender");
                  
                	if(rs5.getString("ageGroup")!=null)
                	campinfo[cnter][25] = rs5.getString("ageGroup");
                   
                	if(rs5.getString("incomeLevel")!=null)
                	campinfo[cnter][26] = rs5.getString("incomeLevel");
                   
                	
                	if(rs5.getString("is_activated")!=null)
                	campinfo[cnter][27] = rs5.getString("is_activated");
                   
                	if(rs5.getString("isGenderTargeting")!=null)
                	campinfo[cnter][28] = rs5.getString("isGenderTargeting");
                   
                	
                	if(rs5.getString("isAgeGroupTargeting")!=null)
                	campinfo[cnter][29] = rs5.getString("isAgeGroupTargeting");
                   
                
                	if(rs5.getString("isIncomeLevelTargeting")!=null)
                	campinfo[cnter][30] = rs5.getString("isIncomeLevelTargeting");
                    
           
                } 
                 
                
                String dtsql2="SELECT * FROM `campaign_segment_detail` WHERE `campaign_id`="+cmpid;
                System.out.println("CampaignId:"+ cmpid + "\n");
                rs6 = st6.executeQuery(dtsql2);
              
                campinfo[cnter][31] = "0";
                campinfo[cnter][32] = "0";
                
                while (rs6.next()) 
                {
                     if(rs6.getString("is_activated")!=null)
                	campinfo[cnter][31] = rs6.getString("is_activated");
                  
                     if(rs6.getString("is_onMatch")!=null)
                     campinfo[cnter][32] = rs6.getString("is_onMatch");
                  
                
                }
                System.out.println("Is Match Debug CampaignId:"+ cmpid + "Is Match Debug Campaigns : "+campinfo[cnter][32] );
                
                
                
                String dtsql3="SELECT * FROM `campaign_channel_retarget` WHERE `campaign_id`="+cmpid;
                System.out.println("CampaignId:"+ cmpid + "\n");
                rs7 = st7.executeQuery(dtsql3);
              
                campinfo[cnter][33] = "0";
                campinfo[cnter][34] = "0";
                
                
                while (rs7.next()) 
                {
                	if(rs7.getString("channel_name")!=null)
                    campinfo[cnter][33] = rs7.getString("channel_name");
                    
                	
                	if(rs7.getString("is_activated")!=null)
                	campinfo[cnter][34] = rs7.getString("is_activated");
                    
                }
                
                
                
                String dtsql5="SELECT * FROM `campaign_cpm` WHERE `campaign_id`="+cmpid;
                System.out.println("CampaignId:"+ cmpid + "\n");
                rs8 = st8.executeQuery(dtsql5);
              
                campinfo[cnter][35] = "0.000001";
              
                
                
                while (rs8.next()) 
                {
                	if(rs8.getString("cpm")!=null)
                    campinfo[cnter][35] = rs8.getString("cpm");
                   
                }
                
                
                String dtsql6="SELECT * FROM `frequency_cap` WHERE `campaign_id`="+cmpid;
                System.out.println("CampaignId:"+ cmpid + "\n");
                rs9 = st9.executeQuery(dtsql6);
              
                campinfo[cnter][36] = "1000000";
                campinfo[cnter][37] = "1000000";
                
                
                while (rs9.next()) 
                {
                	if(rs9.getString("cap")!=null)
                    campinfo[cnter][36] = rs9.getString("cap");
                   
                	if(rs9.getString("timedelta")!=null)
                    campinfo[cnter][37] = rs9.getString("timedelta");
                    
                
                }
                
                String dtsql7="SELECT * FROM `campaign_seq` WHERE `campaign_id`="+cmpid;
                System.out.println("CampaignId:"+ cmpid + "\n");
                rs10 = st10.executeQuery(dtsql7);
              
                campinfo[cnter][38] = "";
                campinfo[cnter][39] = "";
                campinfo[cnter][40] = "";
                campinfo[cnter][41] = "";
                campinfo[cnter][42] = "";
                campinfo[cnter][43] = "";
                
                int flag = 0;
                
                while (rs10.next()) 
                {
                	if(flag == 0){
                	
                	if(rs10.getString("campaign_code")!=null)
                    campinfo[cnter][38] = rs10.getString("campaign_code");
                   
                	if(rs10.getString("campseqId")!=null)
                    campinfo[cnter][39] = rs10.getString("campseqId");
                    
                	}
                	
                	
                	if(flag == 1){
                    	
                    	if(rs10.getString("campaign_code")!=null)
                        campinfo[cnter][40] = rs10.getString("campaign_code");
                       
                    	if(rs10.getString("campseqId")!=null)
                        campinfo[cnter][41] = rs10.getString("campseqId");
                        
                    	}
                	
                	if(flag == 2){
                    	
                    	if(rs10.getString("campaign_code")!=null)
                        campinfo[cnter][42] = rs10.getString("campaign_code");
                       
                    	if(rs10.getString("campseqId")!=null)
                        campinfo[cnter][43] = rs10.getString("campseqId");
                        
                    	}
                	
                	flag++;
                	
                	
                	
                }
                
                
                cnter++;
            }
            
            System.out.println("Saving campinfo to context.");
            sc.setAttribute("campinfo", campinfo);
            
           /*****==================Saved mapping data========================***/
        }
        catch (Exception e) 
        {
            e.printStackTrace();
            System.out.println("Error in Getting Site List/mapping List :"+ e);
        } 
        finally 
        {
            if (rs != null) 
            {
                try 
                {
                    rs.close();
                } catch (SQLException sq) {  }
                rs = null;
            }
            if (rs1 != null)
            {
                try
                {
                    rs1.close();
                } catch (SQLException sq) {  }
                rs1 = null;
            }
            if (rs2 != null)
            {
                try 
                {
                    rs2.close();
                } catch (SQLException sq) {  }
                rs2 = null;
            }
            if (rs3 != null)
            {
                try 
                {
                    rs3.close();
                } catch (SQLException sq) {  }
                rs3 = null;
            }
            if (rs4 != null)
            {
                try 
                {
                    rs4.close();
                } catch (SQLException sq) {  }
                rs4 = null;
            }
           
            if (st != null)
            {
                try 
                {
                    st.close();
                } catch (SQLException sq) {  }
                st = null;
            }
            if (st1 != null) 
            {
                try 
                {
                    st1.close();
                } catch (SQLException sq) {  }
                st1 = null;
            }
             if (st2 != null) 
             {
                try 
                {
                    st2.close();
                } catch (SQLException sq) {  }
                st2 = null;
            }
             if (st3 != null) 
             {
                try 
                {
                    st3.close();
                } catch (SQLException sq) {  }
                st3 = null;
            }
             if (st4 != null) 
             {
                try 
                {
                    st4.close();
                } catch (SQLException sq) {  }
                st4 = null;
            }

            if (con != null) 
            {
                try 
                {
                    if (!con.isClosed()) {    con.close();   }
                } catch (SQLException sq) { }
                con = null;
            }
            if (db != null) 
            {
                db = null;
            }
            siteList = null;
        
     }
    }
    
    //=============================================================processRequest==================================================//
    
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException 
    {
        
       

	//	File file = new File("/root/Controller.log");
	//	FileOutputStream fos = new FileOutputStream(file);
	//	PrintStream ps = new PrintStream(fos);
	//	System.setOut(ps);
    	
    	
    	
    	DBUtility dbu=new DBUtility();
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        
        String error = "";
        String creativeId="";
        String browser = request.getParameter("brtype");
        String os = request.getParameter("os");
        String device = request.getParameter("device");
        String chnlId = request.getParameter("chnlid");
        String width = request.getParameter("width");
        String height = request.getParameter("height");
        String siteId = request.getParameter("siteId"); //--like eros,indyarocks
        String ipAdd = request.getRemoteAddr(); 
		String fingerprintId = request.getParameter("fingerprintId");
		if(fingerprintId  != null && fingerprintId .isEmpty()==false){
			fingerprintId  = fingerprintId.replace("-","_");
		    
		    }
		
		System.out.println("fingerprintId:"+fingerprintId);
		String pub_click_url = request.getParameter("ext_click");
	    String cookie_id = request.getParameter("cookieId");
	    if(cookie_id != null && cookie_id.isEmpty()==false){
	    cookie_id = cookie_id.replace("-","_");
	    System.out.println("cookieId:"+cookie_id);
	    fingerprintId = cookie_id;
	    }
		/*
		Cookie[] cookies = request.getCookies();

		if (cookies != null) {
		 for (Cookie cookie : cookies) {
		   if (cookie.getName().equals("spdsuid")) {
			   fingerprintId = cookie.getValue();
		       fingerprintId = fingerprintId.replace("-", "_");
		       System.out.println("cookie_id:"+fingerprintId);
		   } 
		  }
		}
		*/
		
        String sURL = "";
        String refURL = ""; //request.getParameter("refURL");
        String refURLO=request.getHeader("referer");
        try
        {    
         if(refURLO!=null) { refURL=java.net.URLEncoder.encode(refURLO,"UTF-8"); }
         else if(request.getParameter("refURL")!=null){ refURL = request.getParameter("refURL"); }
         else { refURL = ""; }
             
        }
        catch(Exception w){}
        //System.out.println("encoded refurl "+refURL);  
        
        String reqUrl = request.getRequestURL().toString();
        String userAgent=request.getHeader("User-Agent"); 
        
        
     //   userAgent = "Mozilla/5.0 (Linux; Android 4.4.2; Micromax D320 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36";
        
        
    //    WURFLHolder wurflHolder = WurflHolder.getInstance();
     //   WURFLManager wurfl = wurflHolder.getWURFLManager();
     //    Device device1 = wurfl.getDeviceForRequest(userAgent);
        
        Device device1 = null;
        
        System.out.println(userAgent);
        
        //Device device1 = null;
               
        
        if (request.getParameter("crId")== null || request.getParameter("crId").equals("")|| request.getParameter("crId").equals(" "))
        {
           creativeId="";
        }
        else
        {
           creativeId=request.getParameter("crId");
        }
        
                
        try
        {
            if (ipAdd==null)
            {
                 
                 ipAdd="100.100.100.100";
                            
            }
            else if (ipAdd.equals("") || siteId.equals("")) 
            {
                   
                out.println("");
                System.out.println("IP Error");
            }
            
            else
            {
                sURL = serviceURL(reqUrl); //getting the service url
                try 
                {     
                        String IP="";
                        //System.out.println("ip "+ipAdd);    
                        if (ipAdd.indexOf("\"")==0) {       IP=ipAdd.substring(1,ipAdd.length()-1);  } 
                        else {  IP=ipAdd;    }
                        
                        //System.out.println("ip "+ipAdd); 
                        String ipc_zip="";
                        String ipc_country="";
                        String ipc_city="";
                        String ipc_region="";
                        boolean mobRequest=false;
                        
                        GeoInfo ipc = new GeoInfo(IP,sc);
                        ipc_zip=ipc.getZip();
                        ipc_country=ipc.getCountry();
                        ipc_city=ipc.getCity();
                        ipc_region=ipc.getRegion();
                        
                        System.out.println("Zip : "+ ipc_zip + " Country : "+ipc_country+" City : "+ ipc_city +" Region : "+ipc_region);
                        
                        dbu.registerAdrequest("",ipAdd,"","","","",browser,"",refURL,"",userAgent,siteId,chnlId,ipc_city,ipc_region,ipc_country);
                        //String campinfofound[][]=campInfo(siteId,chnlId,ipc_country,ipc_region,ipc_city,mobRequest, userAgent);
                        
                        String campinfofound[][] = campInfo(siteId,chnlId,ipc,device1,mobRequest, userAgent,fingerprintId);
                        
                        
                        System.out.println("CampInfo function completed. And length of matching camps is:");
                        
                     
                        ProcessRequest rq = new ProcessRequest(siteId,chnlId,creativeId,campinfofound, ipAdd, refURL, sURL,width,height,os,browser,device,ipc_zip,ipc_country,ipc_city,ipc_region,pub_click_url, fingerprintId);             
                        out.print(rq.doProcess());
                        
                         
                        /*==============HARDCODED AD FOR QUICK TEST=============================
                        String adType="1";
                        String Response="";
                        String uuid="39941c84-334e-4302-b4cb-675e99404f56";
                       if(adType.equals("1") || adType.equals("20"))   // falsh or expandable flash wiith built in expand feature
                       { 
                       //Response="<script>var hasFlash = false; try {  var fo = new ActiveXObject('ShockwaveFlash.ShockwaveFlash');  if (fo) { hasFlash = true; } } catch (e) {  if (navigator.mimeTypes  && navigator.mimeTypes['application/x-shockwave-flash'] != undefined  && navigator.mimeTypes['application/x-shockwave-flash'].enabledPlugin) {   hasFlash = true;  }} if (hasFlash) { ";   
                       Response="<script>var hasFlash = false; try {  var fo = new ActiveXObject('ShockwaveFlash.ShockwaveFlash');  if (fo) { hasFlash = true; } } catch (e) {  if (navigator.mimeTypes  && navigator.mimeTypes['application/x-shockwave-flash'] != undefined  && navigator.mimeTypes['application/x-shockwave-flash'].enabledPlugin) {   hasFlash = true;  }} if (hasFlash) { ";
                       //Response= Response+"document.write(\"<img src='"+imptr+"' border='0' width='1' height='1'>\");";
                       Response= Response+"document.write(\"<object id='"+uuid+"' classid='clsid:D27CDB6E-AE6D-11cf-96B8-444553540000' codebase='https://download.macromedia.com/pub/shockwave/cabs/flash/swflash.cab#version=6,0,0,0'";
                       Response =Response+" width='300' height='250'>";
                       Response =Response+"<param name='allowScriptAccess' value='always'/>";
                       Response =Response+"<param name='movie' value='http://54.84.151.220/demo/da/tbnr/Emi.swf'/>";
                       Response =Response+"<param name='FlashVars' value='clickTAG=http://monsoonads.com'>";
                       Response =Response+"<param name='quality' value='high'/>";
                       Response =Response+"<param name='wmode' value='Opaque'/>";
                       Response =Response+"<embed quality='high' allowScriptAccess='always' width='300' height='250' wmode='Opaque' type='application/x-shockwave-flash' pluginspage='https://www.macromedia.com/go/getflashplayer' src='http://54.84.151.220/demo/da/tbnr/Emi.swf' flashvars='clickTAG=http://monsoonads.com'></embed></object>\"); }";
                       //if(ad.hasBackUP().equals("1"))
                       //{                       
                         Response =Response+"else { document.write(\"<a href='http://monsoonads.com' target='_blank'><img width='300' height='250' src='http://54.84.151.220/demo/da/tbnr/Emi.jpg'  alt='Click Here!' title='Click Here!'  border='0' ></a>\");}";
                       //}
                       Response =Response+"</script>";
                      }
                        out.print(Response); 
                       /*===============================================*/ 
                        
                        
                    }  
                    catch (Exception e) 
                    {
                        e.printStackTrace();
                        System.out.println("IP1:"+ipAdd+" siteId:"+siteId +" Exeception occured : "+e);
                        
                     }
                   
                
            }
            out.close();
            
            siteId = null;//--like eros,indyarocks
            ipAdd = null;
           
            sURL = null;
            refURL = null;
            
            reqUrl = null;
            siteId = null;
            error = null;

        } 
        catch (Exception e1)
        {
            e1.printStackTrace();
            System.out.println("Error Returning response");
        }
    }

    private String[][] campInfo(String siteId, String chnlId, GeoInfo ipc, Device device1, boolean mobRequest,
			String userAgent, String fingerprintId) 
    {
    	String [][] allcampinfo = (String[][]) sc.getAttribute("campinfo"); 
    	int allcamplength=allcampinfo.length; 
    	System.out.println("Inside campInfo and CampList Len: "+ allcamplength);
    	GetTargetingList gtl = new GetTargetingList();
    	String [][] finalList = gtl.getList(allcampinfo, siteId, chnlId, ipc, device1, mobRequest, userAgent, fingerprintId);
		return finalList;
	}

	private String serviceURL(String reqURL) 
    {
        String serviceUrl = "";
        int end = 0;
        end = reqURL.indexOf("Controller");       
        serviceUrl = reqURL.substring(0, end);        
        //System.out.println(" serviceURL  "+serviceUrl);
        return serviceUrl;
    }
    
    private String[][] campInfo(String siteID,String chnlID,String country, String state, String city, boolean MR, String userAgent, String fingerprintId)
    {
              
      	String [][] allcampinfo = (String[][]) sc.getAttribute("campinfo"); 
    	int allcamplength=allcampinfo.length; 
    	System.out.println("CampList Len: "+ allcamplength);
    	
        String [][] mycampinfo=new String[allcamplength][21];
        String [][] devicecampinfo=new String[allcamplength][21];
        String deviceInformation = null;
        int os=0;
        int browser=0;
        int os_version=0;
        int tablet = 0;
        int target = 0;
        userAgent = userAgent.toLowerCase();
        
        String os1 = "";
        String browser1 = "";

       
        //=================OS=======================
         if (userAgent.toLowerCase().contains("windows"))
         {
             os1 = "Windows";
         } else if(userAgent.toLowerCase().contains("mac"))
         {
             os1 = "Mac";
         } else if(userAgent.toLowerCase().contains("x11"))
         {
             os1 = "Unix";
         } else if(userAgent.toLowerCase().contains("android"))
         {
             os1 = "Android";
         } else if(userAgent.toLowerCase().contains("iphone"))
         {
             os1 = "IPhone";
         }else{
             os1 = "UnKnown, More-Info: "+userAgent;
         }
         //===============Browser===========================
        if (userAgent.toLowerCase().contains("msie"))
        {
            browser1="IE";
        } 
        else if (userAgent.toLowerCase().contains("safari"))
        {
            browser1="safari";
        } else if ( userAgent.toLowerCase().contains("opr") || userAgent.toLowerCase().contains("opera"))
        {
            
             browser1="opera";
        } else if (userAgent.toLowerCase().contains("chrome"))
        {
            browser1="chrome";
        } else if ((userAgent.toLowerCase().contains("mozilla")) || userAgent.toLowerCase().contains("netscape"))
        {
            //browser=(userAgent.substring(userAgent.indexOf("MSIE")).split(" ")[0]).replace("/", "-");
            browser1 = "Netscape";

        } else if (userAgent.toLowerCase().contains("firefox"))
        {
            browser1="firefox";
        } else if(userAgent.toLowerCase().contains("rv"))
        {
            browser1="IE";
        } else
        {
            browser1 = "UnKnown, More-Info: "+userAgent;
        }
        
        
        
        System.out.println("Operating system:"+os1 + "\n");
       
        System.out.println("Browser:"+browser1 + "\n");
      
        
        int mcicnt=0;
        for (int y = 0; y < allcamplength; y++)
        {   
        //System.out.println("userAgent:"+ userAgent);
        	if (allcampinfo[y][4].equals(chnlID) && allcampinfo[y][0].equals(siteID))
			{
        	try
        	{
        	if(userAgent!=null && userAgent!="")
        	   {
        		if (os1.toLowerCase().contains(allcampinfo[y][15].toLowerCase()))
            		      os=1;
            	if (browser1.toLowerCase().contains(allcampinfo[y][16].toLowerCase()))
              		    browser=1;
                if (os1.toLowerCase().contains(allcampinfo[y][17].toLowerCase()))
                		 os_version=1;
                System.out.println("os:"+os);
               	if ( Integer.parseInt(allcampinfo[y][18]) == 1) 
               	{
                 if(os==1)
                 {
                		    	
                		    	 devicecampinfo[mcicnt][0] = allcampinfo[y][0];
                                 devicecampinfo[mcicnt][1] = allcampinfo[y][1];                 
                                 devicecampinfo[mcicnt][2] = allcampinfo[y][2];                   
                                 devicecampinfo[mcicnt][3] = allcampinfo[y][3];                   
                                 devicecampinfo[mcicnt][4] = allcampinfo[y][4];
                                 devicecampinfo[mcicnt][5] = allcampinfo[y][5]; 
                                 devicecampinfo[mcicnt][6] = allcampinfo[y][6];
                                 devicecampinfo[mcicnt][7] = allcampinfo[y][7]; 
                                 devicecampinfo[mcicnt][8] = allcampinfo[y][8]; 
                                 devicecampinfo[mcicnt][9] = allcampinfo[y][9]; 
                                 devicecampinfo[mcicnt][10] = allcampinfo[y][10];
                                 devicecampinfo[mcicnt][11] = allcampinfo[y][11];
                		         mcicnt++;
                                 System.out.println("CampInfo:"+ devicecampinfo[mcicnt][0]+devicecampinfo[mcicnt][1]+devicecampinfo[mcicnt][2]+devicecampinfo[mcicnt][3]); 
                		          target =1;
                		                         		  
                	}
                   }
               	 if ( Integer.parseInt(allcampinfo[y][19]) == 1)
               	  {
                   if(browser==1)  
                   {
                              devicecampinfo[mcicnt][0] = allcampinfo[y][0];
                              devicecampinfo[mcicnt][1] = allcampinfo[y][1];                 
                              devicecampinfo[mcicnt][2] = allcampinfo[y][2];                   
                              devicecampinfo[mcicnt][3] = allcampinfo[y][3];                   
                              devicecampinfo[mcicnt][4] = allcampinfo[y][4];
                              devicecampinfo[mcicnt][5] = allcampinfo[y][5]; 
                              devicecampinfo[mcicnt][6] = allcampinfo[y][6];
                              devicecampinfo[mcicnt][7] = allcampinfo[y][7]; 
                              devicecampinfo[mcicnt][8] = allcampinfo[y][8]; 
                              devicecampinfo[mcicnt][9] = allcampinfo[y][9]; 
                              devicecampinfo[mcicnt][10] = allcampinfo[y][10];
                              devicecampinfo[mcicnt][11] = allcampinfo[y][11];
                              mcicnt++;
                              target =1; 
                    }
                  }  
           	    }
        }
        catch(Exception e)
        {
        	System.out.println("Exception occured in os/browser target : " + e);
        }
        }
       
       }
       System.out.println("Device --: "+ mcicnt); 
       finalcamplen = mcicnt;
       
       mcicnt = 0;
       
       for (int x = 0; x < allcamplength; x++)
       {   
    	 try
    	 {
    	 if(allcampinfo[x][4].equals(chnlID) && allcampinfo[x][0].equals(siteID))
    	 {
         if (allcampinfo[x][3].equals("1")) //if it is geo targeted
           {
                   //System.out.println("GEO TARGETED - YES ");
                   StringTokenizer token;   
                   int count = 0;
                   if(MR) token = new StringTokenizer(allcampinfo[x][13],"||");
                   else token = new StringTokenizer(allcampinfo[x][12],"||");
                   count=token.countTokens();                   
                   String full_location[]=new String[count];
                   count=0;
                   while (token.hasMoreTokens())
                   {
                      full_location[count]=token.nextToken();
                      count++;
                   }
                   if(allcampinfo[x][7].equals("1")) //type of geo targetting - look match
                   {
                       //System.out.println(" Ad type IS "+allcampinfo[x][13]);
                       //System.out.println(" count ad start IS "+allcampinfo[x][14]);
                       boolean matched=false;
                        for (int ix = 0; ix < full_location.length; ix++) 
                        {
                           //System.out.println("TYPE OF GEO TARGETING - LOOK MATCH in "+full_location[ix]); 
                           StringTokenizer tokenagain;
                           int countagain = 0;
                           tokenagain = new StringTokenizer(full_location[ix],"=");                           
                           String location[]=new String[3];                          
                           while (tokenagain.hasMoreTokens())
                           {
                              location[countagain]=tokenagain.nextToken();
                              //System.out.println(" COUNTRY IS "+location[countagain]);
                              countagain++;
                           }                            
                          try
                          {
                           if(location[0]!=null && location[1]!=null && location[2]!=null)
                           {
                             //System.out.println(" Looking all match country,state and city"); 
                             if((location[0].equals(country)) && (location[1].equals(state)) && (location[2].equals(city)))
                             {
                                 matched=true;
                                 break;
                             }
                           }                           
                           else if(location[0]!=null && location[1]!=null)
                           {
                             //System.out.println(" Looking exact country and state match"); 
                             if((location[0].equals(country)) && (location[1].equals(state)))
                             {
                                 matched=true;
                                 break;
                             }
                           }
                           else
                           {
                             //System.out.println(" Looking exact country match"+location[0]);
                             if(location[0].equals(country)) 
                             {
                                 matched=true;
                                 break;
                             }
                           }
                           
                          }
                          catch(Exception es){  es.printStackTrace();}
                          
                        }
                        if(matched)
                        {
                            //System.out.println(" country list "+allcampinfo[x][4]);
                            //System.out.println(" SUCCESS..MATCH FOUND For "+ allcampinfo[x][1]);
                            mycampinfo[mcicnt][0] = allcampinfo[x][0];
                            mycampinfo[mcicnt][1] = allcampinfo[x][1];                 
                            mycampinfo[mcicnt][2] = allcampinfo[x][2];                   
                            mycampinfo[mcicnt][3] = allcampinfo[x][3];                   
                            mycampinfo[mcicnt][4] = allcampinfo[x][4];
                            mycampinfo[mcicnt][5] = allcampinfo[x][5]; 
                            mycampinfo[mcicnt][6] = allcampinfo[x][6];
                            mycampinfo[mcicnt][7] = allcampinfo[x][7]; 
                            mycampinfo[mcicnt][8] = allcampinfo[x][8]; 
                            mycampinfo[mcicnt][9] = allcampinfo[x][9]; 
                            mycampinfo[mcicnt][10] = allcampinfo[x][10];
                            mycampinfo[mcicnt][11] = allcampinfo[x][11];
                            
                            mycampinfo[mcicnt][12] = country;
                            mycampinfo[mcicnt][13] = state; 
                            mycampinfo[mcicnt][14] = city;
                            mcicnt++; 
                        } 
                        else continue;
                   } //end of if //type of geo targetting - look match
                  
                   else //type of geo targetting - look for not match
                   {
                       //System.out.println("TYPE OF GEO TARGETING - LOOK MATCH (NOT IN) ");  
                       boolean notmatched=true;
                        for (int ix = 0; ix < full_location.length; ix++) 
                        {
                           String location[]=full_location[ix].split("=");
                           if((location.length == 3) && (location[1].equals("") || location[1]== null) && (location[2].equals("") || location[2]==null))
                           {
                             if(location[0].equals(country)) 
                             {
                                 notmatched=false;
                                 break;
                             }
                           }
                           else if( (location.length == 3) && (location[2].equals("") || location[2]==null))
                           {
                             if((location[0].equals(country)) && (location[1].equals(state)))
                             {
                                 notmatched=false;
                                 break;
                             }
                           }
                           else
                           {
                             if((location.length == 3) && (location[0].equals(country)) && (location[1].equals(state)) && (location[2].equals(city)))
                             {
                                 notmatched=false;
                                 break;
                             }
                           }
                      
                          
                        }
                        
                        if(notmatched)
                        {
                            mycampinfo[mcicnt][0] = allcampinfo[x][0];
                            mycampinfo[mcicnt][1] = allcampinfo[x][1];                 
                            mycampinfo[mcicnt][2] = allcampinfo[x][2];                   
                            mycampinfo[mcicnt][3] = allcampinfo[x][3];                   
                            mycampinfo[mcicnt][4] = allcampinfo[x][4];
                            mycampinfo[mcicnt][5] = allcampinfo[x][5]; 
                            mycampinfo[mcicnt][6] = allcampinfo[x][6];
                            mycampinfo[mcicnt][7] = allcampinfo[x][7]; 
                            mycampinfo[mcicnt][8] = allcampinfo[x][8]; 
                            mycampinfo[mcicnt][9] = allcampinfo[x][9]; 
                            mycampinfo[mcicnt][10] = allcampinfo[x][10];
                            mycampinfo[mcicnt][11] = allcampinfo[x][11];
                            
                            mycampinfo[mcicnt][12] = country;
                            mycampinfo[mcicnt][13] = state; 
                            mycampinfo[mcicnt][14] = city;
                            //System.out.println(" in targeted with not match ");
                            mcicnt++; 
                        } 
                        else
                        {
                            continue;
                        }
                        
                    }//end of else //type of geo targetting - look for not match   
           		}                 
                else //if it is not geo targeted
                {
                    //System.out.println("GEO TARGETED - NO (ALL WORLD) ");
                            mycampinfo[mcicnt][0] = allcampinfo[x][0];
                            mycampinfo[mcicnt][1] = allcampinfo[x][1];                 
                            mycampinfo[mcicnt][2] = allcampinfo[x][2];                   
                            mycampinfo[mcicnt][3] = allcampinfo[x][3];                   
                            mycampinfo[mcicnt][4] = allcampinfo[x][4];
                            mycampinfo[mcicnt][5] = allcampinfo[x][5]; 
                            mycampinfo[mcicnt][6] = allcampinfo[x][6];
                            mycampinfo[mcicnt][7] = allcampinfo[x][7]; 
                            mycampinfo[mcicnt][8] = allcampinfo[x][8]; 
                            mycampinfo[mcicnt][9] = allcampinfo[x][9]; 
                            mycampinfo[mcicnt][10] = allcampinfo[x][10];
                            mycampinfo[mcicnt][11] = allcampinfo[x][11];
                            
                            mycampinfo[mcicnt][12] = country;
                            mycampinfo[mcicnt][13] = state; 
                            mycampinfo[mcicnt][14] = city;

                    
                     //System.out.println(" in not targeted  ");
                    mcicnt++; 
                } //end of else
           }
    	 else
    	   {
    		 continue;
    	   }
    	 }
         
         catch(Exception e)
         {
         	System.out.println("Exception occured in geo target : " + e);
         	StackTraceElement[] stackTrace = e.getStackTrace();
         	System.out.println(stackTrace[stackTrace.length - 1].getLineNumber());	
         }
       
       } // end for
       
       System.out.println("Geo -- :" + mcicnt);
       finalcamplen = mcicnt;
       return mycampinfo;    
    }
   
    private boolean loadSites(String siteID) {
        boolean res = false;
        String[] siteList = (String[]) sc.getAttribute("siteList");
        for (int x = 0; x < siteList.length; x++) {
            if (siteList[x].equals(siteID)) {
                res = true;
            }
        }
        siteList = null;
        return res;
    }    
    
 private String allcampinfo[][]=null;
 private int allcamplength=0;
 private String mycampinfo[][]=null;
 private String devicecampinfo[][]=null;
 
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
