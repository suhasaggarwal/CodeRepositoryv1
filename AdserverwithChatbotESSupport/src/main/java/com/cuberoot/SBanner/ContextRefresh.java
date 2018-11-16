package com.cuberoot.SBanner;
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/** USE OF THIS 
 * 1. REFRESHING THE VALUES IN THE CONTEXT WHEN INFORMATION HAS MODIFIED
 *
 * @author 
 */
import java.io.*;
import java.sql.*;
import java.util.*;

import javax.servlet.*;
import javax.servlet.http.*;

import com.cuberoot.SBanner.*;

public class ContextRefresh extends HttpServlet {

    public void init(ServletConfig sconf)
    {
         sc = sconf.getServletContext();
         
    }
    
     ServletContext sc;   
     //public static MemcachedKeyCodeRepository cacheObject = MemcachedKeyCodeRepository.getInstance();
     
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        
        System.out.print(" Calling the reload context method.."); 
        
        
        
        System.out.println("The ApidioAD Servering Platform is started and intialize At: "+ new java.util.Date());
        
     
        String[] siteList;
     
    
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
            st9=  con.createStatement();
            st10= con.createStatement();
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
            
        }
        catch (Exception e) 
        {
            e.printStackTrace();
            System.out.println("Error in Getting Site List/mapping List");
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
            
            siteList = null;
        }
    

    }

    
    //private String allcampinfo[][]=null;
    //private String mycampinfo[][]=null;// <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /** 
     * Handles the HTTP <code>GET</code> method.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /** 
     * Handles the HTTP <code>POST</code> method.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /** 
     * Returns a short description of the servlet.
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>
}
