package com.cuberoot.SBanner;

import net.sourceforge.wurfl.core.CapabilityNotDefinedException;
import net.sourceforge.wurfl.core.Device;

public class DeviceCalc implements Target{

	public String[][] calcList(String[][] allcampinfo, String siteID, String chnlID, Device device1, String userAgent) {
		
		int allcamplength=allcampinfo.length;
        String [][] mycampinfo=new String[allcamplength][35];
        int os= 0;
        int browser= 0;
        int screen = 0;
        //int tablet = 0;
        //String deviceInformation = null;
        
        System.out.println("Inside DeviceCalc Module.");
        
        String brandName = "";
        String deviceOs = "";
        String device_os_version = "";
        String release_date = "";
        String physical_screen_width = "";
        String physical_screen_height = "";
        String desktopcheck = "";
        String model_name = "";
        String marketing_name = "";
        String wireless_device = "";
        String resolution_width = "";
        String resolution_height = "";
        String physical_screen = "";
        
        
        int mcicnt=0;
        int mcicnt1 = 0;
        
        try {
           brandName = device1.getCapability("brand_name");
           model_name=device1.getCapability("model_name");
           marketing_name=device1.getCapability("marketing_name");
           deviceOs=device1.getCapability("device_os");
           device_os_version=device1.getCapability("device_os_version");
           release_date=device1.getCapability("release_date");
           physical_screen_width=device1.getCapability("physical_screen_width");
           physical_screen_height=device1.getCapability("physical_screen_height");
           wireless_device = device1.getCapability("is_wireless_device");
           resolution_width = device1.getCapability("resolution_width");
           resolution_height = device1.getCapability("resolution_height");
           physical_screen = physical_screen_width +"_"+ physical_screen_height;
           
           System.out.println("Brand Name:"+brandName);
           System.out.println("OS:"+deviceOs);
           System.out.println("Model:"+model_name);
           System.out.println("MarketingName:"+marketing_name);
           System.out.println("Version:"+device_os_version);
           System.out.println("Date:"+release_date);
           System.out.println("ScreenWidth:"+physical_screen_width);
           System.out.println("ScreenHeight:"+physical_screen_height);
           System.out.println("DesktopCheck:"+desktopcheck);
           System.out.println("Wireless_Device:"+wireless_device);
           System.out.println("ResolutionWidth:"+resolution_width);
           System.out.println("ResolutionHeight:"+resolution_height);
           System.out.println("PhysicalScreen:"+physical_screen);
         //Desktop and Laptop are not wireless device, can be used for specific mobile targeting 
          
           
        } catch (CapabilityNotDefinedException e) {
           throw new RuntimeException("Bug in WURFL:" + e.getLocalizedMessage(), e);
        }
        catch (Exception e) {
            throw new RuntimeException("Bug in WURFL:" + e.getLocalizedMessage(), e);
         }
        
        
        userAgent = userAgent.toLowerCase();
        
        String os1 = deviceOs;
        String browser1 = "";

        
        
      try
      {
        //=================OS=======================
        if (os1 == null || os1 == "")
        {
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
      }
      catch(Exception e){
    	  System.out.println("Exception in getting browsername and os: "+e);
      }
        
        
        System.out.println("Operating system:"+os1 + "\n");
       
        System.out.println("Browser:"+browser1 + "\n");
      
        if(allcamplength <= 0)
        {
        	System.out.println("No camps to process.!!");
        }
        else
        {
        	int validcamp = 0;
	        mcicnt=0;
	        mcicnt1 = 0;
	        
	       for (int x = 0; x < allcamplength; x++)
	        {   
	        //System.out.println("userAgent:"+ userAgent);
	        validcamp = 0;
	        os= 0;
	        browser= 0;
	        screen = 0;
	        	
	        try
	        {
	        if ((allcampinfo[x][4] != null) && (allcampinfo[x][0] != null)) //Match for publisher.
			  {	
	        
	        	if (allcampinfo[x][4].equals(chnlID) && allcampinfo[x][0].equals(siteID)) 
	        	{
	        	if(allcampinfo[x][17] != null && allcampinfo[x][17] != "" && allcampinfo[x][17].equals("1"))
	          {
	          if(userAgent!=null && userAgent!="")
	        	{
	        	try
	        	{
	            if ((allcampinfo[x][15] != null) && allcampinfo[x][15].toLowerCase().contains(os1.toLowerCase()))
	            	{
	                 os=1;
	                 System.out.println("OS Matched.");
	            	}
	            if ((allcampinfo[x][16] != null) && allcampinfo[x][16].toLowerCase().contains(browser1.toLowerCase()))
	            	{
	                 browser=1;
	                 System.out.println("Browser Matched.");
	            	}
				if ((allcampinfo[x][21] != null) && (allcampinfo[x][21].contains(physical_screen)))
					{
	            	screen = 1;
	            	System.out.println("Screen Matched.");
					}
	        	}
	        	
	        	catch(Exception e)
	        	{
	        		System.out.println("Error in comaparing os and browser. Os: "+allcampinfo[x][15]);
	        	}
	            System.out.println("os:"+os);
	            
	            
	            if (allcampinfo[x][18] != null && allcampinfo[x][18] != "" && Integer.parseInt(allcampinfo[x][18]) == 1) 
	            {
		            if(os==1)
		            	{
		            	validcamp = 1;
		                }
		            else continue;
	            }
	            
	            else
	            {
	            	
	            	validcamp = 1;
	            }
	                    
	            if (allcampinfo[x][19] != null && allcampinfo[x][19] != "" && Integer.parseInt(allcampinfo[x][19]) == 1)
	            	{
	            	if(browser==1 && validcamp == 1)  
	            		{
	            			//pass
	                    }
	            	else
	            		continue;
	              }
	            else{
	            	if(validcamp == 1)
	            	{
	            		//pass
	            	}
	            	else
	            		continue;
	            	}
	            
	            if (allcampinfo[x][20] != null && allcampinfo[x][20] != "" && Integer.parseInt(allcampinfo[x][20]) == 1)
            		 {
	            	if(screen == 1 && validcamp == 1)  
            			{
            			//pass
            			}
	            	else
	            		continue;
            		 }
	            else
	              {
            	  if(validcamp == 1)
            	   {
            		//pass
            	   }
            	  else
            		continue;
	              }
	            
	            }
	        else
	        	{
	        	continue;
	        	}
	          }
	    
	         else
	       		{
	        	 validcamp = 1;  
	       		}
			  
	       
	        
	        if(validcamp == 1)
        	{
        	
        	mycampinfo[mcicnt1][0] = allcampinfo[x][0];
            mycampinfo[mcicnt1][1] = allcampinfo[x][1];                 
            mycampinfo[mcicnt1][2] = allcampinfo[x][2];                   
            mycampinfo[mcicnt1][3] = allcampinfo[x][3];                   
            mycampinfo[mcicnt1][4] = allcampinfo[x][4];
            mycampinfo[mcicnt1][5] = allcampinfo[x][5]; 
            mycampinfo[mcicnt1][6] = allcampinfo[x][6];
            mycampinfo[mcicnt1][7] = allcampinfo[x][7]; 
            mycampinfo[mcicnt1][8] = allcampinfo[x][8]; 
            mycampinfo[mcicnt1][9] = allcampinfo[x][9]; 
            mycampinfo[mcicnt1][10] = allcampinfo[x][10];
            mycampinfo[mcicnt1][11] = allcampinfo[x][11];
            
            mycampinfo[mcicnt1][12] = allcampinfo[x][12];
            mycampinfo[mcicnt1][13] = allcampinfo[x][13];
            mycampinfo[mcicnt1][14] = allcampinfo[x][14];
            mycampinfo[mcicnt1][15] = allcampinfo[x][15];
            mycampinfo[mcicnt1][16] = allcampinfo[x][16];
            mycampinfo[mcicnt1][18] = allcampinfo[x][18];
            mycampinfo[mcicnt1][19] = allcampinfo[x][19];
            mycampinfo[mcicnt1][20] = allcampinfo[x][20];
	        mycampinfo[mcicnt1][21] = allcampinfo[x][21];
	        mycampinfo[mcicnt1][22] = allcampinfo[x][22];
	        mycampinfo[mcicnt1][23] = allcampinfo[x][23];
	        mycampinfo[mcicnt1][24] = allcampinfo[x][24];
	        mycampinfo[mcicnt1][25] = allcampinfo[x][25];
	        mycampinfo[mcicnt1][26] = allcampinfo[x][26];
	        mycampinfo[mcicnt1][27] = allcampinfo[x][27];
	        mycampinfo[mcicnt1][28] = allcampinfo[x][28];
	        mycampinfo[mcicnt1][29] = allcampinfo[x][29];
	        mycampinfo[mcicnt1][30] = allcampinfo[x][30];
	        mycampinfo[mcicnt1][31] = allcampinfo[x][31];
	        mycampinfo[mcicnt1][32] = allcampinfo[x][32];
	        mycampinfo[mcicnt1][33] = allcampinfo[x][33];
	        mycampinfo[mcicnt1][34] = allcampinfo[x][34];
	        mycampinfo[mcicnt1][35] = allcampinfo[x][35];
	        
            mcicnt1++;
	
        	}
	       
			 }
	  	        	
	        }
	        
	        }
	        catch(Exception e)
	        {
	        	System.out.println("Exception occured in device target module: " + e);
	         	StackTraceElement[] stackTrace = e.getStackTrace();
	         	System.out.println(stackTrace[stackTrace.length - 1].getLineNumber());
	        } 
	       
	        }
	        System.out.println("mcicnt and mcicnt1 are : " + mcicnt +" and "+mcicnt1);
        }

return mycampinfo;
}
	
	public String[][] calcList() 
	{
		// TODO Auto-generated method stub
		return null;
	}

}