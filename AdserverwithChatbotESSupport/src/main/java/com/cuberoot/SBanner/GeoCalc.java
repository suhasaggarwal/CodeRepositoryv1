package com.cuberoot.SBanner;
import java.util.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class GeoCalc implements Target{

	public String[][] calcList(String[][] allcampinfo, String siteID, String chnlID, GeoInfo ipc, boolean MR) {
		//final Logger logger = LoggerFactory.getLogger(GeoCalc.class);
		int allcamplength = allcampinfo.length;
		String [][] mycampinfo = new String[allcamplength][35];
		
		
		String zip="";
        String country="";
        String city="";
        String state ="";
		
		zip=ipc.getZip();
        country=ipc.getCountry();
        city=ipc.getCity();
        state =ipc.getRegion();
        System.out.println("Inside GeoCalc.!!");
        System.out.println("Country : "+country+" State : "+state+" City : "+city+" Zip : "+zip);
		
        
    //    zip="NA";
     //   country="IN";
    //    city="";
    //    state ="19";
        
        
        int mcicnt = 0;
        
		for (int x = 0; x < allcamplength; x++)
        {          
			
			if (allcampinfo[x][4].equals(chnlID) && allcampinfo[x][0].equals(siteID)) //Match for publisher.
			{
				try
				{
				System.out.println("Matched Campaign is as follows channel_id : "+chnlID +" SiteID : "+siteID +" Is GeoTarget : "+allcampinfo[x][3]);	
				
				if (allcampinfo[x][3].equals("1")) //if it is Geo targeted
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
				           System.out.println(" SUCCESS..MATCH FOUND For "+ allcampinfo[x][1]);
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
				           mycampinfo[mcicnt][15] = allcampinfo[x][15];
				           mycampinfo[mcicnt][16] = allcampinfo[x][16];
				           mycampinfo[mcicnt][17] = allcampinfo[x][17];
				           mycampinfo[mcicnt][18] = allcampinfo[x][18];
				           mycampinfo[mcicnt][19] = allcampinfo[x][19];
				           mycampinfo[mcicnt][20] = allcampinfo[x][20];
				           mycampinfo[mcicnt][21] = allcampinfo[x][21];
				           mycampinfo[mcicnt][22] = allcampinfo[x][22];
				           mycampinfo[mcicnt][23] = allcampinfo[x][23];
				           mycampinfo[mcicnt][24] = allcampinfo[x][24];
				           mycampinfo[mcicnt][25] = allcampinfo[x][25];
				           mycampinfo[mcicnt][26] = allcampinfo[x][26];
				           mycampinfo[mcicnt][27] = allcampinfo[x][27];
				           mycampinfo[mcicnt][28] = allcampinfo[x][28];
				           mycampinfo[mcicnt][29] = allcampinfo[x][29];
				           mycampinfo[mcicnt][30] = allcampinfo[x][30];
				           mycampinfo[mcicnt][31] = allcampinfo[x][31];
				           mycampinfo[mcicnt][32] = allcampinfo[x][32];
				           mycampinfo[mcicnt][33] = allcampinfo[x][33];
				           mycampinfo[mcicnt][34] = allcampinfo[x][34];
				           mycampinfo[mcicnt][35] = allcampinfo[x][35];
				           mycampinfo[mcicnt][36] = allcampinfo[x][36];
				           mycampinfo[mcicnt][37] = allcampinfo[x][37];
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
				          if( (location[1].equals("") || location[1]==null) && (location[2].equals("") || location[2]==null))
				          {
				            if(location[0].equals(country)) 
				            {
				                notmatched=false;
				                break;
				            }
				          }
				          else if(location[2].equals("") || location[2]==null)
				          {
				            if((location[0].equals(country)) && (location[1].equals(state)))
				            {
				                notmatched=false;
				                break;
				            }
				          }
				          else
				          {
				            if((location[0].equals(country)) && (location[1].equals(state)) && (location[2].equals(city)))
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
				           System.out.println(" in targeted with not match ");
				           
				           mycampinfo[mcicnt][15] = allcampinfo[x][15];
				           mycampinfo[mcicnt][16] = allcampinfo[x][16];
				           mycampinfo[mcicnt][17] = allcampinfo[x][17];
				           mycampinfo[mcicnt][18] = allcampinfo[x][18];
				           mycampinfo[mcicnt][19] = allcampinfo[x][19];
				           mycampinfo[mcicnt][20] = allcampinfo[x][20];
				           mycampinfo[mcicnt][21] = allcampinfo[x][21];
				           mycampinfo[mcicnt][22] = allcampinfo[x][22];
				           mycampinfo[mcicnt][23] = allcampinfo[x][23];
				           mycampinfo[mcicnt][24] = allcampinfo[x][24];
				           mycampinfo[mcicnt][25] = allcampinfo[x][25];
				           mycampinfo[mcicnt][26] = allcampinfo[x][26];
				           mycampinfo[mcicnt][27] = allcampinfo[x][27];
				           mycampinfo[mcicnt][28] = allcampinfo[x][28];
				           mycampinfo[mcicnt][29] = allcampinfo[x][29];
				           mycampinfo[mcicnt][30] = allcampinfo[x][30];
				           mycampinfo[mcicnt][31] = allcampinfo[x][31];
				           mycampinfo[mcicnt][32] = allcampinfo[x][32];
				           mycampinfo[mcicnt][33] = allcampinfo[x][33];
				           mycampinfo[mcicnt][34] = allcampinfo[x][34];
				           mycampinfo[mcicnt][35] = allcampinfo[x][35];
				           mycampinfo[mcicnt][36] = allcampinfo[x][36];
				           mycampinfo[mcicnt][37] = allcampinfo[x][37];
				           mcicnt++; 
				       } 
				       else
				       {
				           continue;
				       }
				       
				   }  //end of else //type of geo targetting - look for not match   
				             
				}
				
				else //if it is not geo targeted
				{
				           System.out.println("GEO TARGETED - NO (ALL WORLD) ");
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
				           
				           mycampinfo[mcicnt][15] = allcampinfo[x][15];
				           mycampinfo[mcicnt][16] = allcampinfo[x][16];
				           mycampinfo[mcicnt][17] = allcampinfo[x][17];
				           mycampinfo[mcicnt][18] = allcampinfo[x][18];
				           mycampinfo[mcicnt][19] = allcampinfo[x][19];
				           mycampinfo[mcicnt][20] = allcampinfo[x][20];
				           mycampinfo[mcicnt][21] = allcampinfo[x][21];
				           mycampinfo[mcicnt][22] = allcampinfo[x][22];
				           mycampinfo[mcicnt][23] = allcampinfo[x][23];
				           mycampinfo[mcicnt][24] = allcampinfo[x][24];
				           mycampinfo[mcicnt][25] = allcampinfo[x][25];
				           mycampinfo[mcicnt][26] = allcampinfo[x][26];
				           mycampinfo[mcicnt][27] = allcampinfo[x][27];
				           mycampinfo[mcicnt][28] = allcampinfo[x][28];
				           mycampinfo[mcicnt][29] = allcampinfo[x][29];
				           mycampinfo[mcicnt][30] = allcampinfo[x][30];
				           mycampinfo[mcicnt][31] = allcampinfo[x][31];
				           mycampinfo[mcicnt][32] = allcampinfo[x][32];
				           mycampinfo[mcicnt][33] = allcampinfo[x][33];
				           mycampinfo[mcicnt][34] = allcampinfo[x][34];
				           mycampinfo[mcicnt][35] = allcampinfo[x][35];
				           mycampinfo[mcicnt][36] = allcampinfo[x][36];
				           mycampinfo[mcicnt][37] = allcampinfo[x][37];
				           
				           //System.out.println(" in not targeted  ");
				   mcicnt++; 
				} //end of else
				}
				catch(Exception e)
				{
					
					System.out.println("Exception occured in geo target : " + e);
		         	StackTraceElement[] stackTrace = e.getStackTrace();
		         	System.out.println(stackTrace[stackTrace.length - 1].getLineNumber());
					
					//logger.debug("Exception occured in geo target : " + e);
				}
			    
	        }
			else
			{
				continue;
			}
        }
		System.out.println("Count of matching campaigns in Geo :"+mcicnt);
		return mycampinfo;
	}

	public String[][] calcList() {
		// TODO Auto-generated method stub
		return null;
	}
}
