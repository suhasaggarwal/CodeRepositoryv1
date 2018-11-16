package com.cuberoot.SBanner;


/** USE OF THIS 
 * 1. GENERATE AD SCRIPT AND RETURN TO CALLING CLASS 
 * 
 *
 * @author RAJ
 */

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;
public class AdGeneratorSpidio 
{
    private String ct;
    private String st;
    private String cnt;
    private String clntid;
    private String contId;
    private String refURL,OS,browserInfo,deviceInfo;
    private String ipadd;  
    private String cookieId;
    public AdGeneratorSpidio() {   }
    public String generateAdResponse(List lst,int errflg,String serivceURL,String pubid,String content,String refurl,String os,String browserinfo,String deviceinfo,String ip,String city,String state,String country,boolean flashMode,String pub_click_url, String fingerprintId)
    {
        String Response="";
        
        Iterator it;
        UUID x=null;
        ct=city;
        st=state;
        cnt=country;
        clntid=pubid;
        contId=content;
        refURL=refurl;
        OS=os;
        browserInfo=browserinfo;
        deviceInfo=deviceinfo;
        ipadd=ip;
        cookieId = fingerprintId;
        //System.out.println("in AdGenerator");
        String uuid = x.randomUUID().toString();
        Collections.shuffle(lst);
        it = lst.iterator();
        int newadid=0;        
        int adserved=0;
        DBUtility db = new DBUtility();
        
        try 
        {
            StoredAd ad;
            Adserved adS = new Adserved(lst,clntid,contId,refURL,OS,browserInfo,deviceInfo,uuid,ipadd,ct,st,cnt,cookieId);
            adS.sent();
            long CHACHEBUSTER = Calendar.getInstance(Locale.US).getTimeInMillis();
            
             
             if(errflg==0 && it.hasNext() )
             {
               while(it.hasNext() && (adserved==0)) 
               {
                  ad = (StoredAd)it.next();
                  String sURL=serivceURL;
                  newadid++; 
                  String adType=ad.getAdType();
                  //System.out.println("adType: "+adType); //+" & campaign type: "+ad.getCampType());
                  //adType=3;
                      int cmpid=ad.getCmpId();
                     
                      EhCacheKeyCodeRepository ehcache = EhCacheKeyCodeRepository.getInstance();
                      String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                      List<String> results  = new ArrayList<String>();
                      List<String> results1 = ehcache.get(fingerprintId,false);
                      String value = fingerprintId+"@"+timestamp+"@"+cmpid;
                      if(results1!=null){
                      results1.add(value);
                      ehcache.put(fingerprintId,results1);
                      }
                      else{
                    	  results.add(value);
                    	  ehcache.put(fingerprintId, results);
                      }
                      int chnlid=ad.getchnlId();
                      int advID=ad.getAdvId();
                 //     int crID=db.getCreativeId(ad.getAdURL1(),ad.getCmpId());
                      String crID = ad.getCrId();
                      String clientimprURL=ad.getImprURL();
                      //String serviceurl="http://localhost:8084/SBanner/AdTracking?";
                      String imptr=sURL+"AdTracking?tr=5&amp;uuid="+uuid+"&amp;adId=1&amp;cmpId="+cmpid+"&amp;crId="+crID+"&amp;advId="+advID+"&amp;pubId="+clntid+"&amp;chnlId="+chnlid+"&amp;cookieId="+fingerprintId;
              //        System.out.println(imptr);
                      
                  //    String responsetr = sURL+"ResponseTracking?tr=101&amp;uuid="+uuid+"&amp;adId=1&amp;cmpId="+cmpid+"&amp;crId="+crID+"&amp;advId="+advID+"&amp;pubId="+clntid+"&amp;chnlId="+chnlid+"&amp;sURL="+sURL+"&amp;width="+ad.getCampWidth1()+"&amp;height="+ad.getCampHeight1()+"&amp;refurl="+refurl+"&amp;os="+os+"&amp;browserinfo="+browserinfo+"&amp;cookieId="+fingerprintId;
                      
                      String responsetr = sURL+"ResponseTracking?tr=101&amp;uuid="+uuid+"&amp;adId=1&amp;cmpId="+cmpid+"&amp;crId="+crID+"&amp;advId="+advID+"&amp;pubId="+clntid+"&amp;chnlId="+chnlid+"&amp;sURL="+sURL+"&amp;width=300&amp;height=250&amp;refurl="+refurl+"&amp;os="+os+"&amp;browserinfo="+browserinfo+"&amp;cookieId="+fingerprintId;
                      
                      if(pub_click_url != null)
                      pub_click_url = URLEncoder.encode(pub_click_url);
                
                      String clickurl=sURL+"AdTracking?tr=99&amp;uuid="+uuid+"&amp;adId=1&amp;cmpId="+cmpid+"&amp;crId="+crID+"&amp;advId="+advID+"&amp;pubId="+clntid+"&amp;chnlId="+chnlid+"&amp;pubClickUrl="+pub_click_url+"&amp;CTU="+ad.getClkThrURL()+"&amp;cookieId="+fingerprintId;
               //       System.out.println(clickurl);
                      
                      adserved=1;
                      
                    //  String iframeurl = "https://dcpub.cuberoot.co/dcode2/passbackscript.php?parameters="+cmpid+","+crID+","+chnl&nid=1&sid=1&pn=a&cn=b
                      
                      
                      
                      if(adType.equals("3"))  // only images----
                      {  
                    	  
                    	  //document.body.appendChild(oImg);
                       Response = Response+"<script language='JavaScript' src="+"'https://dcpub.cuberoot.co/dcode1/get.js'></script>";
                       Response = Response+"<script language='JavaScript' src="+"'https://dcpub.cuberoot.co/dcode1/replacecontent.js'></script>";
                       Response = Response+"<script language='JavaScript' src="+"'https://dcpub.cuberoot.co/dcode1/GenerateResponseUrl.js'></script>";
                       Response = Response + "<script>document.write(\"<div id='storyad' style='display:block;'>\");";  
                       Response = Response+"document.write(\"<a href='"+clickurl+"' target='_blank'><img id='cuberootad' width='"+ad.getCampWidth1()+"' height='"+ad.getCampHeight1()+"' src='"+ad.getAdURL1()+"'  alt='Click Here!' title='Click Here!'  border='0' ></a>\");";
                       Response=Response+"document.write(\"<img id='cuberootTracker' src='"+imptr+"' border='0' width='1' height='1'>\");";
                       if(!clientimprURL.equals("")|| clientimprURL!=null) { Response= Response+"document.write(\"<img id='cuberootclienturl' src='"+clientimprURL+"' border='0' width='1' height='1'>\");"; }
                    //   Response = Response +"document.write(\"<input type='text' id='code' name='code' /><input id='Button1' type='button' value='submit' onclick=\"httpGet('"+responsetr+"&amp;rv="+"document.getElementById("+"'code'"+").value');\"/>"+"\");";
                      
                    //   Response = Response +"document.write(\"<input type='text' id='code' name='code' />\");"+"document.write(\"<input id='Button1' type='button' value='submit' onclick=httpGet('"+responsetr+"&amp;rv=document.getElementById('code').value');/>"+"\");";
                       
                      // Response = Response +"document.write(\"<input type='text' id='code' name='code' >\");"+"document.write(\"<input id='Button1' type='button' value='submit' onclick=appendHtml('storyad',httpGet('"+responsetr+"&amp;rv=this.value'));>"+"\");";
                       
                       Response = Response +"document.write(\"<input type='text' id='code' name='code' >\");"+"document.write(\"<input id='Button1' type='button' value='submit' onclick=generateResponseData('storyad','"+responsetr+"'"+");>"+"\");";
                       
                       Response = Response+"document.write(\"</div>\");";
                       Response =Response+"</script>";
                      
                       
                      }
                      else if(adType.equals("99"))  //External tag;
                      {  
                    	 
                    	  Response ="<script language="+"'JavaScript'>"+
                            	  "var cmpId="+cmpid+";"+"var crId="+crID+";"+"</script>"; 
                    	  
                    	  Response = "<script>";
                          if ((!clientimprURL.equals("")) || (clientimprURL != null)) {
                            Response = Response + "document.write(\"<img src='" + clientimprURL + "' border='0' width='1' height='1' style='position:absolute;'>\");";
                          }
                          String newadurl = ad.getAdURL1().replace("[INSERT_CLICK_TRACKER_MACRO]", "" + clickurl);
                          
                          Response = Response + "</script><div style=\"position:absolute;\">" + newadurl + "</div>";
                          if (((!clientimprURL.equals("")) || (clientimprURL != null))) {
                            Response = Response + "" + clientimprURL + "";
                          }
                          Response = Response + "<img src='" + imptr + "' border='0' width='1' height='1' style='position:absolute;'>";
                          Response = Response+"<script src="+"https://dcpub.cuberoot.co/dcode1/basedc.js></script>";   
                      }
                      else
                      {
                        Response="";
                      }    
                   } 
               
                        
              
             
            
            }
                
            ad=null;
             
        } 
        catch (Exception e1) 
        {   
          //  e1.printStackTrace();  
            Response="";
            
        } 
        finally
        {
         if (db != null) {
                db = null;
            }
         //Response="";
        }
      
       return Response;   
    }
    private String getError(int err)
    {
        String errorString="";
        switch(err)
        {
            case 1:
                errorString="No ads available this time";
        }
       return errorString; 
    }
 
    
}
