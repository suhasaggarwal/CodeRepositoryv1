package com.cuberoot.SBanner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ElasticUtil {

	public static void registerResponseTrackings(String uuid,int adid,int pubid,int chnlid,int cmpid, int advid, int crid,String ipaddress, int tracking, String responseValue,String width,String height,String os,String refurl,String browser, String cookieId) throws SQLException 
    {
		Map<String, Object> result = new HashMap<String,Object>();
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		result.put("cookie_id", uuid);
		result.put("AdId",adid);
		result.put("pubid", pubid);
		result.put("chnlid",chnlid);
		result.put("cmpid",cmpid);
		result.put("advid",advid);
		result.put("crid",crid);
		result.put("ipaddress",ipaddress);
		result.put("tracking",tracking);
		result.put("responseValue",responseValue);
		result.put("width", width);
		result.put("height",height);
		result.put("os", os);
		result.put("refurl",refurl);
		result.put("browser",browser);
		result.put("cookieId",cookieId);
		result.put("request_time",timeStamp);
		IndexCategoriesData.doIndex(SegmentCalc.getEsClient(), "adscognitionresponsetrack",
				"core2", result);
		
		
		
    }		
		
	
	public static void registerTrackings(String uuid,int adid,int pubid,int chnlid,int cmpid, int advid, int crid,String ipaddress, int tr,String cookie_id) throws SQLException 
    {
		Map<String, Object> result = new HashMap<String,Object>();
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		result.put("cookie_id", uuid);
		result.put("AdId",adid);
		result.put("pubid", pubid);
		result.put("chnlid",chnlid);
		result.put("cmpid",cmpid);
		result.put("advid",advid);
		result.put("crid",crid);
		result.put("ipaddress",ipaddress);
		result.put("tracking",tr);
		result.put("cookieId",cookie_id);
		result.put("request_time",timeStamp);

		IndexCategoriesData.doIndex(SegmentCalc.getEsClient(), "adscognitiontrack",
				"core2", result);
		
		
		
    }		
	
	public static void registerAdserved(String publisher_id,String channel_id,String advertiser_id,String campaign_id,String creative_id,String city,String state,String country,String ref_url,String uuid,String adid,String adurl,String clickurl,String callbackurl,String ip,String os,String browser_info,String device_info, String fingerprintId) throws SQLException 
    {
		Map<String, Object> result = new HashMap<String,Object>();
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		result.put("publisher_id", publisher_id);
		result.put("channel_id",channel_id);
		result.put("advertiser_id", advertiser_id);
		result.put("campaign_id",campaign_id);
		result.put("creative_id",creative_id);
		result.put("city",city);
		result.put("state",state);
		result.put("country",country);
		result.put("refurl",ref_url);
		result.put("uuid",uuid);
		result.put("adid",adid);
		result.put("adurl",adurl);
		result.put("clickurl",clickurl);
		result.put("callbackurl",callbackurl);
		result.put("ip",ip);
		result.put("os",os);
		result.put("browser_info",browser_info);
		result.put("device_info",device_info);
		result.put("cookieId",fingerprintId);
		result.put("request_time",timeStamp);

		IndexCategoriesData.doIndex(SegmentCalc.getEsClient(), "adscognitionserved",
				"core2", result);
		
		
		
    }		
	
	
}
