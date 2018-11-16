package util;

/**
 * 
 */
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.CacheManager;

import com.google.inject.Singleton;

import util.GlobalConfiguration;
import util.KeyCodeRepository;

/**
 * @author Sun Ning/SNDA
 * @since Sep 14, 2009
 * 
 */

public class EhCacheKeyCodeRepository implements KeyCodeRepository {

	private Cache cache1;
	private Cache cache2;

    /**
     * 
     */
	public static EhCacheKeyCodeRepository ehcache;
	
	public static EhCacheKeyCodeRepository getInstance(){
		
		if(ehcache == null)
			return new EhCacheKeyCodeRepository();
		
		
		else
			return ehcache;
		
	}
	
	
	
	public EhCacheKeyCodeRepository() {
		init();
	}

    /**
     * initialize properties.
     */
	public void init() {
        CacheManager cm = CacheManager.getInstance();
		cache1 = cm.getCache("cache1");
		cache2 = cm.getCache("cache2");
	
	
	
	
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sdo.captchaserver.repos.KeyCodeRepository#get(java.lang.String)
	 */
	@Override
	public CSVResult get(String key) {
		
		String date = java.time.LocalDate.now().toString();
		if (key.contains(date) && !key.contains("count(*),count(DISTINCT(sessionhash)),cookiehash") && !key.contains("count(*),max(request_time),sessionhash") && !key.contains("count(*),max(request_time),min(request_time),sessionhash") && !key.contains("count(*),count(DISTINCT(refcurrentoriginal)),sessionhash")) {
			Element result = cache2.get(key);
			if (result != null) {
				cache2.remove(key);
				return (CSVResult) result.getValue();
			} else {
				return null;
			}
		} else {
			Element result = cache1.get(key);
			if (result != null) {
				cache1.remove(key);
				return (CSVResult) result.getValue();
			} else {
				return null;
			}

		}
}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sdo.captchaserver.repos.KeyCodeRepository#put(java.lang.String,
	 * com.sdo.captchaserver.repos.KeyCodeModel)
	 */
	@Override
	public void put(String key, CSVResult kcm) {
		
		String date = java.time.LocalDate.now().toString();
		if (key.contains(date)&& !key.contains("count(*),count(DISTINCT(sessionhash)),cookiehash") && !key.contains("count(*),max(request_time),sessionhash") && !key.contains("count(*),max(request_time),min(request_time),sessionhash") && !key.contains("count(*),count(DISTINCT(refcurrentoriginal)),sessionhash")) {
		Element element = new Element(key, kcm);
		System.out.println("Put in Cache");
		cache2.put(element);
		}
		else{
			Element element = new Element(key, kcm);
			System.out.println("Put in Cache");
			cache1.put(element);	
			
			
		}
		
		}

    /**
     * get data from map.
     * @param key
     * @param removeOnGet remove key if set to ture
     * @return
     */
	@Override
	public CSVResult get(String key, boolean removeOnGet) {
		
		String date = java.time.LocalDate.now().toString();
		if (key.contains(date) && !key.contains("count(*),count(DISTINCT(sessionhash)),cookiehash") && !key.contains("count(*),max(request_time),sessionhash") && !key.contains("count(*),max(request_time),min(request_time),sessionhash") && !key.contains("count(*),count(DISTINCT(refcurrentoriginal)),sessionhash")) {
		
		Element result =  cache2.get(key);
		System.out.println("Get from Cache");
		
		if(result != null){
			if(removeOnGet){
				cache2.remove(key);
			}
			return (CSVResult)result.getValue();
		}else{
			return null;
		}
		}
		else{
			
			Element result =  cache1.get(key);
			System.out.println("Get from Cache");
			
			if(result != null){
				if(removeOnGet){
					cache1.remove(key);
				}
				return (CSVResult)result.getValue();
			}else{
				return null;
			}	
			
			
			
		}
		
		
		
		
		
		}

}
