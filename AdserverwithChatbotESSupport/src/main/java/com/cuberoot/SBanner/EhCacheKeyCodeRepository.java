package com.cuberoot.SBanner;

/**
 * 
 */

import java.util.List;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.CacheManager;




public class EhCacheKeyCodeRepository implements KeyCodeRepository {

	private Cache cache;

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
		cache = cm.getCache("keyCodeRepository");
		cache.getCacheConfiguration().setTimeToLiveSeconds(60 * GlobalConfiguration
				.getInt("yan.expireminutes"));
		cache.getCacheConfiguration().setTimeToIdleSeconds(60 * GlobalConfiguration
				.getInt("yan.expireminutes"));
	}

	
	public List<String> get(String key) {
		Element result =  cache.get(key);
		if(result != null){
			cache.remove(key);
			return (List<String>) result.getValue();
		}else{
			return null;
		}
	}

	
	public void put(String key, List<String> kcm) {
		Element element = new Element(key, kcm);
		System.out.println("Put in Cache");
		cache.put(element);
	}

   
	public List<String> get(String key, boolean removeOnGet) {
		Element result =  cache.get(key);
		System.out.println("Get from Cache");
		
		if(result != null){
			if(removeOnGet){
				cache.remove(key);
			}
			return (List<String>) result.getValue();
		}else{
			return null;
		}
	}

}
