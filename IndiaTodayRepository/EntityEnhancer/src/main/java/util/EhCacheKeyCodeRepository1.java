package util;

import com.spidio.UserSegmenter.Entity;

/**
 * 
 */

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.CacheManager;
import util.KeyCodeRepository;

/**
 * @author Sun Ning/SNDA
 * @since Sep 14, 2009
 * 
 */

public class EhCacheKeyCodeRepository1 implements KeyCodeRepository1 {

	private Cache cache;

    /**
     * 
     */
	public static EhCacheKeyCodeRepository1 ehcache;
	
	public static EhCacheKeyCodeRepository1 getInstance(){
		
		if(ehcache == null)
			return new EhCacheKeyCodeRepository1();
		
		
		else
			return ehcache;
		
	}
	
	
	
	public EhCacheKeyCodeRepository1() {
		init();
	}

    /**
     * initialize properties.
     */
	public void init() {
        CacheManager cm = CacheManager.getInstance();
		cache = cm.getCache("cache3");
		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sdo.captchaserver.repos.KeyCodeRepository#get(java.lang.String)
	 */
	@Override
	public Entity get(String key) {
		Element result =  cache.get(key);
		if(result != null){
			cache.remove(key);
			return (Entity)result.getValue();
		}else{
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sdo.captchaserver.repos.KeyCodeRepository#put(java.lang.String,
	 * com.sdo.captchaserver.repos.KeyCodeModel)
	 */
	@Override
	public void put(String key, Entity kcm) {
		Element element = new Element(key, kcm);
		System.out.println("Put in Cache");
		cache.put(element);
	}

    /**
     * get data from map.
     * @param key
     * @param removeOnGet remove key if set to ture
     * @return
     */
	@Override
	public Entity get(String key, boolean removeOnGet) {
		Element result =  cache.get(key);
		System.out.println("Get from Cache");
		
		if(result != null){
			if(removeOnGet){
				cache.remove(key);
			}
			return (Entity)result.getValue();
		}else{
			return null;
		}
	}

}
