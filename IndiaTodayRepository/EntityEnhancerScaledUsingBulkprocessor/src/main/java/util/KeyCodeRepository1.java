package util;

import com.spidio.UserSegmenter.Entity;

/**
 * Key-Code map, update itself by a specified interval.
 * 
 * @author Sun Ning/SNDA
 *
 */
public interface KeyCodeRepository1 {
	
	/**
	 * put data to map
	 * @param key
	 * @param kcm
	 */
	public void put(String key, Entity kcm);
	
	/**
	 * get data from map
	 * @param key
	 * @return
	 */
	public Entity get(String key);
	
	/**
	 * get data from map
	 * @param key
	 * @param removeOnGet whether to remvoe key after get
	 * @return
	 */
	public Object get(String key, boolean removeOnGet);

}
