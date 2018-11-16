package com.cuberoot.SBanner;
import java.util.List;






/**
 * Key-Code map, update itself by a specified interval.
 * 
 * @author Sun Ning/SNDA
 *
 */
public interface KeyCodeRepository {
	
	/**
	 * put data to map
	 * @param key
	 * @param kcm
	 */
	public void put(String key, List<String> kcm);
	
	/**
	 * get data from map
	 * @param key
	 * @return
	 */
	public List<String> get(String key);
	
	/**
	 * get data from map
	 * @param key
	 * @param removeOnGet whether to remvoe key after get
	 * @return
	 */
	public List<String> get(String key, boolean removeOnGet);

}
