package util;

import org.elasticsearch.plugin.nlpcn.executors.CSVResult;



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
	public void put(String key, CSVResult kcm);
	
	/**
	 * get data from map
	 * @param key
	 * @return
	 */
	public CSVResult get(String key);
	
	/**
	 * get data from map
	 * @param key
	 * @param removeOnGet whether to remvoe key after get
	 * @return
	 */
	public Object get(String key, boolean removeOnGet);

}
