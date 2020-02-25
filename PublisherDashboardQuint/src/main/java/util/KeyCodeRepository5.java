package util;

import com.publisherdata.model.Article;
import com.publisherdata.model.User;




/**
 * Key-Code map, update itself by a specified interval.
 * 
 * @author Sun Ning/SNDA
 *
 */
public interface KeyCodeRepository5 {
	
	/**
	 * put data to map
	 * @param key
	 * @param kcm
	 */
	public void put(String key, User kcm);
	
	/**
	 * get data from map
	 * @param key
	 * @return
	 */
	public User get(String key);
	
	/**
	 * get data from map
	 * @param key
	 * @param removeOnGet whether to remvoe key after get
	 * @return
	 */
	public Object get(String key, boolean removeOnGet);

}
