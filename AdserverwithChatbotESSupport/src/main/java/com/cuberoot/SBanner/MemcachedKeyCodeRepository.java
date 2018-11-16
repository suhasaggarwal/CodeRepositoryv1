package com.cuberoot.SBanner;

/**
 * 
 */

import java.io.IOException;
import java.util.List;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;


public class MemcachedKeyCodeRepository implements KeyCodeRepository {

    protected MemcachedClient memcache;

    protected Logger logger = Logger.getLogger(MemcachedKeyCodeRepository.class);

    public MemcachedKeyCodeRepository(){
        try {
            init();
        } catch (IOException ex) {
            logger.fatal(ex.getMessage(), ex);
        }

    }

    protected void init() throws IOException{
        memcache  = new MemcachedClient(AddrUtil.getAddresses(
                GlobalConfiguration.get("yan.mc.host")));
        
    }

	/* (non-Javadoc)
	 * @see com.sdo.captchaserver.repos.KeyCodeRepository#get(java.lang.String)
	 */
	@Override
	public List<String> get(String key) {
        return this.get(key, true);
	}

	/* (non-Javadoc)
	 * @see com.sdo.captchaserver.repos.KeyCodeRepository#put(java.lang.String, com.sdo.captchaserver.repos.KeyCodeModel)
	 */
	@Override
	public void put(String key, List<String> kcm) {
		memcache.set(key, 60*GlobalConfiguration.getInt("yan.expireminutes"), kcm);
	}

	@Override
	public List<String> get(String key, boolean removeOnGet) {
	      List<String> kcm = (List<String>)memcache.get(key);
        if(kcm != null){
            if(removeOnGet){
                memcache.delete(key);
            }
            return kcm;
        }else{
            return null;
        }
	}

}
