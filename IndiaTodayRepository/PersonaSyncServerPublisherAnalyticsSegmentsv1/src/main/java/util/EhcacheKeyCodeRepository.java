package util;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManager;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Configuration;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.impl.internal.statistics.DefaultStatisticsService;
import org.ehcache.xml.XmlConfiguration;

public class EhcacheKeyCodeRepository {
 
  public Cache<String, String> basicCache;
  Configuration xmlConfig = new XmlConfiguration(EhcacheKeyCodeRepository.class.getResource("/ehcache.xml"));
  public static EhcacheKeyCodeRepository ehcache = new EhcacheKeyCodeRepository();
  public CacheManager cacheManager = null;
  private EhcacheKeyCodeRepository() {
	  init();
  } 

  public static EhcacheKeyCodeRepository getInstance() 
  {
      return ehcache; 
  } 

  public void init() {
	
	  cacheManager = newCacheManager(xmlConfig);
	  cacheManager.init();

      basicCache = cacheManager.getCache("basicCache", String.class, String.class);

   //   LOGGER.info("Putting to cache");
   //   basicCache.put(1L, "da one!");
   //   String value = basicCache.get(1L);
  //    LOGGER.info("Retrieved '{}'", value);
    
}
  
}