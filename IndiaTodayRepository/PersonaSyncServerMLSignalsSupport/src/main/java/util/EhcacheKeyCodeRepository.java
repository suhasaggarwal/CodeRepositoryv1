package util;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManager;

import java.util.concurrent.TimeUnit;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.core.Ehcache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.impl.internal.statistics.DefaultStatisticsService;
import org.ehcache.xml.XmlConfiguration;

public class EhcacheKeyCodeRepository {
 
  public Cache<String, String> basicCache;
  Configuration xmlConfig = new XmlConfiguration(EhcacheKeyCodeRepository.class.getResource("/ehcache.xml"));
  public static StatisticsService statisticsService = new DefaultStatisticsService();
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
	
	  //cacheManager = newCacheManager(xmlConfig);
	  //CacheConfiguration<Integer, String> cacheConfiguration =
	  //	      CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class, resources).build();
   
	  CacheConfiguration cacheConfiguration = xmlConfig.getCacheConfigurations().get("basicCache");  
	  CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
	  //PersistentCacheManager persistentCacheManager = (PersistentCacheManager) CacheManagerBuilder.newCacheManagerBuilder()
			   // .with(CacheManagerBuilder.persistence("/mnt/data/cache")) 
	            .withCache("basicCache", cacheConfiguration)
			    .using(statisticsService)
	            .build();
	  cacheManager.init();
      basicCache = cacheManager.getCache("basicCache", String.class, String.class);
     
      
    
      
   //   LOGGER.info("Putting to cache");
   //   basicCache.put(1L, "da one!");
   //   String value = basicCache.get(1L);
  //    LOGGER.info("Retrieved '{}'", value);
    
}
  
}