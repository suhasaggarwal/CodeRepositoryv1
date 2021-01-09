package com.websystique.springmvc.controller;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.publisherdata.model.CaffeineCache;
import com.websystique.springmvc.model.Reports;
import com.websystique.springmvc.service.ReportService;
import util.EhcacheKeyCodeRepository;

@RestController
public class ReportRestController {

	@Autowired
	ReportService reportService; // Service which will do all data retrieval/manipulation work

	// -------------------Retrieve Report with Id--------------------------------------------------------

	public static Map<String,String> cookieMapCache = new HashMap<String,String>();
	
	@CrossOrigin
	@RequestMapping(value = "/getCookieData", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getReport(HttpServletRequest request) {

		String data = reportService.getUserProfile(request);
		if (data == null) {

			return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<String>(data, HttpStatus.OK);
	}

	/*
	@CrossOrigin
	@RequestMapping(value = "/", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> loadCache(HttpServletRequest request) {

	
		BufferedReader br = null;
		FileReader fr = null;
		String key = null;
		String value = null;

		try {
			System.setOut(new PrintStream(new File("I://CacheLogs.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
		try {

			// br = new BufferedReader(new FileReader(FILENAME));
		//	fr = new FileReader(request.getParameter("cacheFile"));
		//	br = new BufferedReader(fr);

			List<String> lines = FileUtils.readLines(new File(request.getParameter("cacheFile")));
			for (String line : lines) {
				String parts[] = line.split(":@");
				if (parts[1].matches("^[@#]+")) {
					// contains only listed chars
				} else {
					System.out.println("Put in cache");
					EhcacheKeyCodeRepository.ehcache.put(parts[0], parts[1]);
				}

			}

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (Exception ex) {

				ex.printStackTrace();

			}
		}
		return new ResponseEntity<String>("", HttpStatus.OK);
	 
	
	}
	
	*/
	
	@CrossOrigin
	@RequestMapping(value = "/loadMap", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> loadMap(HttpServletRequest request) {

		
		BufferedReader br = null;
		FileReader fr = null;
		String key = null;
		String value = null;

		try {
			System.setOut(new PrintStream(new File("I://MapCache.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
		try {

			// br = new BufferedReader(new FileReader(FILENAME));
		//	fr = new FileReader(request.getParameter("cacheFile"));
		//	br = new BufferedReader(fr);

			List<String> lines = FileUtils.readLines(new File(request.getParameter("cacheFile")));
			for (String line : lines) {
				String parts[] = line.split(":@");
				if (parts[1].matches("^[@#]+")) {
					// contains only listed chars
				} else {
					System.out.println("Put in Mapcache");
					cookieMapCache.put(parts[0], parts[1]);
				}

			}

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (Exception ex) {

				ex.printStackTrace();

			}
		}
		return new ResponseEntity<String>("", HttpStatus.OK);
	}

	
	@CrossOrigin
	@RequestMapping(value = "/loadNativeCache", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> loadNativeCache(HttpServletRequest request) {

		
		BufferedReader br = null;
		FileReader fr = null;
		String key = null;
		String value = null;

		try {
			System.setOut(new PrintStream(new File("I://NativeCache.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
		try {

			// br = new BufferedReader(new FileReader(FILENAME));
		//	fr = new FileReader(request.getParameter("cacheFile"));
		//	br = new BufferedReader(fr);

			List<String> lines = FileUtils.readLines(new File(request.getParameter("cacheFile")));
			for (String line : lines) {
				String parts[] = line.split(":@");
				if (parts[1].matches("^[@#]+")) {
					// contains only listed chars
				} else {
					System.out.println("Put in NativeMapcache");
					CaffeineCache.cache.put(parts[0], parts[1]);
				}

			}

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (Exception ex) {

				ex.printStackTrace();

			}
		}
	
		return new ResponseEntity<String>("", HttpStatus.OK);
	
	}
		
	
	@CrossOrigin
	@RequestMapping(value = "/insertKeyCache", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> insertKeyCache(HttpServletRequest request) {
		String key = null;
		String value = null;
		try {

			String cacheEntry = request.getParameter("cacheEntry");
			String parts[] = cacheEntry.split(":@");
			if (parts[1].matches("^[@#]+")) {
					// contains only listed chars
				} else {
					CaffeineCache.cache.put(parts[0], parts[1]);
				}

		} catch (Exception e) {

			e.printStackTrace();

		}
	
		return new ResponseEntity<String>("", HttpStatus.OK);
	
	}
		
	
	

	@CrossOrigin
	@RequestMapping(value = "/report/v1/getLatestData", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public String getLatestDatadata() {

		EhcacheKeyCodeRepository ehcache = EhcacheKeyCodeRepository.getInstance();
	//	ehcache.basicCache.removeAll(keys)

		return "Data Reloaded! Please note there will be 5 minute latencies in Reports are compared to real time";
	}

	@CrossOrigin
	@RequestMapping(value = "/report/v1/printEhcache", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public String getLatestData() {

		try {
			System.setOut(new PrintStream(new File("I://PrintCache.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		EhcacheKeyCodeRepository ehcache = EhcacheKeyCodeRepository.getInstance();
		//Cache cache = (Cache) ehcache.cacheManager.getCache("basicCache", String.class, String.class);
	    //Iterator<Cache.Entry> iterator = cache.iterator();

	    //while (iterator.hasNext()) {
	    //    String key = (String) iterator.next().getKey();
	    //   System.out.println(key);
	   // }
		return "Ehcache printed to the console!!";
	}
	
	
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/printMapcache", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public String getLatestMapData() {
		try {
			System.setOut(new PrintStream(new File("I://PrintMapCache.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		

		Iterator<Map.Entry<String, String>> itr = cookieMapCache.entrySet().iterator();

	    while (itr.hasNext()) {
	        String key = (String) itr.next().getKey();
	        System.out.println(key);
	    }
		return "Mapcache printed to the console!!";
	}

	
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/printNativeMapcache", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public String getLatestNativeMapData() {
	
		try {
			System.setOut(new PrintStream(new File("I://PrintNativeMapCache.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	

		Iterator<Map.Entry<String, String>> itr = CaffeineCache.cache.asMap().entrySet().iterator();

	    while (itr.hasNext()) {
	        String key = (String) itr.next().getKey();
	        System.out.println(key);
	    }

		return "Native Mapcache printed to the console!!";

	}
	
	
	@CrossOrigin
	@RequestMapping(value = "/report/v1/getEhcacheStats", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public String getCacheStats() {

	//	EhCacheKeyCodeRepository ehcache = EhCacheKeyCodeRepository.getInstance();
	//	ehcache.getCacheStats();
		EhcacheKeyCodeRepository ehcache = EhcacheKeyCodeRepository.getInstance();
		//ehcache.basicCache.
		return "Ehcache Statistics Printed !!";
	}

}
