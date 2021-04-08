package com.publisherdata.model;

import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;


public class CaffeineCache {

	public static Cache<String, String> cache = Caffeine.newBuilder()
			  .expireAfterWrite(1440, TimeUnit.MINUTES)
			  .maximumSize(12000000)
			  .build();


}
