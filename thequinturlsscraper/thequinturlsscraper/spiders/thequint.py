# -*- coding: utf-8 -*-
import scrapy
from urllib.parse import urlparse
from scrapy.http import HtmlResponse
from scrapy.spiders import Rule
from scrapy.linkextractor import LinkExtractor
import traceback
import urllib3
import xmltodict
import json
import codecs

class QuintUrlScraper(scrapy.Spider):
    def getxml():
#Fetch seed  urls from quint sitemap
#Use base url if sitemap is absent, it can be directly added to start url and no sitemap parsing is required
       url = "https://www.thequint.com/sitemap/sitemap-section.xml"

       http = urllib3.PoolManager()

       response = http.request('GET', url)
       try:
          data = xmltodict.parse(response.data)
       except:
          print("Failed to parse xml from response (%s)" % traceback.format_exc())
       return data
   
    def is_absolute(self,url):
        return bool(urlparse(url).netloc)


    start_urls = []
    name = 'quinturlscraper'
    allowed_domains = ['thequint.com']
    deny_domains = ['facebook.com','twitter.com']
    xml_response = getxml()
    i=0
#Parse XML here
    for content1 in xml_response['urlset'].items():
        if i != 0:
           key,value = content1
           for dicts in value:
               for key1,value1 in dicts.items():
                   if key1 == "loc":       
                      start_urls.append(value1)   

        i=i+1 
      

  #  start_urls = ['https://www.thequint.com/'] 
    
    def parse(self, response):

        hxs = scrapy.Selector(response)
    # extract all links from page
        all_links = hxs.xpath('@href').extract()
        
    # iterate over links
        for link in all_links:
            if self.is_absolute(link):
#Dump url list to file by calling print in callback             
               yield scrapy.http.Request(url=link, callback=self.print(link))
 
    def print(self,link):     
        fp = codecs.open('quinturlsfulllistv11.json', mode='a', encoding='utf-8')
        line = link
        fp.write(line)
        fp.write("\n")
        fp.close

