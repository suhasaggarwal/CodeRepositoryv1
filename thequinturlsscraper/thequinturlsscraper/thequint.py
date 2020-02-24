# -*- coding: utf-8 -*-
import scrapy


class Digitspiderv1Spider(scrapy.Spider):
    name = 'digitspiderv1'
    allowed_domains = ['digit.in']
    start_urls = ['https://www.thequint.com/entertainment']

    def parse(self, response):
        
        
        hxs = scrapy.Selector(response)
        # extract all links from page
        all_links = hxs.xpath('*//a/@href').extract()
        # iterate over links
        for link in all_links:
            yield scrapy.http.Request(url=link, callback=print_this_link)
 
    def print_this_link(self, link):
       
        import json
        # with open('data30.json', 'a') as fp:
        import codecs
        fp = codecs.open('digiturlsfulllist.json', mode='a', encoding='utf-8')
       
        line = link
        fp.write(line)
        fp.close

