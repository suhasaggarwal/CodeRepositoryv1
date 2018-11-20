# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from digitv2.items import Digitv2Item

class Digitv3Spider(CrawlSpider):
    name = 'digitv3'
    allowed_domains = ['lenskart.com']
    
    start_urls = ['https://www.lenskart.com/']

    rules = (
         Rule(LinkExtractor(), callback='parse_item', follow=True),
         Rule(LinkExtractor(deny=('.+careers.+'),), follow=False),
       
       
)

    def parse_item(self, response):
        href = Digitv2Item()
        href['url'] = response.url
        return href
