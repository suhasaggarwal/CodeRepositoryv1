Delta Crawler Code Works in Sync with Segmentation Module and Entity Index is updated with period of 30 minutes.
Crawler is distributed in nature - each crawler crawls concurrently. 
Distributed Aajtak and IndiaToday Spiders.
Memory Consumption of a single scrapy spiders is less, so multiple scrapy spiders can run in multiple instances/processes to scale crawling.
Each spiders in an instance/process crawl concurrently but uses one CPU Core only. Multiple Scrapy Processes are started so that multiple cores of machine are utilised to fullest.

# Protocol followed - Scrape websites without being blocked

# Tor
Tor makes sure you are untraceable (so it will be impossible to get blocked by IP address). It will probably be more slow, but at least one don't have to worry about searching for proxy servers which might go down or become too slow to use. Tor will always work.

After installing Tor, start it up before scraping. This can easily be done by running the tor command, Tor will start up and run on localhost port 9050 (localhost:9050) by default. When I was running the scraper scripts from a server, I made sure that Tor was automatically started immediately after booting up Ubuntu Server.

# Install Polipo
SOCKS protocol is a lower level protocol than HTTP and it is more transparent in a sense that it doesn’t add extra info like http-header etc. Since Scrapy doesn't work directly with SOCKS proxies, we will use Polipo as a connection between Scrapy and Tor, so we are still able to use the SOCKS protocol (Polipo will talk to Tor using the SOCKS protocol). All three together will create an anonymous crawler. 

# Add Middleware
Add these lines to the middlewares.py inside your project folder (inside the scraper folder). It contains a middleware which will randomly select a different user agent on every request, and it contains the middleware for using a proxy server

import os
import random
from scrapy.conf import settings
class RandomUserAgentMiddleware(object):
    def process_request(self, request, spider):
        ua  = random.choice(settings.get('USER_AGENT_LIST'))
        if ua:
            request.headers.setdefault('User-Agent', ua)

class ProxyMiddleware(object):
    def process_request(self, request, spider):
        request.meta['proxy'] = settings.get('HTTP_PROXY')

# USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0) Gecko/16.0 Firefox/16.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10'
]

# HTTP_PROXY = 'http://127.0.0.1:8123'

# DOWNLOADER_MIDDLEWARES = {
     'scraper.middlewares.RandomUserAgentMiddleware': 400,
     'scraper.middlewares.ProxyMiddleware': 410,
     'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None
    # Disable compression middleware, so the actual HTML pages are cached
}
