Code to generate entities - quint1.py, qunit2.py... There are multiple files as server has enough capacity and multiple cralwing processes can be run in parallel (scrapy crawl quint1, scrapy crawl quint2..) as multithreaded scrapy crawl does not consume much RAM. 
Code to generate Url list from the website (quint)-  thequint.py.
If base website does not serve as efficient seed URL for crawling. Start urls can be derived from sitemap of website as - https://www.thequint.com/sitemap/sitemap-section.xml derived here.
