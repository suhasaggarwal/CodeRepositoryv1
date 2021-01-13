# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup
from newspaper import Article
import requests
import datetime
import requests
from langdetect import detect
import sys
import csv
import urllib.parse
import re


class IndiaToday05Spider(scrapy.Spider):
    name = 'indiatoday05'
    with open('/mnt/data/indiatodaydeltaurls005') as f:
         start_urls = [url.strip() for url in f.readlines()]
        # url = "https://www.indiatoday.in/movies/regional-cinema/story/naachiyar-release-date-out-jyothikas-film-to-hit-the-screens-this-february-1131128-2018-01-10"
        # start = url 
    def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        d = {}
        data = {}
        global k
        publisher = ""
        author = ""
        section = ""
        title = ""
        publishedTime = ""
        i = 1
        additiontime = ""
        tag = ""
        scraped_info = {}
        a = ""
        b = ""
        c = ""
        k = ""
        e = ""
        f = ""
        g = ""
        n = ""
       
        publishedTime = soup.find("meta", {"property":"article:published_time"})
        if publishedTime is not None:
           b = publishedTime['content']
           #print(b)
           n=b.split("+")[0]
           #print("PublishedTime:"+ publishedTime)

        keywords = soup.find("meta", {"name":"news_keywords"})
        if keywords is not None:
           keywords = keywords['content'].replace(",",";")
           #print("Keywords:"+keywords)
         # language = detect(keywords)
          # if language != "en":
          #    keywords = None   
           #print("Keywords:"+ keywords)
#        pattern = re.compile("author-name.*?[,]{1}")
#        pattern = re.compile("_sf_async_config.authors =.*?[;]{1}")
#        author_text = pattern.findall(response.text)
       # author_text = soup.findall(re.compile("author-name.*?[,]{1}"))
#        if author_text is not None and len(author_text) != 0:
#           author = author_text[0].split("=")[1].replace(";","").replace("'","")
           #print("Author:"+ author)
        pattern = re.compile("_sf_async_config.authors =.*?[;]{1}")
        author_text = pattern.findall(response.text)
       # author_text = soup.findall(re.compile("author-name.*?[,]{1}"))
        if author_text is not None and len(author_text) != 0:
           author = author_text[0].split("=")[1].replace(";","").replace("'","")
           #print("Author:"+author)
 
        #print("Url:"+ response.url)
        from urllib.parse import urlparse
        path = ""
        path = urlparse(response.url).path
        #print("Path:" + path)
#        section = []
#        section = path.split("/")
#        print(section)
#        relevantSection = soup.find(itemprop="name")
#        if relevantSection is not None:
#           publishedTime=publishedTime.get("content")
#           b = publishedTime
#           print(b)
#           n=b.split("+")[0]
#        print("Section:" + relevantSection)
        relevantSection = ""
     #   for span in soup.findAll("div",class_="sec_name"):
     #       for b in span.findAll(itemprop="name"):
     #           relevantSection=b.text.strip()
        parser = soup.findAll("span", class_="sec_name")
        relevantSection = parser[0].get_text()

        article = Article(response.url)
        article.download()
        #  print(article.html)
        article.parse()
      # a = article.authors
      #  a = ";".join(a)
      #  print(a)
      #  a=a.split(";")[0]
        c = article.top_img
        #print(c)
        t = article.title
       # lang = detect(t)
       # if lang != "en":
       #    t=path 
        if t is None:
           t=path
        #print("Title:"+t)
        t=t.replace("-"," ")
        if keywords is None:
            r1 = requests.get("http://192.168.103.138:5001/getTextAnalytics?url=" + t)
            import json
            jData = r1.json()
            invalidJSONcheck = json.loads("""{"message": "Internal Server Error"}""")
            if jData != invalidJSONcheck:
              #  print(jData)
                for x in jData:
                    if x['rho'] < 0.1:
                        jData.remove(x)
                y = set()
                k = ""
                for x in jData:
                 #   print(x['Title'])
                    y.add(x['Title'])
                    k = ';'.join(str(s) for s in y)

        if keywords is not None:
           k=keywords
        if k is not None:
           print("Keywords1:"+k)
        #print(e)
        #print("TagsNormal:" + e)
        #print("TagsSemantics:" + k)
        # sql="UPDATE Article SET Author= '%s',Tags = '%s', PublishDate = '%s', MainImage = '%s',ArticleTitle = '%s' WHERE ArticleUrl = '%s'" % (a,e,b,c,d,url)
        # print(sql)
        #     cur1.execute(sql)
        #     con.commit()

        # published Data present in meta tags
        #b = publishedTime  
        #print(b)
        #n=b.split("+")[0]


        additiontime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        parsedurl = urlparse(response.url)
        if k is not None:
           k=k.replace(",", ";").replace("null"," ")
           k=str(k.encode('ascii','ignore').decode('utf-8')).replace("'","")
        t=t.replace(",", ";")
        t=str(t.encode('ascii','ignore').decode('utf-8')).replace("'","")
       # print("Title:"+t)
        s=relevantSection.replace(",", ";")
        import hashlib
        pbHash = hashlib.sha1(str(response.url).encode('utf-8')).hexdigest()

        d["Entity6"] = k
        d["Entity4"] = str(author.encode('ascii','ignore').decode('utf-8')).replace("'","").replace(",",";")
        d["Entity7"] = c
        d["Entity5"] = n
        d["Entity3"] = ""
        d["Entity8"] = ""
        d["Entity10"] = ""
        d["Entity11"] = "gkkghfc82b3ceae411d8bc38bced7a1fff3b"
        d["Entity1"] = response.url.split('?')[0]
        d["Entity12"] = s
        d["Entity9"] = t
        d["Entity2"] = parsedurl.path
        d["Entity13"] = additiontime
        d["EntityId"] = pbHash
        #   yield d.values()

        import json
        # with open('data30.json', 'a') as fp:
        import codecs
        fp = codecs.open('indiatodaydelta05.json', mode='a', encoding='utf-8')
        line1 = '{"index":{}}' + "\n"
        line = line1 + json.dumps(d) + "\n"
        fp.write(line)
        fp.close
