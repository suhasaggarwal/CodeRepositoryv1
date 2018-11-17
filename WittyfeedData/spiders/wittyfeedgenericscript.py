import scrapy
from bs4 import BeautifulSoup
from langdetect import detect
import requests

class WittyfeedSpider(scrapy.Spider):
   name = 'wittyfeedgyanchand'
   with open('/root/fulluncrawledurls.txt') as f:
       start_urls = [url.strip() for url in f.readlines()]
  

    
   

   def parse(self, response):
    soup = BeautifulSoup(response.text, "html.parser")
    d= {}
    data={}
    global k
    publisher= ""
    author=""
    section=""
    title=""
    publishedTime=""
    i=1
    additiontime=""
    tag=""
    scraped_info={}
    a=""
    b=""
    c=""
    k=""
    e=""
    f=""
    g=""

#    publisher = soup.find("meta", property="article:publisher")
#    a=publisher['content']
    author = soup.find("meta", property="article:author")
    if author is not None:
              b=author['content']
    section = soup.find("meta", property="article:section")
   
    if section is not None:
               c=section['content']
    

 #  title = soup.find("meta", property="og:title")
  #  k=title['content']
    publishedTime = soup.find("meta", property="article:published_time")

    if publishedTime is not None:    
                     e=publishedTime['content']
    image = soup.find("meta", property="og:image")
    
    if image is not None: 
             f=image['content']
  
    tag = soup.findAll("meta", property="article:tag")
    words = []
    lang = ""
    langugage = ""
    l=""
    for tagdata in tag:
        lang = detect(tagdata['content'])
        if lang == "en":
           g=g+","+tagdata['content'] 
       #g=g+","+tagdata['content']
       # ''' 
       # words = tagdata.split("-")
       # for word in words:
        #    lang = detect(word)
         #   if lang != "en":
          #     language = "other"     
        #if language != 'other':
       # '''    
       #  g=g+","+tagdata['content']
    g=g[1:]
    l=''.join([i if ord(i) < 128 else '' for i in g])
    l=l.replace("--","").replace("---","").replace("----","").replace("-----","").replace(";;","")
    from urlparse import urlparse
    parsedurl = urlparse(response.url)
    parsedurlpathv1 = parsedurl.path.replace("/"," ").replace("-"," ").replace("html","")

    r1 = requests.get("http://101.53.130.215:8080/SemanticClassifierv2/getTextAnalysis?text=" + parsedurlpathv1)
    import json
    jData = r1.json()
        # print(jData)

    for x in jData:
        if x['rho'] < 0.1:
           jData.remove(x)
    y = set()
    k = ""
    for x in jData:
        print(x['Title'])
        y.add(x['Title'])
    k = ';'.join(str(s) for s in y)

 #   print(a)
    print(b)
    print(c)
    print(d)
    print(e)
    print(f)
#    print(k)
    import datetime

    additiontime= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    from urlparse import urlparse
    parsedurl = urlparse(response.url)

    import hashlib
    pbHash=hashlib.sha1(str(response.url).encode('utf-8')).hexdigest()

    d["Entity6"]=k.replace(",",";");
    d["Entity4"]=urlparse(b).path[1:]
    d["Entity7"]=f
    d["Entity5"]=e.replace("T"," ")
    d["Entity3"]=""
    d["Entity8"]=""
    d["Entity10"]=""
    d["Entity11"]="gaac82b3ceae411d8bc38bced7a1fff3"
    d["Entity1"]=response.url
    d["Entity12"]=c.replace(",",";")
    import string
    d["Entity9"]=string.capwords(parsedurl.path.replace("-"," "))
    d["Entity2"]=parsedurl.path
    d["Entity13"]=additiontime
    d["EntityId"]=pbHash
 #   yield d.values()

    import json
    #with open('data30.json', 'a') as fp:
    import codecs
    fp= codecs.open('wittyfeeditemsgyanchanddelta.json', mode = 'a', encoding = 'utf-8')
    if len(d) != 0:
       line1 = '{"index":{}}'+"\n"
       line = line1 + json.dumps(d) + "\n"
       fp.write(line)
       fp.close
