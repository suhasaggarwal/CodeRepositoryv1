import requests
from lxml import html
from bs4 import BeautifulSoup

import MySQLdb as mdb
import sys
import newspaper
from newspaper import Article

#fname="UrlListSimulatedTraffic.txt"
#with open(fname) as f:
 #   content = f.readlines()


#con = mdb.connect('205.147.102.47', 'root',
 #       'qwerty12@', 'middleware');



brand= ""
description=""
productImage=""
productId=""
price=""
category=""
seller=""
#content = ['https://www.proptiger.com/chennai/arakkonam/gkp-city-sri-krishna-nagar-664111']
#content = ['https://www.proptiger.com/greater-noida/techzone-4/gaursons-gaur-city-center-1651624']
#content = ['https://www.proptiger.com/greater-noida/techzone-4/rsl-sports-home-662979']
#content = ['https://www.proptiger.com/chennai/gkp-city-sri-krishna-nagar-arakkonam-5236566/940-sqft-plot']
content = {'https://www.proptiger.com/chennai/vadapalani/sapthrishi-buildcon-llp-asta-avm-780657'}
url = 'https://www.proptiger.com/chennai/vadapalani/sapthrishi-buildcon-llp-asta-avm-780657'
url = 'https://www.proptiger.com/chennai/vadapalani/sapthrishi-buildcon-llp-asta-avm-780657'
list1=[]
list3=[]
lista=[]
location=""
k=0
data1=""
data2=""
data3=""
tags= set()
category1= ""
author=""
d={}
builder=""
image=""
with open('proptiger.txt') as f:
    content = f.read().splitlines()

for x in content:
    r = requests.get(x.strip())
    soup = BeautifulSoup(r.content, "html.parser")
    maincategory = ""
    category=""
    categorydata=""
    data1=""
    data2=""
    data3=""
    image=""
    location=""
    builder=""
    builderprice=""
    title=""
    titledata=""
    parsedurl=""
    d={}
    k=0
    for span in soup.findAll("div",class_="js-breadcrumb-seo breadcrumb-seo ta-l black-bc"):
       # for a in span.findAll('ul'):
            #print("Description:"+a.text.strip())
        #external_span = soup.find('li')

        for e in span('ul'):
            e.decompose()
        for c in span.findAll(itemprop="name"):
            category=category+"/"+c.text.strip()
            #list2.append(category)
        e=""

    for span in soup.findAll("div",class_="title-builder"):
        for b in span.findAll('a'):
            title=b.text
            print(title)
    for span in soup.findAll("div",class_="max1140 pos-rel ht100 d-flex js-gallery-open"):
        image=span.attrs['data-absolutepath']


    for span in soup.findAll("span",class_="va-middle proj-address"):
        for b in span.findAll('a'):
            location=b.text.replace("(show on map)","")
            break;

    for span in soup.findAll("div", class_="title-builder"):
        for b in span.findAll('a'):
            builder = b.text





    for r in soup.findAll("div", class_="spec-value f16"):
        if k==0:
           data1=r.text

        if k==1:
           data2=r.text
        k=k+1


    if data1 is None:
       data1 = ""

    if data2 is None:
       data2 = ""


    for r1 in soup.findAll("div", class_="general-range"):
        builderprice=r1.text
        builderprice1=builderprice.strip()
        #print(builderprice1)
        data3=builderprice1
        break;#for a in span.findAll(itemprop="brand"):
            #print("Brand:"+a.text.strip())

    data3=data1.replace("\u20b9\u00a0",'Rs').replace("\u00a0-\u00a0","")
    data1=data1.replace("\u00a0","")
    data2=data2.replace("\u00a0","").replace(",","")
    print(data1)
    print(data2)
    print(data3)


    category="/".join(category.split("/")[:6])

    if data3 is None:
       data3 = ""

    if data1 == data3:
       data3= ""

    print(location)

    if location is None:
       location = ""

    if category is None:
       category = ""

    print(category)
    title = soup.find("meta", property="og:title")
    if title is not None:
       titledata=title["content"]
    print(titledata)

    from urllib.parse import urlparse

    parsedurl = urlparse(x.strip())
    import datetime

    additiontime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # from urllib.parse import urlparse
    # parsedurl = urlparse(response.url)

    import hashlib

    pbHash = hashlib.sha1(str(x.strip()).encode('utf-8')).hexdigest()
    location=location.replace(",",";")
    d["Entity6"] = data1+";"+data2+";"+data3
    d["Entity4"] = builder
    d["Entity7"] = image
    d["Entity5"] = ""
    d["Entity3"] = ""
    d["Entity8"] = category
    categorydata = category.split("/")

    if len(categorydata)>2:
       maincategory = categorydata[1] + "/" + categorydata[2]
    if category == "":
        maincategory = ""

    d["Entity10"] = maincategory

    d["Entity11"] = "gaac82b3ceae411d8bc38bced7a1fff559"
    d["Entity1"] = x.strip()
    d["Entity12"] = location.replace(",",";")
    import string

    d["Entity9"] = string.capwords(parsedurl.path.replace("-", " "))
    d["Entity2"] = parsedurl.path
    d["Entity13"] = additiontime
    d["EntityId"] = pbHash

    import json
    # with open('data30.json', 'a') as fp:
    import codecs

    fp = codecs.open('proptigerdatav21.json', mode='a', encoding='utf-8')
    if len(d) != 0:
        line1 = '{"index":{}}' + "\n"
        line = line1 + json.dumps(d) + "\n"
        fp.write(line)
        fp.close
