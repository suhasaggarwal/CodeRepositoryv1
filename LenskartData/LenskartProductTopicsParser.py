import requests
from lxml import html
from bs4 import BeautifulSoup
import re
import MySQLdb as mdb
import sys
import newspaper
from newspaper import Article
#fname="UrlListSimulatedTraffic.txt"
#with open(fname) as f:
 #   content = f.readlines()


#con = mdb.connect('205.147.102.47', 'root',
 #       'qwerty12@', 'middleware');

import scrapy
from bs4 import BeautifulSoup
from langdetect import detect
import requests


class lenskartSpider(scrapy.Spider):
   name = 'lenskartv1'

   with open('/root/lenskart.txt') as f:
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
#content = {'https://traveltriangle.com/packages/4nights-5days-andaman-honeymoon?id=1136&travelmonth=7'}
#content = ['https://traveltriangle.com/packages/1night-2days-jim-corbett-family-tour?id=5809&travelmonth=7']
#content = ['https://traveltriangle.com/packages/3nights-4days-kasol-tour?id=5285&travelmonth=7']
#content = ['https://traveltriangle.com/packages/manali_new_year_hilly_escape_2nights_3days?id=5153&pprod=f']
#content = ['https://traveltriangle.com/packages/chakrata_nature_delight_1night_2days?id=5000&pprod=f']
#content = ['https://traveltriangle.com/packages/7nights-8days-jammu-kashmir-tour?id=479&travelmonth=7']
#content = ['https://traveltriangle.com/packages/4nights-5days-bestselling-sikkim-gangtok-darjeeling-tour?id=531&travelmonth=7']
#content = ['https://traveltriangle.com/packages/2nights-3days-goa-sightseeing-tour?id=9302&travelmonth=7']
#content = ['https://traveltriangle.com/packages/2nights-3days-goa-vacation-delight?id=4909&travelmonth=7']
#content = ['https://traveltriangle.com/tamil-nadu-tourism/ooty/places-to-visit/pykara-lake']
#url = 'https://traveltriangle.com/packages/7nights-8days-vietnam-with-cambodia-trip?id=4031&travelmonth=7'
#content =  ['https://www.lenskart.com/black-sky-blue-full-rim-rectangle-medium-size-51-vincent-chase-air-vc-0318-uo20-eyeglasses.html']
#content = ['https://www.lenskart.com/tommy-hilfiger-th9018-c3-size-50-sunglasses.html']
#content = ['https://www.lenskart.com/silver-rimless-rectangle-medium-size-53-john-jacobs-shibuya-crossing-jj-0039-c24-eyeglasses.html']
#content = ['https://www.lenskart.com/black-sky-blue-full-rim-rectangle-medium-size-51-vincent-chase-air-vc-0318-uo20-eyeglasses.html']
#content= ['https://www.lenskart.com/vogue-vo2787-2278-size-51women-s-eyeglasses.html']
    content = ['https://www.lenskart.com/blue-gunmetal-matte-blue-full-rim-wayfarer-medium-size-50-vincent-chase-vc-air-007-vc-e10115-c3-eyeglasses.html']
    list1=[]
    list3=[]
    lista=[]
    location=""
    k=0
    data1=""
    data2=""
    data3=""
    url = ""
    category1=""
    variable1=""
    variable2 =""
    datavariables1=[]





    for span in soup.findAll("ul",class_="breadcrumb breadcrumb-lk col-sm-8"):
        for c in span.findAll("span"):
            category = c.text.strip()
            category1=category1+category
            print(category)


    i=0
    specifications=""
    for span in soup.findAll("div",class_="pdpInfo"):
        for d1 in span.findAll("span"):
            if i % 2 == 0:
                if "technical" not in d1.text.strip():
                    if "general" not in d1.text.strip():
                        if d1.text.strip() not in specifications:
                            specifications = specifications.strip() + d1.text.strip() + ","
            else:
                if "technical" not in d1.text.strip():
                    if "general" not in d1.text.strip():
                        if d1.text.strip() not in specifications:
                           specifications = specifications.strip() + d1.text.strip()


#print(specifications)
    specifications = specifications.replace(",,,",";").replace("Frame Size,","")
    print(specifications)
#specifications = specifications.replace(",",":")
    specifications = re.sub('(,[^,]*),', r'\1;', specifications)
    specifications=specifications.replace(",",":")
    print(specifications)
    author = specifications.split(";")[0].split(":")[1]
    print("Author:"+author)
    section = specifications.split(";")[1].split(":")[1]
    print("Section:"+section)
        #    if i==1:
        #       variable1=variable1+":"+d1.text.strip()
        #      i=0;
        #        if ":" in variable1:
        #          datavariables1.append(variable1)
        #    if i==0:
        #       variable1=d1.text.strip()
        #      if variable1 != 'technical':
        #           i=i+1
        #       if variable1 != 'general':
        #           i=i+1

        #for d in span.findAll("a"):
           # print(d.text.strip())

    for span in soup.findAll("div",class_="text-center default-color fs20"):
             print(span.text.strip())

    print(datavariables1)
    category1 = category1.replace("»","/")
    print(category1)
    article = Article(url)
    article.download()
            #  print(article.html)
    article.parse()
    a = article.authors
    print(a)
    b = article.publish_date
    print(b)
    c = article.top_image
    print(c)
    d = article.title
    print(d)


    #brand=a.text.strip()
        #for a in span.findAll("img"):
            #print("Product Image:"+a['src'])
            #productImage=a['src']
        #for a in span.findAll(class_="pID"):
            #print("Product Id:"+a.text.strip())
            #productId=a.text.strip()
        #for a in span.findAll(class_="f_price"):
            #print("Price :"+a.text.strip())
            #price=a.text.strip()
#for div in soup.findAll("div",class_="pdp_info wrapper"):
    #for div1 in div.findAll("div",class_="breadcrums"):
    #for b in soup.findAll("span",attrs={"itemprop":"title"}):
        #print("Category:\n"+b.text.strip())
        #category=category+">"+b.text

    #for span in soup.findAll("div",itemprop="seller"):
        #for seller in span.findAll(itemprop="name"):
            #print("Seller:"+seller.text.strip())
            #seller=seller.text.strip()

    #category = category.strip()
    #print(brand)
    #print(seller)
    #print(productId)
    #print(productImage)
    #print(category)
    #print(price)

    #productId = productId.split(":")[1].strip()
    #description=description.replace("\n",",")
    #print(description + "\n")
    #productTopicsMeta = description.split(",")
    #productTopics="Brand:"+brand
    #productTopics = productTopics.strip()
    #i=0
    #for k in productTopicsMeta:

        #if ":" in k:
         #   k1 = k
          #  if i==0:
           #    if brand!='':
            #      productTopics = productTopics + ","+k1.strip()
             #  else:
              #     productTopics=k1.strip()

            #else:
             #   productTopics = productTopics + "," + k1.strip()



    #    i=i+1

#    print(productTopics)
 #   if productTopics != "Brand:":
  #     sql = "UPDATE Article SET Tags = '%s' WHERE ArticleUrl like '%%%s%%'" % (productTopics.strip(), x.strip())
   #    print(sql)
    #   cur2 = con.cursor()
     #  cur2.execute(sql)
      # con.commit()




