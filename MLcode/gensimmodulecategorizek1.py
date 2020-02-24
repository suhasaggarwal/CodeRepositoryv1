import collections
from newspaper import Article
import urllib.request
import requests
import json
import sys
import csv
from flask import Flask, request
from flask_restful import Resource, Api
from json import dumps
from bs4 import BeautifulSoup
from langdetect import detect

app = Flask(__name__)
api = Api(app)





class computeIAB(Resource):
    def search(self,uri,term):
        query = json.dumps({
           "query": {
            "match": {
                "Entity1": term
            }
          }
        })
        response = requests.get(uri, data=query)
        results = json.loads(response.text)
        return results    


    def get(self):
        #Connect to database 
        import urllib
        from flask import request
        args = request.args
#        print(args)  # For debugging
        text = args['url']
        import urllib.parse
        text=urllib.parse.unquote(text)
        uri_search = 'http://192.168.101.132:9200/entity/core2/_search'
        queryresult = self.search(uri_search, text)
        text1 = text
        data1 = [doc1 for doc1 in queryresult['hits']['hits']]
        if data1 is not None:
            for doc1 in data1:
                 if doc1['_source'] is not None:
#                    print(doc['_source'])
                    text = doc1['_source']['Entity9']
                    if text is not None:
#                       print(tags)
                       break

        if "hindi.thequint" in text1:       
           text = urllib.parse.urlparse(text1).path
           text = text.replace(r'/',' ').replace('-',' ')
        text = text.replace("’", "")
#        print(text)
        r = requests.get(text1.strip())
        soup = BeautifulSoup(r.content, "html.parser")
        g = ""
        keywords = soup.find("meta", {"name":"keywords"})
        if keywords is not None:
           keywords = keywords['content'].replace(","," ")
           language = detect(keywords)
           if language != "en":
              keywords = ""
           g = keywords
#           print(g)
#        print(queryresult)
        tags = ""
        data = [doc for doc in queryresult['hits']['hits']]
        if data is not None:
            for doc in data:
                 if doc['_source'] is not None:
#                    print(doc['_source'])
                    tags = doc['_source']['Entity6']
                    if tags is not None:
#                       print(tags)
                       break
        if tags is not None:
            y = tags
        if y is "":
            y = text
        l = g
        if l is "":
            l = y

#        print(l.lower().replace("-", " ").replace(","," ").replace(";"," "))
        r = requests.get("http://localhost:82/IAB/" + l.lower().replace("-", " ").replace(","," ").replace(";"," "))
#        print(r.content)
        data = r.content.decode("utf-8").strip().replace('"','')
#        print(data)
        return data


api.add_resource(computeIAB, '/getTextCategory',endpoint='getTextCategory')

if __name__ == '__main__':
     app.run(host='0.0.0.0',port=5002,threaded=True)



