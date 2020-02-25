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

app = Flask(__name__)
api = Api(app)





class computeIAB(Resource):
    def get(self):
        #Connect to databse

        import urllib
        from flask import request
        args = request.args
        print(args)  # For debugging
        text = args['url']
        
        if "hindi.thequint" in text:
            text = urllib.parse.urlparse(text).path.split('?')[0]
            text = text.replace(r'/',' ').replace('-',' ')
            print(text)

        if "http" in text and "hindi.thequint" not in text:
           import urllib.parse
           text=urllib.parse.unquote(text)
           text1 = text
           article = Article(text1)
           article.download()
           article.parse()
        # article.nlp()
           text = article.title
        else: 
           text = text.replace("’", "")
        text = text.replace("’", "")
      

        print(text)

        r1 = requests.get("http://localhost:8080/SemanticClassifierv2/getTextAnalysis?text=" + text)

        jData = json.loads(r1.content.decode())
        print(jData)

        for x in jData:
            if x['rho'] < 0.1:
                jData.remove(x)
        y = ""
        z = []
        k = ""

        
        print(jData)
        return jData


api.add_resource(computeIAB, '/getTextAnalytics',endpoint='getTextAnalytics')

if __name__ == '__main__':
     app.run(host='0.0.0.0',port=5001,threaded=True)



