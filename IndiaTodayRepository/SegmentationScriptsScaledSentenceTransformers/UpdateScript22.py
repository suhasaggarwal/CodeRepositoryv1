import time
import traceback
from cognite.processpool import ProcessPool, WorkerDiedException
import threading

start_time = time.time()
model = ""

lock = threading.Lock()
import json
import nltk

ntlkstopwords = nltk.corpus.stopwords.words('english')

#urlsegmentDatabase = open('urlsegmentDatabase.txt', "w")

#tagDatabase = open('tagDatabase.txt', "a")


def searchdocId(uri, term):
    query = json.dumps({
        "size": 50000,
        "query": {
            "match": {
                "Entity1": term
            }
        },
        "_source": {
            "includes": ["meta.*"]
        },
    })
    # print("Query",query)
    try:
       response = requests.get(uri, data=query)
    except Exception:
        traceback.print_exc()
        print(uri)
    results = json.loads(response.text)

    return results

import requests

from elasticsearch import Elasticsearch

es = Elasticsearch([
    'http://192.168.106.114:9200/'
])


def update(url, entity8, entity10):
    try:
        script = "ctx._source.Entity8 = " + entity8 + "; ctx._source.Entity10 = " + entity10 + ";"

        queryresult = searchdocId('http://192.168.106.114:9200/entity/core2/_search', url)

        update_body = {
            "doc": {
                "Entity8": entity8,
                "Entity10": entity10
            }
        }
        data1 = [doc1 for doc1 in queryresult['hits']['hits']]
        documentIdList = []
        if data1 is not None:
            for doc1 in data1:
                ids = doc1["_id"]
                print(url)
                print("Document Id", ids)
                try:
                   response = es.update(index='entity', doc_type='core2', id=ids, body=update_body)
                # print("Response", response)
                except:
                      print(url)
                      traceback.print_exc()
                      continue
    except Exception:
        traceback.print_exc()


class UpdateWorker:
    def run(self, urlsegmentList):
        validText = ""
        newslist = []
        for url in urlsegmentList:
            try:
               parts = url.replace("http://", "").replace("https://", "").split("###")
               url1 = parts[0]
               url = url.split("###")[0]
               url = url.replace("\n", "")
               entity8 = parts[1].split("@@")[0].replace("\n", "")
               #print(entity8)
               entity10 = parts[1].split("@@")[1].replace("\n", "")
               #print(entity10)
               update(url, entity8, entity10)
                
            except Exception:
                #print(parts[1])
                #print(url)
                # print(categoryv2)
                # print(audienceSegmentv2)
                # update(url, categoryv2, audienceSegmentv2)
                traceback.print_exc()
                continue
        return ""

fname = "urlsegmentDatabase22.txt"
with open(fname) as f:
    urlsegmentdatabase = f.readlines()
entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
plol = entitysplit(urlsegmentdatabase, 4)
print(len(plol))
t0 = time.time()
pool = ProcessPool(UpdateWorker, 4)
futures = [pool.submit_job(entity) for entity in plol]
responseList = [f.result() for f in futures]
t1 = time.time()
print(t1-t0)
pool.join()
