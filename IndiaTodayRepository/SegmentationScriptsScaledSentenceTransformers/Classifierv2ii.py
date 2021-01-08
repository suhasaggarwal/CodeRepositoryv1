from sentence_transformers import SentenceTransformer, util
import time
import nltk
import traceback
from cognite.processpool import ProcessPool, WorkerDiedException
import threading
import re
start_time = time.time()
# model = SentenceTransformer('distilbert-base-nli-stsb-mean-tokens')
# model = SentenceTransformer('roberta-base-nli-stsb-mean-tokens')
# model = SentenceTransformer('roberta-large-nli-stsb-mean-tokens')
model = SentenceTransformer('bert-large-nli-stsb-mean-tokens')
# model = SentenceTransformer('albert_xxlarge')
lock = threading.Lock()
import urllib.request
import requests
import json
import nltk

ntlkstopwords = nltk.corpus.stopwords.words('english')

urlsegmentDatabase = open('urlsegmentDatabaseii.txt', "w")

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
    response = requests.get(uri, data=query)
    results = json.loads(response.text)

    return results


def search(uri, term):
    query = json.dumps({
        "query": {
            "match": {
                "Entity1": term
            }
        },
        "_source": ["Entity6"]
    })
    # print("Query",query)
    response = requests.get(uri, data=query)
    results = json.loads(response.text)

    return results


import urllib
import urllib.parse
import requests

from elasticsearch import Elasticsearch

es = Elasticsearch([
    'http://192.168.106.114:9200/'
])


def update(url, entity8, entity10):
    try:
        script = "ctx._source.Entity8 = " + entity8 + "; ctx._source.Entity10 = " + entity10 + ";"

        documentIdList = getdocumentIds(url)

        update_body = {
            "doc": {
                "Entity8": entity8,
                "Entity10": entity10
            }
        }
        for ids in documentIdList:
            # print(ids)
            try:
                response = es.update(index='entity', doc_type='core2', id=ids, body=update_body)
                # print("Response", response)
            except:
                traceback.print_exc()
                continue
    except Exception:
        traceback.print_exc()


def getdocumentIds(text):
    uri_search = 'http://192.168.106.114:9200/entity/core2/_search'
    queryresult = searchdocId(uri_search, text.replace("\n", ""))
    text1 = text
    data1 = [doc1 for doc1 in queryresult['hits']['hits']]
    #    print(data1)
    documentIdList = []
    if data1 is not None:
        for doc1 in data1:
            if doc1['_source'] is not None:
                # print(doc['_source'])
                text2 = doc1['_source']['Entity6']
                if text2 is not None:
                    text2 = ' '.join(text2.split())
                    text2 = text2.replace(r'/', ' ').replace("-", ' ')
                    id = doc1['_id']
                    documentIdList.append(id)
    return documentIdList


def path(text):
    uri_search = 'http://192.168.106.114:9200/entity/core2/_search'
    queryresult = search(uri_search, text.replace("\n", ""))
    text1 = text
    print(text)
    data1 = [doc1 for doc1 in queryresult['hits']['hits']]
    if data1 is not None:
        for doc1 in data1:
            if doc1['_source'] is not None:
                text2 = doc1['_source']['Entity6']
                if text2 is not None:
                    text2 = ' '.join(text2.split())
                    text2 = text2.replace(r'/', ' ').replace("-", ' ')
                    return text2
    text3 = urllib.parse.urlparse(text1).path.replace("\n", "")
    text3 = ''.join([i for i in text3 if not i.isdigit()])
    text3 = text3.replace(r'/', ' ').replace(';', ' ').replace("-", ' ')
    text5 = text3
    if "embed.indiatoday.in" in text:
        parts = text5.split()
        text5 = parts[2]
    if "m.aajtak.in/video/embed" in text:
        parts = text5.split()
        text5 = parts[0]
    text5 = text5
    return text5


sentences1 = []
wordlist = []
segmentDatabase = open('IABsegmentsv1.txt', "r")
sentenceList = segmentDatabase.readlines()
for sentence in sentenceList:
    sentences1.append(sentence.replace("\n", ""))

stopwords = open('semanticstopwords.txt', "r")
wordlist1 = stopwords.readlines()
for stopword in wordlist1:
    wordlist.append(stopword.replace("\n", ""))

for nltkwords in ntlkstopwords:
    wordlist.append(nltkwords.replace("\n", ""))
embeddings1 = model.encode(sentences1, convert_to_tensor=True)


# Compute embedding for both lists
# embeddings1 = model.encode(sentences1, convert_to_tensor=True)

def removeSpecialCharacter(s):
    i = 0
    # print("String", s)
    while i < len(s):
        try:
            # Finding the character whose
            # ASCII value fall under this
            # range
            if ((ord(s[i]) < ord('A') or
                 ord(s[i]) > ord('Z') and
                 ord(s[i]) < ord('a') or
                 ord(s[i]) > ord('z')) and
                    ord(s[i]) != ord(' ')):
                # erase function to erase
                # the character

                del s[i]
                i -= 1
            i += 1
        except Exception:
            print("String2", s[i])
            traceback.print_exc()
            continue
    return "".join(s)


cList = {}

categoriesDatabase = open('categories.txt', "r")
cdatabase = categoriesDatabase.readlines()
for categories in cdatabase:
    cList[categories.replace("_", " ").replace("\n", "").strip()] = "_" + categories.replace("\n", "")
#   print(cList)
# print(cList)
rcList = []

rootCategoriesDatabase = open('rootCategories.txt', "r")
rcdatabase = rootCategoriesDatabase.readlines()
for rootcategories in rcdatabase:
    rcList.append(rootcategories.replace("\n", "").strip())


#  print(rcList)

# print(rcList)

def getAudienceSegment(segment):
    for k in range(len(rcList)):
        if rcList[k] in segment:
            # print(rcList[k])
            # print(segment)
            return rcList[k]

    return ""


def deriveSegment(sentences2):
    segment = ""
    embeddings2 = model.encode(sentences2, batch_size=1000, show_progress_bar=True, convert_to_tensor=True,
                               num_workers=5)
    buffer = []
    # Compute cosine-similarits
    cosine_scores = util.pytorch_cos_sim(embeddings1, embeddings2)
    # Output the pairs with their score
    for i in range(len(sentences2)):
        score = cosine_scores[0][i]
        segment = sentences1[0]
        for j in range(len(sentences1)):
            #   print(score)
            #   print(sentences2)
            #   print(sentences1[0])
            #   print(cosine_scores[i][0])
            if score < cosine_scores[j][i]:
                segment = sentences1[j]
                score = cosine_scores[j][i]
        if segment is not None:
            categoryv1 = segment.replace("\"", "").strip()
            # print(categoryv1)
            categoryv2 = cList[categoryv1]
            # print(categoryv2)
            audienceSegmentv2 = getAudienceSegment(categoryv2.replace("_", ""))
            # print(audienceSegmentv2)

            if categoryv2 is not None:
                categoryv2 = categoryv2.replace(",", ".").replace("   ", " ").replace(" ", ".")
                categoryv2 = categoryv2.replace(" ", ".").strip()
                categoryv2 = categoryv2.replace("..", ".")
            if audienceSegmentv2 is not None:
                audienceSegmentv2 = audienceSegmentv2.replace(",", ".").replace("   ", " ").replace(" ", ".")
                audienceSegmentv2 = audienceSegmentv2.replace(" ", ".").strip()
                audienceSegmentv2 = audienceSegmentv2.replace("..", ".")
            print(categoryv2)
            print(audienceSegmentv2)
            buffer.append(categoryv2 + "@@" + audienceSegmentv2)
    return buffer


#urlDatabase = open('urldata.csv', "r")


class SegmentComputationWorker:
    def run(self, urlList):
        validText = ""
        newslist = []
        for url in urlList:
            try:
                # print(url)
                buffer = []
                parts = path(url)
                validpath = parts
                validText = ""
                taglist = validpath.split(';')
                validpath = ""
                for word in taglist:
                    word = word.replace("\n", "")
                    # print(word)
                    if word not in wordlist:
                        word = word.replace(":", "").replace("\r", "").replace("\n", "").replace(
                            "\t", "").replace("(", "").replace(")", "").replace('"', '')
                        validpath += word + " "
                        # print(validpath)
                validpath = ''.join([i for i in validpath if not i.isdigit()])
                validpath = ' '.join(validpath.split())
                validpath = ' '.join(validpath.strip().split())
                # print(validpath)
                word = ""
                for word in validpath.split():
                    if word not in stopwords:
                        validText = validText + " " + word.strip()
                        validText = validText.strip().lower()

                if validText is not None:
                    validText = removeSpecialCharacter(list(validText))
                validText = validText.replace("world wide we", "")
                newslist.append(validText)
            except Exception:
                # print(categoryv2)
                # print(audienceSegmentv2)
                # update(url, categoryv2, audienceSegmentv2)
                traceback.print_exc()
                continue
        return newslist


def populate_batch_buffer(segmentList):
    global lock
    buffer = []
    lock.acquire()
    buffer.extend(segmentList)
    create_database(segmentList)
    buffer.clear()
    print("buffer cleared")
    lock.release()
    print("lock released")
    return


def create_database(buffer):
    segmentDatabase = urlsegmentDatabase
    for item in buffer:
        print("Database being written")
        segmentDatabase.write("%s\n" % item)
        print("Database has been written")
    return


fname1 = "indiatodayurlListv1ii.txt"
with open(fname1) as f:
    urllist = f.readlines()

#fname = "tag01"
#with open(fname) as f:
#    taglist = f.readlines()
news_list = []
segmentList = []
entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
plol = entitysplit(urllist, 4)
print(len(plol))
t0 = time.time()
pool = ProcessPool(SegmentComputationWorker, 4)
futures = [pool.submit_job(entity) for entity in plol]
segmentOutput = [f.result() for f in futures]
news_list = [item for sublist in segmentOutput for item in sublist]
print(news_list)
pool.join()
#for tags in news_list:
#    tagDatabase.write(tags+"\n")
t1 = time.time()
print(t1-t0)
t2 = time.time()
if len(news_list) > 0:
   segmentList = deriveSegment(news_list)
print("SegmentList:", segmentList)
for url, segment in zip(urllist, segmentList):
    urlsegmentDatabase.write(url.replace("\n", "") + ":" + segment.replace("\n", "") + "\n")
t3 = time.time()
print(t3 - t2)
