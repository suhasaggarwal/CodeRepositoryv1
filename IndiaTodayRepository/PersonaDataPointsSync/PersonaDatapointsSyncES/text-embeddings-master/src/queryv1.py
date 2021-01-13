# import the Elasticsearch client library
from elasticsearch import Elasticsearch
import time
import numpy as np
from elasticsearch import Elasticsearch
from emstore import Emstore
import traceback
from elasticsearch import logger as es_logger
es_logger.setLevel("DEBUG")
# for debugging purposes
import logging

# used to make HTTP requests to the Elasticsearch cluster
import requests

# import 'json' to convert strings to JSON format
import json

# catch import errors that arise from breaking version
# changes from Python 2 and 3
try:
    # try Python 2
    import httplib as http_client
except (ImportError, ModuleNotFoundError) as error:
    # try Python 3
    print ("Python 3: Importing http.client:", error, '\n')
    import http.client as http_client

# set the debug level
http_client.HTTPConnection.debuglevel = 1

# initialize the logging to have the debugger return information
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# store the DEBUG information in a 'requests_log' variable
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

'''
make this Elasticsearch cURL request in Python:
curl -XGET localhost:9200/_cat/indices?v
'''

# use a Python tuple to pass header options to the request call
param = (('v', ''),) # '-v' is for --verbose

# call the class's method to get an HTTP response model
resp = requests.get('http://localhost:9200/_cat/indices', params=param)
# the call will return a `requests.models.Response` object

print ('\nHTTP code:', resp.status_code, '-- response:', resp, '\n')
print ('dir:', dir(resp), '\n')
print ('response text:', resp.text)

# function for the cURL requests
def elasticsearch_curl(uri='http://localhost:9200/', json_body='', verb='get'):
    # pass header option for content type if request has a
    # body to avoid Content-Type error in Elasticsearch v6.0
    headers = {
        'Content-Type': 'application/json',
    }

    try:
        # make HTTP verb parameter case-insensitive by converting to lower()
        if verb.lower() == "get":
            resp = requests.get(uri, headers=headers, data=json_body)
        elif verb.lower() == "post":
            resp = requests.post(uri, headers=headers, data=json_body)
        elif verb.lower() == "put":
            resp = requests.put(uri, headers=headers, data=json_body)

        # read the text object string
        try:
            resp_text = json.loads(resp.text)
        except:
            resp_text = resp.text

        # catch exceptions and print errors to terminal
    except Exception as error:
        print ('\nelasticsearch_curl() error:', error)
        resp_text = error

    # return the Python dict of the request
    print ("resp_text:", resp_text)
    return resp_text



##### SEARCHING #####

def run_query_loop(cv):
    try:
        handle_query(cv)
    except:
        traceback.print_exc()

def handle_query(cv):
    #query_vector = [-0.217529296875, -0.0740966796875, -0.01471710205078125, 0.056304931640625, 0.08001708984375, -0.07208251953125, 0.18701171875, 0.08001708984375, 0.11920166015625, -0.1470947265625, -0.076171875, -0.05316162109375, -0.2225341796875, 0.047271728515625, -0.1278076171875, 0.0843505859375, -0.134521484375, -0.039794921875, -0.1470947265625, 0.172607421875, -0.10113525390625, 0.2325439453125, 0.08612060546875, 0.0224151611328125, -0.0299072265625, 0.173583984375, -0.1016845703125, 0.09808349609375, -0.09222412109375, -0.1014404296875, 0.061981201171875, -0.132080078125, -0.09375, -0.2098388671875, 0.2255859375, 0.1683349609375, 0.1400146484375, -0.05938720703125, 0.1025390625, -0.3203125, -0.07940673828125, -0.0286407470703125, 0.02020263671875, -0.041748046875, -0.283203125, -0.042694091796875, -0.061492919921875, 0.1416015625, -0.24755859375, 0.0259552001953125, 0.2154541015625, -0.14892578125, -0.061309814453125, 0.236328125, 0.2015380859375, 0.11138916015625, 0.0380859375, -0.2078857421875, -0.0762939453125, -0.119384765625, -0.040008544921875, 0.31591796875, -0.08538818359375, -0.0247039794921875, 0.1646728515625, -0.05377197265625, -0.026153564453125, 0.1163330078125, 0.109130859375, 0.034332275390625, -0.0258331298828125, 0.190673828125, -0.1444091796875, 0.0546875, -0.1014404296875, -0.06842041015625, -0.210205078125, -0.163818359375, -0.0253753662109375, -0.004390716552734375, -0.027740478515625, 0.0556640625, -0.22607421875, -0.1446533203125, -0.1458740234375, -0.00569915771484375, -0.0169525146484375, -0.04443359375, 0.0689697265625, 0.11944580078125, 0.10028076171875, -0.00737762451171875, 0.05682373046875, -0.1422119140625, -0.06439208984375, -0.03802490234375, -0.034637451171875, -0.093505859375, -0.0849609375, 0.09820556640625, -0.05560302734375, 0.1146240234375, 0.08203125, 0.056793212890625, 0.019866943359375, 0.09600830078125, 0.15283203125, 0.086669921875, 0.200927734375, 0.1943359375, 0.0975341796875, 0.2257080078125, 0.16064453125, 0.059722900390625, 0.303955078125, -0.03948974609375, -0.06365966796875, 0.016876220703125, 0.1378173828125, 0.044891357421875, 0.0189056396484375, -0.0286407470703125, -0.06427001953125, -0.1494140625, -0.1832275390625, -0.1265869140625, -0.04022216796875, -0.0540771484375, -0.0269927978515625, -0.11907958984375, 0.168701171875, -0.1082763671875, -0.10491943359375, -0.07452392578125, -0.16552734375, 0.007450103759765625, -0.0290374755859375, -0.037872314453125, 0.1583251953125, -0.10430908203125, -0.019439697265625, 0.11346435546875, -0.0001596212387084961, 0.0885009765625, 0.1424560546875, -0.07568359375, 0.10601806640625, 0.07470703125, 0.005146026611328125, -0.0180206298828125, 0.18408203125, 0.056976318359375, 0.169189453125, 0.060516357421875, -0.161376953125, -0.021820068359375, 0.033599853515625, 0.1470947265625, 0.07672119140625, 0.01104736328125, -0.0002570152282714844, -0.00826263427734375, 0.12005615234375, -0.08929443359375, 0.09588623046875, -0.0192108154296875, -0.07611083984375, 0.06878662109375, -0.0673828125, -0.0185394287109375, 0.166015625, -0.0081939697265625, 0.037322998046875, -0.1094970703125, -0.11260986328125, -0.10711669921875, 0.33447265625, 0.02008056640625, -0.150146484375, 0.0170135498046875, 0.2103271484375, -0.11651611328125, 0.0638427734375, -0.005847930908203125, 0.06463623046875, 0.1949462890625, -0.125, 0.038787841796875, -0.283203125, -0.0654296875, 0.09234619140625, -0.139892578125, 0.0127105712890625, 0.055511474609375, 0.10162353515625, 0.11798095703125, -0.08050537109375, -0.09783935546875, 0.10247802734375, -0.04339599609375, 0.02545166015625, 0.1591796875, -0.013519287109375, -0.10760498046875, -0.1031494140625, 0.0251312255859375, 0.202880859375, -0.09375, -0.13720703125, -0.2125244140625, -0.0198822021484375, -0.09619140625, 0.1204833984375, 0.0863037109375, -0.06158447265625, 0.035003662109375, -0.06890869140625, -0.09088134765625, 0.2880859375, -0.1148681640625, 0.1201171875, 0.052734375, 0.189453125, 0.105224609375, -0.14599609375, 0.11798095703125, 0.2054443359375, -0.10882568359375, 0.281005859375, 0.06463623046875, -0.038726806640625, -0.00787353515625, 0.264892578125, -0.015899658203125, 0.098388671875, 0.1007080078125, -0.176513671875, -0.05120849609375, -0.1632080078125, -0.0587158203125, 0.0684814453125, -0.1866455078125, 0.02801513671875, -0.034637451171875, 0.1246337890625, 0.048858642578125, -0.022796630859375, 0.022979736328125, 0.11798095703125, -0.057403564453125, -0.1282958984375, 0.1944580078125, 0.034912109375, 0.004375457763671875, -0.0487060546875, 0.055206298828125, -0.10015869140625, 0.0113525390625, 0.04644775390625, -0.1173095703125, 0.1986083984375, -0.1932373046875, -0.06390380859375, -0.06829833984375, 0.2423095703125, 0.189697265625, 0.3095703125, -0.0179595947265625, 0.0142364501953125, 0.164794921875, -0.09088134765625, 0.04998779296875, 0.09307861328125, 0.203125, 0.212890625, 0.2161865234375, -0.267578125, -0.09832763671875, 0.128662109375, -0.1326904296875, -0.162841796875, -0.056243896484375, 0.053070068359375, 0.10064697265625, -0.003261566162109375, 0.047607421875, -0.0193328857421875, 0.094482421875, -0.050750732421875, -0.114990234375, 0.1341552734375, 0.2408447265625, 0.04522705078125, -0.2314453125, -0.0291595458984375, 0.035369873046875, -0.1224365234375, -0.09991455078125, 0.051788330078125, -0.05316162109375]
    query_vector = np.array(cv).tolist()
    print(query_vector)
    script_query = {
        "script_score": {
            "query": {"match_all": {}},
            "script": {
                "source": "cosineSimilarity(params.persona_vector_wiki, doc['persona_vector_wiki']) + 1.0",
                "params": {"persona_vector_wiki": query_vector}
            }
        }
    }



    try:
        response = elasticsearch_curl(
            'http://localhost:9200/personaaffinity',
            verb='get',
            json_body=script_query
        )
        print(response)

    except Exception as e:
           print(e)
           traceback.print_exc()
    #for hit in response["hits"]["hits"]:
    #    print("id: {}, score: {}".format(hit["_id"], hit["_score"]))
    #    print(hit["_source"])


##### MAIN SCRIPT #####
#### FOr generating Look Alike Clone Cookie pools
if __name__ == '__main__':
    INDEX_NAME = "personaaffinity"
    SEARCH_SIZE = 100
    client = Elasticsearch()
    pcWiki = []
    indiatodaywikidb = Emstore('../data/IndiaTodayWikiFullCorpus')
    for ck, cv in indiatodaywikidb.__iter__():
        try:
           pcWiki.append((ck, cv))
           run_query_loop(cv)
           break
        except:
            traceback.print_exc()
            continue
    client = Elasticsearch(timeout=600)
    print("Cookie Clusters Generated")


