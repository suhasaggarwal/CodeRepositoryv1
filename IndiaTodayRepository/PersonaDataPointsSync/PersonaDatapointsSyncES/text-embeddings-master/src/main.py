import json
import time
import itertools
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from cognite.processpool import ProcessPool, WorkerDiedException
from datetime import datetime
import traceback
personaSignalsbatch = []

class AffinityScoreMapWorker:
    # To do: process Entity in batches
    def run(self, entity1, entity2a1, entity2b1, entity2c1, entity2a2, entity2b2, entity2c2, entity2a3, entity2b3, entity2c3, entity3, entity4, entity5, entity6, entity7, entity8, entity9):
        for line1, line2a1, line2b1, line2c1, line2a2, line2b2, line2c2, line2a3, line2b3, line2c3, line3, line4, line5, line6, line7, line8, line9 in itertools.zip_longest(entity1, entity2a1, entity2b1, entity2c1, entity2a2, entity2b2, entity2c2, entity2a3, entity2b3, entity2c3, entity3, entity4, entity5, entity6, entity7, entity8, entity9):
            try:
               doc = {}
               parts = line1.split('-emotion-')
               print(line1)
               cookie_id = parts[0]
               doc["cookie_id"] = cookie_id
               emotionsList = parts[1].rstrip(',\n').split(',')
               for scores in emotionsList:
                   emotionParts = scores.split(':')
                   doc[emotionParts[0]] = int(float(emotionParts[1]))
               parts = line2a1.split('-gender-')
               print(line2a1)
               demgender = parts[1].rstrip(',\n')
               doc["genderwiki"] = demgender
               parts = line2b1.split('-agegroup-')
               print(line2b1)
               demag = parts[1].rstrip(',\n')
               doc["agegroupwiki"] = demag
               parts = line2c1.split('-incomelevel-')
               print(line2c1)
               demil = parts[1].rstrip(',\n')
               doc["incomelevelwiki"] = demil
               parts = line2a2.split('gender-')
               print(line2a2)
               demgender = parts[1].rstrip(',\n')
               doc["gendertwitter"] = demgender
               parts = line2b2.split('agegroup-')
               print(line2b2)
               demag = parts[1].rstrip(',\n')
               doc["agegrouptwitter"] = demag
               parts = line2c2.split('incomelevel-')
               print(line2c2)
               demil = parts[1].rstrip(',\n')
               doc["incomeleveltwitter"] = demil
               parts = line2a3.split('gender-')
               print(line2a3)
               demgender = parts[1].rstrip(',\n')
               doc["genderconceptnet"] = demgender
               parts = line2b3.split('agegroup-')
               print(line2b3)
               demag = parts[1].rstrip(',\n')
               doc["agegroupconceptnet"] = demag
               parts = line2c3.split('incomelevel-')
               print(line2c3)
               demil = parts[1].rstrip(',\n')
               doc["incomelevelconceptnet"] = demil
               parts = line3.split('@:')
               topicscores = parts[1].rstrip(',\n')
               doc["topicsegmentaffinity"] = topicscores.replace(":", ",")
               parts = line4.split('@:')
               segmentscores = parts[1].rstrip(',\n')
               doc["segmentsegmentaffinity"] = segmentscores.replace(":", ",")
               parts = line9.split(':@')
               persona = parts[1].rstrip(',\n')
               doc["full_persona"] = persona
               parts = line5.split('@:')
               personasegment = parts[1].rstrip(',\n')
               doc["personasegmentaffinity"] = personasegment.replace(":", ",")
               parts = line6.split('@:')
               personacreative = parts[1].rstrip(',\n')
               doc["personacreativeaffinity"] = personacreative.replace(":", ",")
               parts = line7.split('@:')
               personatopiccreative = parts[1].rstrip(',\n')
               doc["personatopiccreativeaffinity"] = personatopiccreative.replace(":", ",")
               parts = line8.split('@:')
               personasegmentcreative = parts[1].rstrip(',\n')
               doc["personasegmentcreativeaffinity"] = personasegmentcreative.replace(":", ",")
               doc["insertion_time"] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
               personaSignalsbatch.append(doc)
               if len(personaSignalsbatch) == 4:
                  index_batch(personaSignalsbatch)
                  personaSignalsbatch.clear()
            except:
                  traceback.print_exc()
                  continue
        return

def index_batch(docs):
    bulk(client, docs, timeout="30s", index=INDEX_NAME)
    print(docs)
    print("Documents Indexed")

##### SEARCHING #####

def run_query_loop():
    while True:
        try:
            handle_query()
        except KeyboardInterrupt:
            return

def handle_query():
    query = input("Enter query: ")

    embedding_start = time.time()
    embedding_time = time.time() - embedding_start
    query_vector = ""
    script_query = {
        "script_score": {
            "query": {"match_all": {}},
            "script": {
                "source": "cosineSimilarity(params.query_vector, doc['refcurrentoriginal_vector']) + 1.0",
                "params": {"query_vector": query_vector}
            }
        }
    }
    SEARCH_SIZE = 10
    search_start = time.time()
    response = client.search(
        index=INDEX_NAME,
        body={
            "size": SEARCH_SIZE,
            "query": script_query,
            "_source": {"includes": ["refcurrentoriginal"]}
        }
    )
    search_time = time.time() - search_start

    print()
    print("{} total hits.".format(response["hits"]["total"]["value"]))
    print("embedding time: {:.2f} ms".format(embedding_time * 1000))
    print("search time: {:.2f} ms".format(search_time * 1000))
    for hit in response["hits"]["hits"]:
        print("id: {}, score: {}".format(hit["_id"], hit["_score"]))
        print(hit["_source"])
        print()


##### MAIN SCRIPT #####

if __name__ == '__main__':
    INDEX_NAME = "personaaffinity"
    Enhanced_Persona_Data_File = "../data/enhancedpersonaDatabase.txt"
    Emotion_Persona_Data_File = "../data/emotionsPersonaDatabasev1.txt"
    Demographic_Persona_Data_File11 = "../data/dp11.txt"
    Demographic_Persona_Data_File12 = "../data/dp12.txt"
    Demographic_Persona_Data_File13 = "../data/dp13.txt"
    Demographic_Persona_Data_File21 = "../data/dp21.txt"
    Demographic_Persona_Data_File22 = "../data/dp22.txt"
    Demographic_Persona_Data_File23 = "../data/dp23.txt"
    Demographic_Persona_Data_File31 = "../data/dp31.txt"
    Demographic_Persona_Data_File32 = "../data/dp32.txt"
    Demographic_Persona_Data_File33 = "../data/dp33.txt"
    Persona_Topics_Segment_Affinity_File = "../data/cookietopicsegmentaffinitydatabasev1.txt"
    Persona_Segment_Segment_Affinity_File = "../data/cookietopicsegmentaffinitydatabasev1.txt"
    Full_Persona_Segment_Affinity_File = "../data/cookietopicsegmentaffinitydatabasev1.txt"
    Full_Persona_Creative_Affinity_File = "../data/cookietopicsegmentaffinitydatabasev1.txt"
    Persona_Topics_Creative_Affinity_File = "../data/cookietopicsegmentaffinitydatabasev1.txt"
    Persona_Segment_Creative_Affinity_File = "../data/cookietopicsegmentaffinitydatabasev1.txt"
    client = Elasticsearch()
    print('Persona Segment Affinity Computation Worker')
    database = ""
    with open(Emotion_Persona_Data_File) as f:
        ep = f.readlines()
    with open(Demographic_Persona_Data_File11) as f:
        dc11 = f.readlines()
    with open(Demographic_Persona_Data_File21) as f:
        dc21 = f.readlines()
    with open(Demographic_Persona_Data_File31) as f:
        dc31 = f.readlines()
    with open(Demographic_Persona_Data_File12) as f:
        dc12 = f.readlines()
    with open(Demographic_Persona_Data_File22) as f:
        dc22 = f.readlines()
    with open(Demographic_Persona_Data_File32) as f:
        dc32 = f.readlines()
    with open(Demographic_Persona_Data_File13) as f:
        dc13 = f.readlines()
    with open(Demographic_Persona_Data_File23) as f:
        dc23 = f.readlines()
    with open(Demographic_Persona_Data_File33) as f:
        dc33 = f.readlines()
    with open(Persona_Topics_Segment_Affinity_File) as f:
        ptsa = f.readlines()
    with open(Persona_Segment_Segment_Affinity_File) as f:
        pssa = f.readlines()
    with open(Full_Persona_Segment_Affinity_File) as f:
        fpsa = f.readlines()
    with open(Full_Persona_Creative_Affinity_File) as f:
        fpca = f.readlines()
    with open(Persona_Topics_Creative_Affinity_File) as f:
        ptca = f.readlines()
    with open(Persona_Segment_Creative_Affinity_File) as f:
        psca = f.readlines()
    with open(Enhanced_Persona_Data_File) as f:
        enhancedPersona = f.readlines()

    BATCH_SIZE = 10000

    entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    epl = entitysplit(ep, 4)
    dcl11 = entitysplit(dc11, 4)
    dcl21 = entitysplit(dc12, 4)
    dcl31 = entitysplit(dc13, 4)
    dcl12 = entitysplit(dc21, 4)
    dcl22 = entitysplit(dc22, 4)
    dcl32 = entitysplit(dc23, 4)
    dcl13 = entitysplit(dc31, 4)
    dcl23 = entitysplit(dc32, 4)
    dcl33 = entitysplit(dc33, 4)
    ptsal = entitysplit(ptsa, 4)
    pssal = entitysplit(pssa, 4)
    fpsal = entitysplit(fpsa, 4)
    fpcal = entitysplit(fpca, 4)
    ptcal = entitysplit(ptca, 4)
    pscal = entitysplit(psca, 4)
    enhancedPersona = entitysplit(enhancedPersona, 4)
    pool = ProcessPool(AffinityScoreMapWorker, 4)
    futures = [pool.submit_job(entity1, entity2a1, entity2b1, entity2c1, entity2a2, entity2b2, entity2c2, entity2a3, entity2b3, entity2c3, entity3, entity4, entity5, entity6, entity7, entity8, entity9) for entity1, entity2a1, entity2b1, entity2c1, entity2a2, entity2b2, entity2c2, entity2a3, entity2b3, entity2c3, entity3, entity4, entity5, entity6, entity7, entity8, entity9 in itertools.zip_longest(epl, dcl11, dcl21, dcl31, dcl12, dcl22, dcl32, dcl13, dcl23, dcl33, ptsal, pssal, fpsal, fpcal, ptcal, pscal, enhancedPersona)]
    segments = [f.result() for f in futures]
    pool.join()
