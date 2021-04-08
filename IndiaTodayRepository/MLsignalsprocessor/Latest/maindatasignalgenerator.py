import json
import time
import itertools
#from elasticsearch import Elasticsearch
#from elasticsearch.helpers import bulk
from cognite.processpool import ProcessPool, WorkerDiedException
from datetime import datetime
import traceback
import re
import sys

personaSignalsbatch = []
colormap = {}
ehcacheDatabase = open('EhcacheUploadFilepart1.txt', "w")


class AffinityScoreMapWorker:
    # To do: process Entity in batches
    def run(self, entity1, entity2a1, entity2b1, entity2c1, entity2a2, entity2b2, entity2c2, entity2a3, entity2b3,
            entity2c3, entity3, entity4, entity5, entity6, entity7, entity8, entity9, entity10):
        print(entity1)
        print(entity2a1)
        print(entity2b1)
        print(entity2c1)
        print(entity2a2)
        print(entity2b2)
        print(entity2c2)
        print(entity2a3)
        print(entity2b3)
        print(entity2c3)
        print(entity3)
        print(entity4)
        print(entity5)
        print(entity6)
        print(entity7)
        print(entity8)
        print(entity9)
        print(entity10)
        doc = {}
        doc["cookie_id"] = ""
        doc["genderwiki"] = ""
        doc["agegroupwiki"] = ""
        doc["incomelevelwiki"] = ""
        doc["gendertwitter"] = ""
        doc["agegrouptwitter"] = ""
        doc["incomeleveltwitter"] = ""
        doc["genderconceptnet"] = ""
        doc["agegroupconceptnet"] = ""
        doc["incomelevelconceptnet"] = ""
        doc["segmentsegmentaffinity"] = ""
        doc["topicsegmentaffinity"] = ""
        doc["full_persona"] = ""
        doc["personasegmentaffinity"] = ""
        doc["personacreativeaffinity"] = ""
        doc["personatopiccreativeaffinity"] = ""
        doc["personasegmentcreativeaffinity"] = ""
        doc["color_affinities"] = ""
        for line1, line2a1, line2b1, line2c1, line2a2, line2b2, line2c2, line2a3, line2b3, line2c3, line3, line4, line5, line6, line7, line8, line9, line10 in itertools.zip_longest(
                entity1, entity2a1, entity2b1, entity2c1, entity2a2, entity2b2, entity2c2, entity2a3, entity2b3,
                entity2c3, entity3, entity4, entity5, entity6, entity7, entity8, entity9, entity10):
            try:
                emotionscores = ""

                if line1 is not None:
                    parts1 = line1.split('-emotion-')
                print(line1)
                cookie_id = parts1[0]
                if cookie_id is not None:
                    doc["cookie_id"] = cookie_id
                if parts1[1] is not None:
                    emotionsList = parts1[1].rstrip(',\n').split(',')
                for scores in emotionsList:
                    emotionParts = scores.split(':')
                    if len(emotionParts) > 1:
                        doc[emotionParts[0]] = int(float(emotionParts[1]))
                        emotionscores = emotionscores + str(int(float(emotionParts[1]))) + ","
                        emotionscores = emotionscores.rstrip('\n')
                if line2a1 is not None:
                    parts2 = line2a1.split('-gender-wiki-')
                    print(line2a1)
                    if parts2[1] is not None:
                        demgender = parts2[1].rstrip(',\n')
                        doc["genderwiki"] = demgender
                if line2b1 is not None:
                    parts3 = line2b1.split('-agegroup-wiki-')
                    print(line2b1)
                    if parts3[1] is not None:
                        demag = parts3[1].rstrip(',\n')
                        doc["agegroupwiki"] = demag
                if line2c1 is not None:
                    parts4 = line2c1.split('-incomelevel-wiki-')
                    print(line2c1)
                    if parts4[1] is not None:
                        demil = parts4[1].rstrip(',\n')
                        doc["incomelevelwiki"] = demil
                if line2a2 is not None:
                    parts5 = line2a2.split('-gender-twitter-')
                    # print(line2a2)
                    if parts5[1] is not None:
                        demgender = parts5[1].rstrip(',\n')
                        doc["gendertwitter"] = demgender
                if line2b2 is not None:
                    parts6 = line2b2.split('-agegroup-twitter-')
                    # print(line2b2)
                    if parts6[1] is not None:
                        demag = parts6[1].rstrip(',\n')
                        doc["agegrouptwitter"] = demag
                if line2c2 is not None:
                    parts7 = line2c2.split('-incomelevel-twitter-')
                    # print(line2c2)
                    if parts7[1] is not None:
                        demil = parts7[1].rstrip(',\n')
                        doc["incomeleveltwitter"] = demil
                if line2a3 is not None:
                    parts8 = line2a3.split('-gender-conceptnet-')
                    # print(line2a3)
                    if parts8[1] is not None:
                        demgender = parts8[1].rstrip(',\n')
                        doc["genderconceptnet"] = demgender
                if line2b3 is not None:
                    parts9 = line2b3.split('-agegroup-conceptnet-')
                    if parts9[1] is not None:
                        demag = parts9[1].rstrip(',\n')
                        doc["agegroupconceptnet"] = demag
                if line2c3 is not None:
                    parts10 = line2c3.split('-incomelevel-conceptnet-')
                    if parts10[1] is not None:
                        demil = parts10[1].rstrip(',\n')
                        doc["incomelevelconceptnet"] = demil
                if line3 is not None:
                    parts11 = line3.split('@:')
                    if parts11[1] is not None:
                        topicscores = parts11[1].rstrip(',\n')
                        doc["topicsegmentaffinity"] = topicscores.replace(":", ",")
                if line4 is not None:
                    parts12 = line4.split('@:')
                    if parts12[1] is not None:
                        segmentscores = parts12[1].rstrip(',\n')
                        doc["segmentsegmentaffinity"] = segmentscores.replace(":", ",")
                if line9 is not None:
                    partscolor = line9.split(':@')
                    if partscolor[1] is not None:
                        personacolor = partscolor[1].rstrip(',\n')
                        cookie_id_color = partscolor[0]
                if line10 is not None:
                    parts13 = line10.split(':@')
                    if parts13[1] is not None:
                        persona = parts13[1].rstrip(',\n')
                        cookie = parts13[0]
                        doc["full_persona"] = persona
                if line5 is not None:
                    parts14 = line5.split(':')
                    if parts14[2] is not None:
                        personasegment = parts14[2].rstrip(',\n')
                        if doc["personasegmentaffinity"] is None:
                            doc["personasegmentaffinity"] = personasegment.replace(".0", "")
                        else:
                            doc["personasegmentaffinity"] = doc[
                                                                "personasegmentaffinity"] + "," + personasegment.replace(
                                ".0", "")
                        doc["personasegmentaffinity"].rstrip('\n')
                if line6 is not None:
                    parts15 = line6.split(':')
                    if parts15[2] is not None:
                        personacreative = parts15[2].rstrip(',\n')
                        if doc["personacreativeaffinity"] is None:
                            doc["personacreativeaffinity"] = personacreative.replace(".0", "")
                        else:
                            doc["personacreativeaffinity"] = doc[
                                                                 "personacreativeaffinity"] + "," + personacreative.replace(
                                ".0", "")
                        doc["personacreativeaffinity"].rstrip('\n')
                if line7 is not None:
                    parts16 = line7.split('@:')
                    if parts16[1] is not None:
                        personatopiccreative = parts16[1].rstrip(',\n')
                        doc["personatopiccreativeaffinity"] = personatopiccreative.replace(":", ",")
                if line8 is not None:
                    parts17 = line8.split('@:')
                    if parts17[1] is not None:
                        personasegmentcreative = parts17[1].rstrip(',\n')
                        doc["personasegmentcreativeaffinity"] = personasegmentcreative.replace(":", ",")
                doc["insertion_time"] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                if personacolor is not None:
                    colormapkey = personacolor.split(",")
                    doc["color_affinities"] = colormap[
                        colormapkey[len(colormapkey) - 2] + " " + colormapkey[len(colormapkey) - 3]].rstrip(',\n')
                # print(doc)
                if persona is not None:
                    personaparts = persona.split('@@');
                colorpadder = 6
                colorcodeslist = ""
                if doc["color_affinities"] is not None:
                    colorcodes = doc["color_affinities"].split(",")
                for colorcode in colorcodes:
                    colorpadder = colorpadder - 1
                    colorcodeslist = colorcodeslist + colorcode + ","
                for i in range(colorpadder):
                    if i == 0:
                        colorcodeslist = colorcodeslist + "0"
                    else:
                        colorcodeslist = colorcodeslist + "," + "0"
                if colorcodeslist is not None:
                    colorcodeslist = colorcodeslist.rstrip(',\n')
                sectionpadder = 15
                personaPart6 = []
                personaPart7 = ""
                if len(personaparts) > 5 and personaparts[5] is not None:
                    personaparts[5] = personaparts[5].replace("-", " ")
                    publisherAnalyticsSignalString = personaparts[5]
                    personaPart5 = re.findall('([a-zA-Z,]*)\d*.*', personaparts[5])
                    # print(personaPart5[0])
                    # personaPart5 = personaPart5[0].rstrip(',')
                    # personaParts5 = personaparts[5].split(",")
                    if len(personaPart5) > 0:
                        if len(personaparts) > 5:
                            print("Persona Part 5:" + personaparts[5])
                            print("Persona Part 6:" + personaPart5[0])
                            publisherAnalyticsSignalString = personaparts[5].replace(personaPart5[0], "")
                            personaPart6 = personaPart5[0].split(",")
                for i in personaPart6:
                    sectionpadder = sectionpadder - 1
                for i in range(sectionpadder):
                    if len(personaPart5) > 0:
                        if "##" not in personaPart5[0]:
                            if i == 0:
                                personaPart5[0] = personaPart5[0] + "," + "0" + ","
                            else:
                                personaPart5[0] = personaPart5[0] + "0" + ","
                            personaPart7 = personaPart5[0].rstrip(',')
                analyticSignals = personaPart7 + publisherAnalyticsSignalString
                if analyticSignals is not None:
                    analyticSignals = analyticSignals.rstrip(',\n')
                if analyticSignals is None:
                    analyticSignals = ""
                mlsignals = analyticSignals + "," + emotionscores + colorcodeslist + doc["personasegmentaffinity"] + \
                            doc["personacreativeaffinity"]
                caffinsegmentpadder = 30
                personaPart3 = []
                if personaparts[3] is not None:
                    personaPart3 = personaparts[3].split(",")
                for i in personaPart3:
                    caffinsegmentpadder = caffinsegmentpadder - 1
                if personaparts[3] is None:
                    personaparts[3] = ""
                for i in range(caffinsegmentpadder):
                    if "##" not in personaparts[3]:
                        if i == 0:
                            personaparts[3] = personaparts[3] + "," + "0" + ","
                        else:
                            personaparts[3] = personaparts[3] + "0" + ","
                personaparts[3] = personaparts[3].rstrip(',\n')
                topicpadder = 30
                personaPart9 = []
                if personaparts[9] is not None:
                    personaPart9 = personaparts[9].split(",")
                for i in personaPart9:
                    topicpadder = topicpadder - 1
                if personaparts[9] is None:
                    personaparts[9] = ""
                for i in range(topicpadder):
                    if "##" not in personaparts[9]:
                        if i == 0:
                            personaparts[9] = personaparts[9] + "," + "0" + ","
                        else:
                            personaparts[9] = personaparts[9] + "0" + ","
                personaparts[9] = personaparts[9].rstrip(',\n')
                cinmasegmentpadder = 30
                personaPart4 = []
                if personaparts[4] is not None:
                    personaPart4 = personaparts[4].split(",")
                for i in personaPart4:
                    cinmasegmentpadder = cinmasegmentpadder - 1
                if personaparts[4] is None:
                    personaparts[4] = ""
                for i in range(cinmasegmentpadder):
                    if "##" not in personaparts[4]:
                        if i == 0:
                            personaparts[4] = personaparts[4] + "," + "0" + ","
                        else:
                            personaparts[4] = personaparts[4] + "0" + ","
                personaparts[4] = personaparts[4].rstrip(',\n')
                if cookie_id is None:
                    cookie_id = ""
                if personaparts[0] is None:
                    personaparts[0] = ""
                if personaparts[1] is None:
                    personaparts[1] = ""
                if personaparts[2] is None:
                    personaparts[2] = ""
                if personaparts[3] is None:
                    personaparts[3] = ""
                if personaparts[4] is None:
                    personaparts[4] = ""
                if doc["genderwiki"] is None:
                    doc["genderwiki"] = ""
                if doc["gendertwitter"] is None:
                    doc["gendertwitter"] = ""
                if doc["genderconceptnet"] is None:
                    doc["genderconceptnet"] = ""
                if doc["agegroupwiki"] is None:
                    doc["agegroupwiki"] = ""
                if doc["agegrouptwitter"] is None:
                    doc["agegrouptwitter"] = ""
                if doc["agegroupconceptnet"] is None:
                    doc["agegroupconceptnet"] = ""
                if doc["incomelevelwiki"] is None:
                    doc["incomelevelwiki"] = ""
                if doc["incomeleveltwitter"] is None:
                    doc["incomeleveltwitter"] = ""
                if doc["incomelevelconceptnet"] is None:
                    doc["incomelevelconceptnet"] = ""
                    # if len(personaSignalsbatch) == 4:
                    #   index_batch(personaSignalsbatch)
                    #   personaSignalsbatch.clear()
            except:
                traceback.print_exc()
                continue

        ehcacheData = cookie_id.replace("\n", "") + "@:" + personaparts[0].replace("\n", "") + "@@" + personaparts[
            1].replace("\n", "") + "@@" + personaparts[2].replace("\n", "") + "@@" + personaparts[3].replace(
            "\n", "") + "@@" + personaparts[4].replace("\n", "") + "@@" + mlsignals.replace("\n", "") + "@@" + doc[
                          "genderwiki"].replace(":", ",").replace(".0", "").replace("\n", "") + "," + doc[
                          "gendertwitter"].replace(":", ",").replace(".0", "").replace("\n", "") + "," + doc[
                          "genderconceptnet"].replace(":", ",").replace(".0", "").replace("\n", "") + "@@" + doc[
                          "agegroupwiki"].replace(":", ",").replace(".0", "").replace("\n", "") + "," + doc[
                          "agegrouptwitter"].replace(":", ",").replace(".0", "").replace("\n", "") + "," + doc[
                          "agegroupconceptnet"].replace(":", ",").replace(".0", "").replace("\n", "") + "@@" + doc[
                          "incomelevelwiki"].replace(":", ",").replace(".0", "").replace("\n", "") + "," + doc[
                          "incomeleveltwitter"].replace(":", ",").replace(".0", "").replace("\n", "") + "," + doc[
                          "incomelevelconceptnet"].replace(":", ",").replace(".0", "").replace("\n", "") + "\n"
        print(ehcacheData)
        ehcacheData = ehcacheData.replace("1000", "0")
        ehcacheDatabase.write(ehcacheData)
        personaSignalsbatch.append(doc)

        return


def index_batch(docs):
    #bulk(client, docs, timeout="30s", index=INDEX_NAME)
    # print(docs)
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
    '''
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
    ''' 

##### MAIN SCRIPT #####

if __name__ == '__main__':
    path = sys.argv[1]
    INDEX_NAME = "personaaffinity"
    Enhanced_Persona_Data_File = path + "/enhancedpersonaDatabase.txt"
    Persona_Data_File = path + "/OPD11.txt"
    Emotion_Persona_Data_File = path + "/emotionsPersonaDatabase.txt"
    Demographic_Persona_Data_File11 = path + "/dp11.txt"
    Demographic_Persona_Data_File12 = path + "/dp12.txt"
    Demographic_Persona_Data_File13 = path + "/dp13.txt"
    Demographic_Persona_Data_File21 = path + "/dp21.txt"
    Demographic_Persona_Data_File22 = path + "/dp22.txt"
    Demographic_Persona_Data_File23 = path + "/dp23.txt"
    Demographic_Persona_Data_File31 = path + "/dp31.txt"
    Demographic_Persona_Data_File32 = path + "/dp32.txt"
    Demographic_Persona_Data_File33 = path + "/dp33.txt"
    Persona_Topics_Segment_Affinity_File = path + "/cookietopicsegmentaffinitydatabasev1.txt"
    Persona_Segment_Segment_Affinity_File = path + "/cookietopicsegmentaffinitydatabasev1.txt"
    Full_Persona_Segment_Affinity_File = path + "/personasegmentAffinityDatabaseProcessed.txt"
    Full_Persona_Creative_Affinity_File = path + "/personacreativeAffinityDatabaseProcessed.txt"
    Persona_Topics_Creative_Affinity_File = path + "/cookietopicsegmentaffinitydatabasev1.txt"
    Persona_Segment_Creative_Affinity_File = path + "/cookietopicsegmentaffinitydatabasev1.txt"
    Persona_Color_Codes = path + "/colorcodes.txt"
    #client = Elasticsearch()
    print('Persona Segment Affinity Computation Worker 1')
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
    with open(Persona_Data_File) as f:
        originalPersona = f.readlines()
    with open(Persona_Color_Codes) as f:
        colorcodes = f.readlines()
    BATCH_SIZE = 10000
    userdoccity = {}
    userdoccountry = {}
    userdocmobile = {}
    userdocaffinseg = {}
    userdoccinmaseg = {}
    userdoctopicseg = {}
    userdocsectionseg = {}
    userdocemotionseg = {}
    userdocgenderseg = {}
    userdoccolorseg = {}
    userdocuserid = {}
    userdocfpsa = {}
    userdocfpca = {}
    userdocgenderwiki = {}
    userdocgendertwitter = {}
    userdocgenderconcept = {}
    userdocagegroupwiki = {}
    userdocagegrouptwitter = {}
    userdocagegroupconcept = {}
    userdocincomelevelwiki = {}
    userdocincomeleveltwitter = {}
    userdocincomelevelconcept = {}
    analyticSignals = ""
    personaparts = []
    for line in colorcodes:
        parts = line.split(":")
        colormap[parts[0].strip()] = parts[1].strip()
    for personadata in originalPersona:
        if personadata is not None:
            mainpersonaparts = personadata.split(':@')
            if mainpersonaparts[1] is not None:
                persona = mainpersonaparts[1].rstrip(',\n')
                if persona is not None:
                    personaparts = persona.split('@@');
                cookie = mainpersonaparts[0]
                userdocuserid[cookie] = cookie
                if len(personaparts) > 0:
                    userdoccity[cookie] = personaparts[0]
                if len(personaparts) > 1:
                    userdoccountry[cookie] = personaparts[1]
                if len(personaparts) > 2:
                    userdocmobile[cookie] = personaparts[2]
                sectionpadder = 15
                personaPart6 = []
                personaPart7 = ""
                analyticSignals = ""
                if len(personaparts) > 5 and personaparts[5] is not None:
                    userdocsectionseg[cookie] = personaparts[5].rstrip(',\n')
                else:
                    userdocsectionseg[cookie] = ""
                caffinsegmentpadder = 30
                personaPart3 = []
                if len(personaparts) > 3 and personaparts[3] is not None:
                    userdocaffinseg[cookie] = personaparts[3].rstrip(',\n')
                else:
                    userdocaffinseg[cookie] = ""
                topicpadder = 30
                personaPart9 = []
                if len(personaparts) > 9 and personaparts[9] is not None:
                    userdoctopicseg[cookie] = personaparts[9].rstrip(',\n')
                else:
                    userdoctopicseg[cookie] = ""
                cinmasegmentpadder = 30
                personaPart4 = []
                if len(personaparts) > 4 and personaparts[4] is not None:
                    userdoccinmaseg[cookie] = personaparts[4].rstrip(',\n')
                else:
                    userdoccinmaseg[cookie] = ""
    for e in ep:
        emotionscores = ""
        if e is not None:
            parts1 = e.split('-emotion-')
            #print(e)
            cookie_id = parts1[0]
            if len(parts1) > 1 and parts1[1] is not None:
                emotionsList = parts1[1].rstrip(',\n').split(',')
                for scores in emotionsList:
                    emotionParts = scores.split(':')
                    if len(emotionParts) > 1:
                        # doc[emotionParts[0]] = int(float(emotionParts[1]))
                        emotionscores = emotionscores + str(int(float(emotionParts[1]))) + ","
                        emotionscores = emotionscores.rstrip('\n')
            userdocemotionseg[cookie_id] = emotionscores

    for gw in dc11:
        if gw is not None:
            parts2 = gw.split('-gender-wiki-')
            #print("gender-wiki" + gw)
            if len(parts2) > 1 and parts2[1] is not None:
                demgender = parts2[1].rstrip(',\n')
                userdocgenderwiki[parts2[0]] = demgender
    for aw in dc21:
        if aw is not None:
            parts3 = aw.split('-agegroup-wiki-')
            #print("agegroup-wiki" + aw)
            if len(parts3) > 1 and parts3[1] is not None:
                demag = parts3[1].rstrip(',\n')
                userdocagegroupwiki[parts3[0]] = demag
    for iw in dc31:
        if iw is not None:
            parts4 = iw.split('-incomelevel-wiki-')
            #print("incomelevel-wiki" + iw)
            if len(parts4) > 1 and parts4[1] is not None:
                demil = parts4[1].rstrip(',\n')
                userdocincomelevelwiki[parts4[0]] = demil
    for gt in dc12:
        if gt is not None:
            parts5 = gt.split('-gender-twitter-')
            #print("gender-twitter" + gt)
            if len(parts5) > 1 and parts5[1] is not None:
                demgender = parts5[1].rstrip(',\n')
                userdocgendertwitter[parts5[0]] = demgender
    for at in dc22:
        if at is not None:
            parts6 = at.split('-agegroup-twitter-')
            #print("agegroup-twitter" + at)
            if len(parts6) > 1 and parts6[1] is not None:
                demag = parts6[1].rstrip(',\n')
                userdocagegrouptwitter[parts6[0]] = demag
    for it in dc32:
        if it is not None:
            parts7 = it.split('-incomelevel-twitter-')
            #print("incomelevel-twitter" + it)
            if len(parts7) > 1 and parts7[1] is not None:
                demil = parts7[1].rstrip(',\n')
                userdocincomeleveltwitter[parts7[0]] = demil
    for gc in dc13:
        if gc is not None:
            parts8 = gc.split('-gender-conceptnet-')
            #print("gender-conceptnet" + gc)
            if len(parts8) > 1 and parts8[1] is not None:
                demgender = parts8[1].rstrip(',\n')
                userdocgenderconcept[parts8[0]] = demgender
    for ac in dc23:
        if ac is not None:
            parts9 = ac.split('-agegroup-conceptnet-')
            #print("agegroup-conceptnet" + ac)
            if len(parts9) > 1 and parts9[1] is not None:
                demag = parts9[1].rstrip(',\n')
                userdocagegroupconcept[parts9[0]] = demag
    for ic in dc33:
        if ic is not None:
            parts10 = ic.split('-incomelevel-conceptnet-')
            #print("incomelevel-conceptnet" + ic)
            if len(parts10) > 1 and parts10[1] is not None:
                demil = parts10[1].rstrip(',\n')
                userdocincomelevelconcept[parts10[0]] = demil
    for epersona in enhancedPersona:
        if epersona is not None:
            partscolor = epersona.split(':@')
            if len(partscolor) > 1 and partscolor[1] is not None:
                personacolor = partscolor[1].rstrip(',\n')
                cookie_id_color = partscolor[0]
                colorpadder = 6
                colorcodeslist = ""
            key = ""
            userdoccolorseg[cookie_id_color] = ""
            if personacolor is not None:
                colormapkey = personacolor.split(",")
               # print("Color Persona:" + personacolor)
                if personacolor is not None:
                    colormapkey = personacolor.split(",")
                    if len(colormapkey) > 2:
                        key = colormapkey[len(colormapkey) - 2].strip() + " " + colormapkey[len(colormapkey) - 3]
                    if len(colormapkey) > 1:
                        key = colormapkey[len(colormapkey) - 2].strip()
                    matchingcolorkey = key
                    if len(matchingcolorkey) > 0 and len(cookie_id_color) > 0:
                       userdoccolorseg[cookie_id_color] = colormap[matchingcolorkey.strip()].rstrip(',\n').strip()
                    #print("color map:" + userdoccolorseg[cookie_id_color])
                if personacolor is not None:
                    personacolorparts = personacolor.split('@@');
                    colorpadder = 12
                    colorcodeslist = ""
                    if userdoccolorseg[cookie_id_color] is not None:
                        colorcodes = userdoccolorseg[cookie_id_color].split(",")
                    for colorcode in colorcodes:
                        colorpadder = colorpadder - 1
                        colorcodeslist = colorcodeslist + colorcode + ","
                    for i in range(colorpadder):
                        if i == 0:
                            colorcodeslist = colorcodeslist + "0"
                        else:
                            colorcodeslist = colorcodeslist + "," + "0"
                    if colorcodeslist is not None:
                        colorcodeslist = colorcodeslist.rstrip(',\n')
                    print("color code list:" + colorcodeslist)   
                    userdoccolorseg[cookie_id_color] = colorcodeslist

    #print(fpsa)
    for psa in fpsa:
        if psa is not None:
            parts14 = psa.split(':')
            #print(psa)
            if len(parts14) > 1 and parts14[1] is not None:
                personasegment = parts14[1].rstrip(',\n')
            if len(parts14) > 0 and parts14[0] is not None:
                userdocfpsa[parts14[0]] = personasegment.replace(".0", "")
    #print(fpca)
    for pca in fpca:
        if pca is not None:
            parts15 = pca.split(':')
            #print(pca)
            if len(parts15) > 1 and parts15[1] is not None:
                personacreative = parts15[1].rstrip(',\n')
            if len(parts15) > 0 and parts15[0] is not None:
                userdocfpca[parts15[0]] = personacreative.replace(".0", "")

    for key in userdocuserid.keys():
        #print(key)
        Mlsignalsmarker = True
        Citymarker = ""
        Mobilemarker = ""
        Countrymarker = ""
        AffinitySegmentsmarker = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        InMarketSegmentsmarker = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        EmotionsPadder = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        ColorPadder = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        fpsaPadder = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        fpcaPadder = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        SectionPadder = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        thirdpartydatasetspadder = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        genderwiki = ""
        agegroupwiki = ""
        incomelevelwiki = ""
        gendertwitter = ""
        agegrouptwitter = ""
        incomeleveltwitter = ""
        genderconceptnet = ""
        agegroupconceptnet = ""
        incomelevelconceptnet = ""

        try:
            if key in userdocsectionseg:
                print("Section:")
            else:
                Mlsignalsmarker = False
                userdocsectionseg[key] = SectionPadder
            if key in userdocemotionseg:
                print("Emotion:")
            else:
                Mlsignalsmarker = False
                userdocemotionseg[key] = EmotionsPadder
            if key in userdoccolorseg:
                print("Color:")
            else:
                Mlsignalsmarker = False
                userdoccolorseg[key] = ColorPadder
            if key in userdocfpsa:
                if userdocfpsa[key].count(',') < 99:
                   while True: 
                       userdocfpsa[key] += ",0"
                       if userdocfpsa[key].count(',') == 99:
                          break 
            else:
                Mlsignalsmarker = False
                userdocfpsa[key] = fpsaPadder
            if key in userdocfpca:
                print("")
            else:
                Mlsignalsmarker = False
                userdocfpca[key] = fpcaPadder
            if key in userdoccinmaseg:
                print("")
            else:
                InMarketSegmentsmarker = False
                userdoccinmaseg[key] = InMarketSegmentsmarker
            if key in userdocaffinseg:
                print("")
            else:
                AffinitySegmentsmarker = False
                userdocaffinseg[key] = AffinitySegmentsmarker
            if key in userdoccity:
                print("")
            else:
                Citymarker = False
                userdoccity[key] = ""
            if key in userdoccountry:
                print("")
            else:
                Countrymarker = False
                userdoccountry[key] = ""
            if key in userdocmobile:
                print("")
            else:
                Mobilemarker = False
                userdocmobile[key] = ""
            if key in userdocgenderwiki:
                print("genderwiki")
            else:
                userdocgenderwiki[key] = ""
            if key in userdocagegroupwiki:
                print("agegroupwiki")
            else:
                userdocagegroupwiki[key] = ""
            if key in userdocincomelevelwiki:
                print("incomelevelwiki")
            else:
                userdocincomelevelwiki[key] = ""
            if key in userdocgendertwitter:
                print("gendertwitter")
            else:
                userdocgendertwitter[key] = ""
            if key in userdocagegrouptwitter:
                print("agegrouptwitter")
            else:
                userdocagegrouptwitter[key] = ""
            if key in userdocincomeleveltwitter:
                print("incomeleveltwitter")
            else:
                userdocincomeleveltwitter[key] = ""
            if key in userdocgenderconcept:
                print("genderconcept")
            else:
                userdocgenderconcept[key] = ""
            if key in userdocagegroupconcept:
                print("agegroupconcept")
            else:
                userdocagegroupconcept[key] = ""
            if key in userdocincomelevelconcept:
                print("incomelevelconcept")
            else:
                userdocincomelevelconcept[key] = ""
            if key in userdoctopicseg:
                print(userdoctopicseg[key])
            else:
                userdoctopicseg[key] = ""

            mlsignals = userdocsectionseg[key].rstrip(",") + "," + userdocemotionseg[key].rstrip(",") + "," + \
                        userdoccolorseg[key].rstrip(",") + "," + userdocfpsa[key].rstrip(",") + "," + thirdpartydatasetspadder + "," + userdocfpca[
                            key].rstrip(",")
            print("ML signals:" + mlsignals)

            ehcacheData = userdocuserid[key].replace("\n", "") + ":@" + userdoccity[key].replace("\n", "") + "@@" + \
                          userdoccountry[
                              key].replace("\n", "") + "@@" + userdocmobile[key].replace("\n", "") + "@@" + \
                          userdocaffinseg[key].replace("\n", "") + "@@" + userdoccinmaseg[key].replace("\n",
                                                                                                       "") + "@@" + mlsignals.replace(
                "\n", "") + "@@" + userdocgenderwiki[key].replace(":",
                                                                                                                  ",").replace(
                ".0", "").replace("\n", "") + "," + userdocgendertwitter[
                              key].replace(":", ",").replace(".0", "").replace("\n", "") + "," + userdocgenderconcept[
                              key].replace(":", ",").replace(".0", "").replace("\n",
                                                                               "") + "," + "wikipedia" + "," + "twitter" + "," + "commonsense" + "@@" + \
                          userdocagegroupwiki[
                              key].replace(":", ",").replace(".0", "").replace("\n", "") + "," + userdocagegrouptwitter[
                              key].replace(":", ",").replace(".0", "").replace("\n", "") + "," + \
                          userdocagegroupconcept[
                              key].replace(":", ",").replace(".0", "").replace("\n",
                                                                               "") + "," + "wikipedia" + "," + "twitter" + "," + "commonsense" + "@@" + \
                          userdocincomelevelwiki[
                              key].replace(":", ",").replace(".0", "").replace("\n", "") + "," + \
                          userdocincomeleveltwitter[
                              key].replace(":", ",").replace(".0", "").replace("\n", "") + "," + \
                          userdocincomelevelconcept[
                              key].replace(":", ",").replace(".0", "").replace("\n",
                                                                               "") + "," + "wikipedia" + "," + "twitter" + "," + "commonsense" + "@@" + userdoctopicseg[key].replace("\n", "") + "\n"
            # print(ehcacheData)
            ehcacheData = ehcacheData.replace("100", "0").replace(",,", ",")
            ehcacheDatabase.write(ehcacheData)
        except Exception:
            traceback.print_exc()
            continue

    ehcacheDatabase.close()

    ''' 
    entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    epl = entitysplit(ep, 1)
    dcl11 = entitysplit(dc11, 1)
    dcl21 = entitysplit(dc12, 1)
    dcl31 = entitysplit(dc13, 1)
    dcl12 = entitysplit(dc21, 1)
    dcl22 = entitysplit(dc22, 1)
    dcl32 = entitysplit(dc23, 1)
    dcl13 = entitysplit(dc31, 1)
    dcl23 = entitysplit(dc32, 1)
    dcl33 = entitysplit(dc33, 1)
    ptsal = entitysplit(ptsa, 1)
    pssal = entitysplit(pssa, 1)
    fpsal = entitysplit(fpsa, 1)
    fpcal = entitysplit(fpca, 1)
    ptcal = entitysplit(ptca, 1)
    pscal = entitysplit(psca, 1)
    enhancedPersona = entitysplit(enhancedPersona, 1)
    originalPersona = entitysplit(originalPersona, 1)
    pool = ProcessPool(AffinityScoreMapWorker, 1)
    futures = [pool.submit_job(entity1, entity2a1, entity2b1, entity2c1, entity2a2, entity2b2, entity2c2, entity2a3, entity2b3,entity2c3, entity3, entity4, entity5, entity6, entity7, entity8, entity9, entity10) for entity1, entity2a1, entity2b1, entity2c1, entity2a2, entity2b2, entity2c2, entity2a3, entity2b3, entity2c3, entity3, entity4, entity5, entity6, entity7, entity8, entity9, entity10 in itertools.zip_longest(epl, dcl11, dcl21, dcl31, dcl12, dcl22, dcl32, dcl13, dcl23, dcl33, ptsal, pssal, fpsal, fpcal, ptcal, pscal, enhancedPersona, originalPersona)]
    segments = [f.result() for f in futures]
    
    pool.join()
    '''
