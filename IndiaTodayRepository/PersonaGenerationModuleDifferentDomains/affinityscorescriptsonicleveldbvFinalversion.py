import numpy as np
from cognite.processpool import ProcessPool, WorkerDiedException
from gensim.models.keyedvectors import KeyedVectors
import gensim
import sys
from json import dumps
import numba
import emstore
from emstore.create import populate_batch_buffer_leveldb
from emstore import Emstore
import threading
import traceback
import memcache
import time
import plyvel
import multiprocessing
import datetime
from collections import defaultdict
from multiprocessing import Pool
import os
from  more_itertools import unique_everseen
import itertools

segsegaffinity = defaultdict(dict)
topicsegaffinity = defaultdict(dict)

modeldatawiki = None
modeldatatwitter = None
modeldataconceptnet = None
modeldatatopics = None

lock = threading.Lock()

staticVectors = {}
fn = "googlenews"
# get average vector for sentence 1
# Read Category Data from Category File
fname = 'emotions.txt'
with open(fname) as f:
    content = f.readlines()

fname = 'gender.txt'
with open(fname) as f:
    gender = f.readlines()

fname = 'agegroup.txt'
with open(fname) as f:
    agegroup = f.readlines()

fname = 'income.txt'
with open(fname) as f:
    income = f.readlines()

epDatabase = open('emotionsPersonaDatabase.txt', "a")
ecDatabase = open('emotionsCreativeDatabase.txt', "a")
dpDatabase = open('demographicPersonaDatabase.txt', "a")
dcDatabase = open('demographicCreativeDatabase.txt', "a")
enhancedpersonaDatabase = open('enhancedpersonaDatabase.txt', "a")
enhancedcreativeDatabase = open('enhancedcreativeDatabase.txt', "a")
affinitywiki = open('affinitywikidatabase.txt', "a")
affinitytwitter = open('affinitytwitterdatabase.txt', "a")
affinityconcept = open('affinityconceptdatabaseDatabase.txt', "a")
topiccreativeaffinitywikiDatabase = open('topiccreativeAffinitywikiDatabase.txt', "a")
segmentcreativeaffinitywikiDatabase = open('segmentcreativeAffinitywikiDatabase.txt', "a")
personacreativeaffinitywikiDatabase = open('personacreativeAffinitywikiDatabase.txt', "a")
topiccreativeaffinitytwitterDatabase = open('topiccreativeAffinitytwitterDatabase.txt', "a")
segmentcreativeaffinitytwitterDatabase = open('segmentcreativeAffinitytwitterDatabase.txt', "a")
personacreativeaffinitywikiDatabase = open('personacreativeAffinitytwitterDatabase.txt', "a")
topiccreativeaffinityconceptnetDatabase = open('topiccreativeAffinityconceptnetDatabase.txt', "a")
segmentcreativeconceptnetDatabase = open('segmentcreativeAffinityconceptnetDatabase.txt', "a")
personacreativeaffinityconceptnetDatabase = open('personacreativeAffinityconceptnetDatabase.txt', "a")
topicsegmentaffinitywikiDatabase = open('topicsegmentAffinityDatabaseversion.txt', "r")
segmentsegmentaffinitywikiDatabase = open('segmentsegmentaffinitydatabase.txt', "r")
personasegmentaffinitywikiDatabase = open('personasegmentAffinitywikiDatabase.txt', "a")
personatopicaffinitywikiDatabase = open('personatopicAffinitywikiDatabase.txt', "a")
topicsegmentaffinitytwitterDatabase = open('topicsegmentAffinitytwitterDatabase.txt', "a")
segmentsegmentaffinitytwitterDatabase = open('segmentsegmentAffinitytwitterDatabase.txt', "a")
personasegmentaffinitytwitterDatabase = open('personasegmentAffinitytwitterDatabase.txt', "a")
miscellaneousDatabase = open('miscellaneousDatabase.txt', "a")


windex2word_set = None
tindex2word_set = None
cindex2word_set = None
personawikibatch = []
personatwitterbatch = []
personaconceptnetbatch = []
creativewikibatch = []
creativetwitterbatch = []
creativeconceptnetbatch = []
cookiewikibatch = []
cookietwitterbatch = []
cookieconceptnetbatch = []
creativeidwikibatch = []
creativeidtwitterbatch = []
creativeidconceptnetbatch = []
emotionDatabaseBuffer = []
demographicDatabaseBuffer = []


def loadModelWikipedia():
    # Connect to database
    global modeldatawiki
    global windex2word_set
    if modeldatawiki is None:
        # Load wikipedia word vectors
        modeldatawiki = KeyedVectors.load_word2vec_format("/mnt/data/root/databases/wiki.en.bin", binary='True',
                                                          unicode_errors='ignore')
        wikiindex2word_set = set(modeldatawiki.wv.index2word)
        windex2word_set = wikiindex2word_set
    print("wiki loaded")
    return modeldatawiki


def loadModelTwitter():
    # Connect to databse
    global modeldatatwitter
    global tindex2word_set
    if modeldatatwitter is None:
        # Load wikipedia word vectors
        modeldatatwitter = KeyedVectors.load_word2vec_format(
            "/mnt/data/root/twitterembeddings/word2vec_twitter_tokens.bin", binary='True', unicode_errors='ignore')
        twitterindex2word_set = set(modeldatatwitter.wv.index2word)
        tindex2word_set = twitterindex2word_set
    print("twitter loaded")
    return modeldatatwitter


def loadModelConceptNet():
    # Connect to databse
    global modeldataconceptnet
    global cindex2word_set
    if modeldataconceptnet is None:
        # Load wikipedia word vectors
        modeldataconceptnet = KeyedVectors.load_word2vec_format("/mnt/data/root/commonsense.bin", binary='True',
                                                                unicode_errors='ignore')
        conceptnetindex2word_set = set(modeldataconceptnet.wv.index2word)
        cindex2word_set = conceptnetindex2word_set
    print("conceptnet loaded")
    return modeldataconceptnet


def binaryfileconverter(file):
    txtfile  = open(file, 'r') 
    mytextstring = txtfile.read()
    binarray = ' '.join(format(ch, 'b') for ch in bytearray(mytextstring))
    binaryfile = open(file, 'br+') 
    binaryfile.write(binarray)

 
mc = memcache.Client(['127.0.0.1:11211'], debug=0)

#def processTopicSegmentDic(line):
    #mc = memcache.Client(['127.0.0.1:11211'], debug=0)
    #topicsegmentaffinitywikiDatabase = open(file, "r") 
    #while(1):
    #        for lines in range(50):
    #            topicsegmentaffinityLines = topicsegmentaffinitywikiDatabase.readline()
#    topicsegmentaffinityLines = topicsegmentaffinitywikiDatabase.readlines()
     #       for topicsegmentaffinity in topicsegmentaffinityLines:
#    topicsegparts = line.split(":")
        #topicsegaffinity[topicsegparts[1]][topicsegparts[2]] = topicsegparts[3]
        #print(topicsegmentaffinity)
#    mc.set(topicsegparts[1]+topicsegparts[2],topicsegparts[3])

#loadTopicSegmentDic()

#loadSegmentSegmentDic()


#print("Start at:", datetime.datetime.now())
#pool = Pool(16)
#pool.map(loadTopicSegmentDic,os.listdir("/mnt/data/root/logdirectory/data"))
#pool.close()
#pool.join()
#print("End at:", datetime.datetime.now())



def process_wrapper(chunkStart, chunkSize):
    with open("/mnt/data/root/logdirectory/data/topicsegmentAffinityDatabaseversion.txt") as f:
        f.seek(chunkStart)
        lines = f.read(chunkSize).splitlines()
        for line in lines:
            processTopicSegmentDic(line)

def chunkify(fname,size=1024*1024):
    fileEnd = os.path.getsize(fname)
    with open(fname,'r') as f:
        chunkEnd = f.tell()
    while True:
        chunkStart = chunkEnd
        f.seek(size,1)
        f.readline()
        chunkEnd = f.tell()
        yield chunkStart, chunkEnd - chunkStart
        if chunkEnd > fileEnd:
            break

#init objects
#pool = Pool(16)
#jobs = []

#create jobs
#for chunkStart,chunkSize in chunkify("/mnt/data/root/logdirectory/data/topicsegmentAffinityDatabaseversion.txt"):
#    jobs.append( pool.apply_async(process_wrapper,(chunkStart,chunkSize)) )

#wait for all jobs to finish
#for job in jobs:
#    job.get()

#clean up
#pool.close()
segmentfilename = "/mnt/data/root/logdirectory/segmentsegmentaffinitydatabase.txt"
filesize = os.path.getsize(segmentfilename)
split_size = 100*1024*1024



topicfilename = "/mnt/data/root/logdirectory/topic_segment_file0.txt"
filesize = os.path.getsize(topicfilename)
split_size = 100*1024*1024


def processSegmentSegmentDic(segmentfilename, start=0, stop=0):
    with open(segmentfilename, 'r') as fh:
         fh.seek(start)
         lines = fh.readlines(stop - start)
         for line in lines:
             segsegparts = line.split(":")
             print('key:'+ (segsegparts[0]+segsegparts[1]+segsegparts[2]).replace(" ","") + 'value:'+ segsegparts[3].replace(".0",""))
             mc.set((segsegparts[0]+segsegparts[1]+segsegparts[2]).replace(" ",""),segsegparts[3].replace(".0",""))
    


def processTopicSegmentDic(topicfilename, start=0, stop=0):
    with open(topicfilename, 'r') as fh:
         fh.seek(start)
         lines = fh.readlines(stop - start)
         for line in lines:
              topicsegparts = line.split(":")
             # print('key:'+ (topicsegparts[0]+topicsegparts[1]+topicsegparts[2]).replace(" ","") + 'value:'+ topicsegparts[3].replace(".0",""))
              mc.set((topicsegparts[0]+topicsegparts[1]+topicsegparts[2]).replace(" ",""),topicsegparts[3].replace(".0",""))


# determine if it needs to be split
def loadTopicSegmentDic():
    if filesize > split_size:

        # create pool, initialize chunk start location (cursor)
       pool = Pool(128)
       cursor = 0
       results = []
       with open(topicfilename, 'r') as fh:

            # for every chunk in the file...
            for chunk in range(filesize // split_size):

                # determine where the chunk ends, is it the last one?
                if cursor + split_size > filesize:
                   end = filesize
                else:
                    end = cursor + split_size

                # seek to end of chunk and read next line to ensure you 
                # continue entire lines to the processfile function
                fh.seek(end)
                fh.readline()

                # get current file location
                end = fh.tell()

                # add chunk to process pool, save reference to get results
                proc = pool.apply_async(processTopicSegmentDic, args=[topicfilename, cursor, end])
                results.append(proc)

                # setup next chunk
                cursor = end

        # close and wait for pool to finish
       pool.close()
       pool.join()

        # iterate through results
       for proc in results:
           processfile_result = proc.get()



# determine if it needs to be split
def loadSegmentSegmentDic():
    if filesize > split_size:

        # create pool, initialize chunk start location (cursor)
       pool = Pool(16)
       cursor = 0
       results = []
       with open(segmentfilename, 'r') as fh:

            # for every chunk in the file...
            for chunk in range(filesize // split_size):

                # determine where the chunk ends, is it the last one?
                if cursor + split_size > filesize:
                   end = filesize
                else:
                    end = cursor + split_size

                # seek to end of chunk and read next line to ensure you
                # continue entire lines to the processfile function
                fh.seek(end)
                fh.readline()

                # get current file location
                end = fh.tell()

                # add chunk to process pool, save reference to get results
                proc = pool.apply_async(processSegmentSegmentDic, args=[segmentfilename, cursor, end])
                results.append(proc)

                # setup next chunk
                cursor = end

        # close and wait for pool to finish
       pool.close()
       pool.join()

        # iterate through results
       for proc in results:
           processfile_result = proc.get()


class VectorComputationWorker:
    def run(self, entityList, entityType):
        if entityType == "Persona":
            for entity in entityList:
                try:
                    entity = entity.replace("\n", "")
                    personaData = entity.split(':@')[1]
                    cookieId = entity.split(':@')[0]
                    twitter_avg_vector = avg_feature_vector(personaData.split(' '), "twitter", tindex2word_set,
                                                            num_features=400)
                    emotion = computeEmotionsAffinity(emotionDatabaseBuffer, entityType, personaData.split(' '),
                                                      cookieId, twitter_avg_vector, "twitter")
                    # entity = entity.rstrip("\n") + "," + emotion
                    wiki_avg_vector = avg_feature_vector(personaData.split(' '), "wiki", windex2word_set,
                                                         num_features=300)
                    twitter_avg_vector = avg_feature_vector(personaData.split(' '), "twitter", tindex2word_set,
                                                            num_features=400)
                    conceptnet_avg_vector = avg_feature_vector(personaData.split(' '), "conceptnet", cindex2word_set,
                                                               num_features=300)
                    genderTwitter = computeGenderAffinity(demographicDatabaseBuffer, entityType, personaData.split(' '),
                                                          cookieId, twitter_avg_vector,
                                                          "twitter")
                 #   print('gent', entity, genderTwitter)
                    genderWiki = computeGenderAffinity(demographicDatabaseBuffer, entityType, personaData.split(' '),
                                                       cookieId, wiki_avg_vector, "wiki")
                 #   print('genw', entity, genderWiki)
                    genderConcept = computeGenderAffinity(demographicDatabaseBuffer, entityType, personaData.split(' '),
                                                          cookieId, conceptnet_avg_vector,
                                                          "conceptnet")
                 #   print('genc', entity, genderConcept)
                    gender = genderTwitter + genderWiki + genderConcept
                    print(gender)
                    if genderTwitter == genderWiki or genderConcept == genderTwitter or genderWiki == genderConcept:
                        if gender > 0:
                            gender = "male"
                        elif gender < 0:
                            gender = "female"
                        else:
                            gender = ""
                    if gender == "":
                        if genderConcept != -2:
                            gender = genderConcept
                        elif genderWiki != -2:
                            gender = genderWiki
                        elif genderTwitter != -2:
                            gender = genderTwitter
                        else:
                            gender = ""

                        if gender != "":
                            if gender > 0:
                                gender = "male"
                            else:
                                gender = "female"

                  #  print("Final Gender", entity, gender)
                    entity = entity.rstrip("\n") + "," + gender
                    wiki_avg_vector = avg_feature_vector(personaData.split(' '), "wiki", windex2word_set,
                                                         num_features=300)
                    twitter_avg_vector = avg_feature_vector(personaData.split(' '), "twitter", tindex2word_set,
                                                            num_features=400)
                    conceptnet_avg_vector = avg_feature_vector(personaData.split(' '), "conceptnet", cindex2word_set,
                                                               num_features=300)
                    ageGroupTwitter = computeagegroupAffinity(demographicDatabaseBuffer, entityType,
                                                              personaData.split(' '), cookieId, twitter_avg_vector,
                                                              "twitter")
                 #   print('agt', entity, ageGroupTwitter)
                    ageGroupWiki = computeagegroupAffinity(demographicDatabaseBuffer, entityType,
                                                           personaData.split(' '),
                                                           cookieId, wiki_avg_vector, "wiki")
                 #   print('agw', entity, ageGroupWiki)
                    ageGroupConcept = computeagegroupAffinity(demographicDatabaseBuffer, entityType,
                                                              personaData.split(' '), cookieId, conceptnet_avg_vector,
                                                              "conceptnet")
                 #   print('agc', entity, ageGroupConcept)
                    agegroup = ""
                    if ageGroupTwitter == ageGroupWiki == ageGroupConcept:
                        agegroup = ageGroupTwitter
                    elif ageGroupTwitter == ageGroupWiki:
                        agegroup = ageGroupTwitter
                    elif ageGroupWiki == ageGroupConcept:
                        agegroup = ageGroupWiki
                    elif ageGroupTwitter == ageGroupConcept:
                        agegroup = ageGroupTwitter
                    else:
                        agegroup = ""

                    if agegroup == "":
                        if ageGroupConcept != "":
                            agegroup = ageGroupConcept
                        elif ageGroupWiki != "":
                            agegroup = ageGroupWiki
                        elif ageGroupTwitter != "":
                            agegroup = ageGroupTwitter
                        else:
                            agegroup = ""

                   # print("Finalag", entity, agegroup)
                    entity = entity.rstrip("\n") + "," + agegroup

                    wiki_avg_vector = avg_feature_vector(personaData.split(' '), "wiki", windex2word_set,
                                                         num_features=300)
                    twitter_avg_vector = avg_feature_vector(personaData.split(' '), "twitter", tindex2word_set,
                                                            num_features=400)
                    conceptnet_avg_vector = avg_feature_vector(personaData.split(' '), "conceptnet", cindex2word_set,
                                                               num_features=300)

                    incomeGroupTwitter = computeincomelevelAffinity(demographicDatabaseBuffer, entityType,
                                                                    personaData.split(' '), cookieId, wiki_avg_vector,
                                                                    "wiki")
                 #   print('igt', entity, incomeGroupTwitter)
                    incomeGroupWiki = computeincomelevelAffinity(demographicDatabaseBuffer, entityType,
                                                                 personaData.split(' '), cookieId, twitter_avg_vector,
                                                                 "twitter")
                 #   print('igw', entity, incomeGroupWiki)
                    incomeGroupConcept = computeincomelevelAffinity(demographicDatabaseBuffer, entityType,
                                                                    personaData.split(' '), cookieId,
                                                                    conceptnet_avg_vector, "conceptnet")
                 #   print('igc', entity, incomeGroupConcept)
                    incomelevel = incomeGroupTwitter + incomeGroupWiki + incomeGroupConcept

                    print("Point2", incomelevel)
                    if incomeGroupTwitter == incomeGroupWiki or incomeGroupConcept == incomeGroupTwitter or incomeGroupWiki == incomeGroupConcept:
                        if incomelevel > 0:
                            incomelevel = "rich"
                        elif incomelevel < 0:
                            incomelevel = "poor"
                        else:
                            incomelevel = "middle income"
                    else:
                        incomelevel = ""

                    if incomelevel == "":
                        if incomeGroupConcept != -2:
                            incomelevel = incomeGroupConcept
                        elif incomeGroupWiki != -2:
                            incomelevel = incomeGroupWiki
                        elif incomeGroupTwitter != -2:
                            incomelevel = incomeGroupTwitter
                        else:
                            incomelevel = ""

                        if incomelevel != "":
                            if incomelevel > 0:
                                incomelevel = "rich"
                            elif incomelevel < 0:
                                incomelevel = "poor"
                            else:
                                incomelevel = "middle income"

                    entity = entity.rstrip("\n") + "," + incomelevel

                   # print("Finalinc", entity, incomelevel)

                    enhancedpersonaDatabase.write(entity + "\n")
                    print("Point4")
                    personawikibatch.append(wiki_avg_vector)
                    print("Point5")
                    personatwitterbatch.append(twitter_avg_vector)
                    print("Point6")
                    personaconceptnetbatch.append(conceptnet_avg_vector)
                    print("Point7")
                    cookiewikibatch.append(cookieId + "wiki")
                    print("Point8")
                    cookietwitterbatch.append(cookieId + "twitter")
                    print("Point9")
                    cookieconceptnetbatch.append(cookieId + "conceptnet")
                    print("Point10")
                    print("List Size", len(personawikibatch))
                    print("Point11")
                    if len(personawikibatch) == 100:
                       
                       for i in range(1,4):
                           i=1
                           try:
                               print('Wiki Vector Batch sent to Emstore')
                       
                               populate_batch_buffer_leveldb(cookiewikibatch, personawikibatch, 300,
                                                          '/mnt/data/root/logdirectory/IndiaTodayWikiFullCorpus')
                               cookiewikibatch.clear()
                               personawikibatch.clear()
                               break
                           except Exception as e:
                               print(e)
                               continue

                    if len(personatwitterbatch) == 100:
                       for i in range(1,4):
                           i=1
                           try:
                              print('Twitter Vector Batch sent to Emstore')
                              populate_batch_buffer_leveldb(cookietwitterbatch, personatwitterbatch, 400,
                                                          '/mnt/data/root/logdirectory/IndiaTodayTwitterFullCorpus')
                              cookietwitterbatch.clear()
                              personatwitterbatch.clear()
                              break
                           except Exception as e:
                               print(e)
                               continue

                    if len(personaconceptnetbatch) == 100:
                       for i in range(1,4):
                           i=1
                           try:
                   
                              print('Conceptnet Vector Batch sent to Emstore')
                        
                              populate_batch_buffer_leveldb(cookieconceptnetbatch, personaconceptnetbatch, 300,
                                                          '/mnt/data/root/logdirectory/IndiaTodayConceptNetFullCorpus')
                              cookieconceptnetbatch.clear()
                              personaconceptnetbatch.clear()
                           except Exception as e:
                               print(e)
                               continue
                               break     
                except Exception:
                    traceback.print_exc()
                    continue

            print('Final Wiki Vector Batch sent to Emstore')
            for i in range(1,4):
                i=1
                try:
                   populate_batch_buffer_leveldb(cookiewikibatch, personawikibatch, 300,
                                              '/mnt/data/root/logdirectory/IndiaTodayWikiFullCorpus')
                   cookiewikibatch.clear()

                   personawikibatch.clear()
                   break
                except Exception as e:
                    print(e)
                    continue

            for i in range(1,4):
                i=1
                print('Final Twitter Vector Batch sent to Emstore')
                try:
                   populate_batch_buffer_leveldb(cookietwitterbatch, personatwitterbatch, 400,
                                              '/mnt/data/root/logdirectory/IndiaTodayTwitterFullCorpus')
                   cookietwitterbatch.clear()
                   personatwitterbatch.clear()
                   break
                except Exception as e:
                    print(e)
                    continue
 
            for i in range(1,4):
                i=1
                print('Final Conceptnet Vector Batch sent to Emstore')
                try:
                   populate_batch_buffer_leveldb(cookieconceptnetbatch, personaconceptnetbatch, 300,
                                              '/mnt/data/root/logdirectory/IndiaTodayConceptNetFullCorpus')
                   cookieconceptnetbatch.clear()
                   personaconceptnetbatch.clear()
                   break
                except Exception as e:
                    print(e)
                    continue

        else:
            try:
                for entity in entityList:
                    entity = entity.replace("\n", "")
                    cpersonaData = entity.split(':@')[1]
                    creativeId = entity.split(':@')[0]
                    twitter_avg_vector = avg_feature_vector(cpersonaData.split(','), "twitter", tindex2word_set,
                                                            num_features=400)
                    emotion = computeEmotionsAffinity(emotionDatabaseBuffer, entityType, cpersonaData.split(','),
                                                      creativeId, twitter_avg_vector, "twitter")
                    # entity = entity.rstrip("\n") + "," + emotion
                    wiki_avg_vector = avg_feature_vector(cpersonaData.split(','), "wiki", windex2word_set,
                                                         num_features=300)
                    twitter_avg_vector = avg_feature_vector(cpersonaData.split(','), "twitter", tindex2word_set,
                                                            num_features=400)
                    conceptnet_avg_vector = avg_feature_vector(cpersonaData.split(','), "conceptnet", cindex2word_set,
                                                               num_features=300)
                    tgender = computeGenderAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                    creativeId, twitter_avg_vector, "twitter")
                    print("Creative", tgender)
                    wgender = computeGenderAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                    creativeId, wiki_avg_vector, "wiki")
                    print("Creative", wgender)
                    cgender = computeGenderAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                    creativeId, conceptnet_avg_vector, "conceptnet")
                    print("Creative", cgender)
                    tag = computeagegroupAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                  creativeId, twitter_avg_vector, "twitter")
                    print("Creative", tag)
                    wag = computeagegroupAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                  creativeId, wiki_avg_vector, "wiki")
                    print("Creative", wag)
                    cag = computeagegroupAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                  creativeId, conceptnet_avg_vector, "conceptnet")
                    print("Creative", cag)
                    tinc = computeincomelevelAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                      creativeId, twitter_avg_vector, "twitter")
                    print("Creative", tinc)
                    winc = computeincomelevelAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                      creativeId, wiki_avg_vector, "wiki")
                    print("Creative", winc)
                    cinc = computeincomelevelAffinity(demographicDatabaseBuffer, entityType, cpersonaData.split(','),
                                                      creativeId, conceptnet_avg_vector, "conceptnet")
                    print("Creative", cinc)
                    enhancedcreativeDatabase.write(cpersonaData + "\n")
                    creativewikibatch.append(wiki_avg_vector)
                    creativetwitterbatch.append(twitter_avg_vector)
                    creativeconceptnetbatch.append(conceptnet_avg_vector)
                    creativeidwikibatch.append(creativeId + "wiki")
                    creativeidtwitterbatch.append(creativeId + "twitter")
                    creativeidconceptnetbatch.append(creativeId + "conceptnet")
                    print(len(creativewikibatch))
                    if len(creativewikibatch) == 1:
                        print(creativewikibatch)
                        print('Wiki Vector Batch sent to Emstore')
                    try:
                        populate_batch_buffer_leveldb(creativeidwikibatch, creativewikibatch, 300,
                                                      '/mnt/data/root/logdirectory/CreativeWikiFullCorpus')
                        creativeidwikibatch.clear()
                        creativewikibatch.clear()
                    except Exception as e:
                        traceback.print_exc()

                    if len(creativetwitterbatch) == 1:
                        print('Twitter Vector Batch sent to Emstore')
                    try:
                        populate_batch_buffer_leveldb(creativeidtwitterbatch, creativetwitterbatch, 400,
                                                      '/mnt/data/root/logdirectory/CreativeTwitterFullCorpus')
                        creativeidtwitterbatch.clear()
                        creativetwitterbatch.clear()
                    except Exception as e:
                        traceback.print_exc()

                    if len(creativeconceptnetbatch) == 1:
                        print('Conceptnet Vector Batch sent to Emstore')
                    try:
                        populate_batch_buffer_leveldb(creativeidconceptnetbatch, creativeconceptnetbatch, 300,
                                                      '/mnt/data/root/logdirectory/CreativeConceptNetFullCorpus')
                        creativeidconceptnetbatch.clear()
                        creativeconceptnetbatch.clear()
                    except Exception as e:
                        traceback.print_exc()
            except Exception as e:
                traceback.print_exc()

        return


class FullPersonaCreativeAffinityComputationWorker:
    def run(self, ecWiki, ecTwitter, ecConcept, pcWiki, pcTwitter, pcConcept):
        affinityWiki = []
        affinityTwitter = []
        affinityConcept = []
        for personaKey, personaValue in pcWiki:
            for creativeKey, creativeValue in ecWiki:
              #  print(creativeValue)
             #   print(personaValue)
                try:
                    creative_persona_similarity = cosine_similarity(np.array(creativeValue), np.array(personaValue))
                    if creative_persona_similarity != 1.0:
                       affinityWiki.append(
                          creativeKey + ":" + personaKey + ":" + str(round(creative_persona_similarity, 3) * 1000))
                       print(creativeKey + ":" + personaKey + ":" + str(round(creative_persona_similarity, 3) * 1000))
                except Exception as e:
                    traceback.print_exc()
                    continue
        for personaKey, personaValue in pcTwitter:
            for creativeKey, creativeValue in ecTwitter:
                try:
                    creative_persona_similarity = cosine_similarity(np.array(creativeValue), np.array(personaValue))
                    if creative_persona_similarity != 1.0:
                       affinityTwitter.append(
                          creativeKey + ":" + personaKey + ":" + str(round(creative_persona_similarity, 3) * 1000))
                except Exception as e:
                    traceback.print_exc()
                    continue
        for personaKey, personaValue in pcConcept:
            for creativeKey, creativeValue in ecConcept:
                try:
                   creative_persona_similarity = cosine_similarity(np.array(creativeValue), np.array(personaValue))
                   if creative_persona_similarity != 1.0:
                      affinityConcept.append(
                         creativeKey + ":" + personaKey + ":" + str(round(creative_persona_similarity, 3) * 1000))
                except Exception as e:
                    traceback.print_exc()
                    continue                   
        populate_batch_buffer("wiki", "affinity", affinityWiki)
        populate_batch_buffer("twitter", "affinity", affinityTwitter)
        populate_batch_buffer("Concept", "Affinity", affinityConcept)

        return


class FullPersonaSegmentAffinityComputationWorker:
    def run(self, segWiki, segTwitter, segConcept, pcWiki, pcTwitter, pcConcept):
        affinityWiki = []
        affinityTwitter = []
        affinityConcept = []
        for personaKey, personaValue in pcWiki:
            for segKey, segValue in segWiki:
                try:
                    seg_persona_similarity = cosine_similarity(np.array(personaValue), np.array(segValue))
                    affinityWiki.append(segKey + ":" + personaKey + ":" + str(round(seg_persona_similarity, 3) * 1000))
                except Exception as e:
                    traceback.print_exc()
                    continue
        for personaKey, personaValue in pcTwitter:
            for segKey, segValue in segTwitter:
                try:
                    seg_persona_similarity = cosine_similarity(np.array(segValue), np.array(personaValue))
                    affinityTwitter.append(
                        segKey + ":" + personaKey + ":" + str(round(seg_persona_similarity, 3) * 1000))
                except Exception as e:
                    traceback.print_exc()
                    continue
        for personaKey, personaValue in pcConcept:
            for segKey, segValue in segConcept:
                seg_persona_similarity = cosine_similarity(np.array(segValue), np.array(personaValue))
                affinityConcept.append(segKey + ":" + personaKey + ":" + str(round(seg_persona_similarity, 3) * 1000))
        populate_batch_buffer("wiki", "affinity", affinityWiki)
        populate_batch_buffer("twitter", "affinity", affinityTwitter)
        populate_batch_buffer("Concept", "Affinity", affinityConcept)

        return


class PersonaTopicAffinityComputationWorker:
    def run(self, entity):
        i = 0
        fname = 'segments.txt'
        with open(fname) as f:
            segmentList = f.readlines()
        segmentData = [x.strip() for x in segmentList]
        topicScoresList = []
        #content1 = [x.strip().split('@@')[9] for x in content]
        #content2 = content1.split(",")
        keys = []
        counter1 = 0
        counter2 = 0
        counter3 = 0
        for z in entity:
            counter1 = counter1 + 1
           # print("Counter1",counter1)
            cookie_id = z.split(':@')[0] 
            semanticscores = cookie_id + "@"
            y = z.strip().split('@@')[9]
            content2 = y.split(",")
            for i in content2:
                    #print(i)
                counter2 = counter2 + 1
            #    print("Counter2",counter2)
            #    semanticscores = cookie_id + "@"  
                for x in segmentData:
                    counter3 = counter3 + 1
            #        print("Counter3",counter3)
                        #print(x)
                        #topicScoresList.append(z.strip().split('@@')[0].split(':@')[0] + mc.get("wiki"+(x+i).replace(" ","")))
                        #print(z.strip().split('@@')[0].split(':@')[0] + mc.get("wiki"+(x+i).replace(" ","")))      
                        #print((x+i).replace(" ",""))
                    keys.append("wiki"+(x+i).replace(" ","")) 
                              #print((x+i).replace(" ",""))
                           #print(mc.get("wiki"+(x+i).replace(" ","")
                counter3 = 0
            result = mc.get_multi(keys)    
            for key in keys:
                if result.get(key) is None:
                   semanticscores = semanticscores + ":" + "0"
                else:
                    semanticscores = semanticscores + ":" + result.get(key).replace("\n","")
            
            topicScoresList.append(semanticscores+"\n")
            keys.clear()    
            counter2 = 0 
        populate_batch_buffer("personatopicwiki", "affinity", topicScoresList)
        topicScoresList.clear()
        return


class PersonaSegmentAffinityComputationWorker:
    def run(self,entity):
        i = 0
        fname = 'segments.txt'
        with open(fname) as f:
            segmentList = f.readlines()
        segmentData = [x.strip() for x in segmentList]
        segmentScoresList = []
        # Personas needs to be transformed with SegCodes replaced with original segment names
        #fname = 'PersonaDatabase.txt'
        #with open(fname) as f:
        #    content = f.readlines()
        #content1 = [','.join(set(x.strip().split('@@')[3] + "," + x.strip().split('@@')[4]).split(',')) for x in
        #            content]
        #content2 = content1.split(",")
        counter1 = 0
        counter2 = 0
        counter3 = 0
        for z in entity:
            counter1 = counter1 + 1
            cookie_id = z.split(':@')[0]
            #print(counter1)
            content1 = list(unique_everseen((z.strip().split('@@')[3] + "," + z.strip().split('@@')[4]).split(',')))
            content2 = itertools.islice(content1, 20)
            for y in content2:
                counter2 = counter2 + 1
                #print(counter2)
                for x in segmentData:
                    try:
                       if mc.get("wiki"+(x+y).replace(" ","")) is not None:
                              #print((x+i).replace(" ",""))
                          #print(mc.get("wiki"+(x+y).replace(" ","")))
                          #print(mc.get("wiki"+(x+y).replace(" ","")))
                          segmentScoresList.append(cookie_id + ":" + mc.get("wiki"+(x+y).replace(" ","")))
                       else:
                           segmentScoresList.append(cookie_id + ":" + "0")                           
                    except Exception as e:
                               #traceback.print_exc()
                           continue
          
  

                  #  segmentScoresList.append(
                        #z.strip().split('@@')[0].split(':@')[0] + str(round(segsegaffinity[y][x], 3) * 1000))
                  #       mc.get("wiki"+(x+y).replace(" ","")))
            
   
            counter2 = 0 
                         
                         
        populate_batch_buffer("personasegmentwiki", "affinity", segmentScoresList)
        return


def scipy_cosine_similarity(v1, v2):
    "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"
    from scipy.spatial.distance import cosine
    return 1 - (cosine(v1, v2))


@numba.jit(target='cpu', nopython=True)
def cosine_similarity(u, v):
    m = u.shape[0]
    udotv = 0
    u_norm = 0
    v_norm = 0
    for i in range(m):
        if (np.isnan(u[i])) or (np.isnan(v[i])):
            continue

        udotv += u[i] * v[i]
        u_norm += u[i] * u[i]
        v_norm += v[i] * v[i]

    u_norm = np.sqrt(u_norm)
    v_norm = np.sqrt(v_norm)

    if (u_norm == 0) or (v_norm == 0):
        ratio = 1.0
    else:
        ratio = udotv / (u_norm * v_norm)
    return ratio


@numba.jit(target='cpu', nopython=True, parallel=True)
def fast_cosine_matrix(u, M):
    scores = np.zeros(M.shape[0])
    for i in numba.prange(M.shape[0]):
        v = M[i]
        m = u.shape[0]
        udotv = 0
        u_norm = 0
        v_norm = 0
        for j in range(m):
            if (np.isnan(u[j])) or (np.isnan(v[j])):
                continue

            udotv += u[j] * v[j]
            u_norm += u[j] * u[j]
            v_norm += v[j] * v[j]

        u_norm = np.sqrt(u_norm)
        v_norm = np.sqrt(v_norm)

        if (u_norm == 0) or (v_norm == 0):
            ratio = 1.0
        else:
            ratio = udotv / (u_norm * v_norm)
        scores[i] = ratio
    return scores


def avg_feature_vector(words, embeddingType, index2word_set, num_features):
    # function to average all words vectors in a given paragraph
    featureVec = np.zeros((num_features,), dtype="float32")
    nwords = 0
    if embeddingType == "wiki":
        model = modeldatawiki
    if embeddingType == "twitter":
        model = modeldatatwitter
    if embeddingType == "conceptnet":
        model = modeldataconceptnet
    # list containing names of words in the vocabulary
    # index2word_set = set(model.index2word) this is moved as input param for performance reasons
    for word in words:
        if word in index2word_set:
            nwords = nwords + 1
            featureVec = np.add(featureVec, model[word])

    if (nwords > 0):
        featureVec = np.divide(featureVec, nwords)
    return featureVec


# One full line of emotions and their scores
# this ensures file is consistant for Process Pool
def computeEmotionsAffinity(emotionDatabaseBuffer, entityType, entity, entityId, sentence_1_avg_vector, embeddingType):
    print('Emotion affinity')
    i = 0
    highestScore = 0

    similarSentence = ""

    fname = 'emotions.txt'
    with open(fname) as f:
        emotion = f.readlines()

    emotionRankings = ""
    content1 = [x.strip() for x in emotion]
    # global modeldata
    # model = modeldata
    for y in content1:
        try:
            i = i + 1
            if embeddingType == "wiki":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector, modeldatawiki[y])
            if embeddingType == "twitter":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector, modeldatatwitter[y])
            if embeddingType == "conceptnet":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector, modeldataconceptnet[y])
            emotionRankings = emotionRankings + y + ":" + str(round(sen1_sen2_similarity, 3) * 1000) + ","
            if sen1_sen2_similarity > highestScore:
                highestScore = sen1_sen2_similarity
                similarSentence = y
        except:
            traceback.print_exc()
            continue
    i = 0
    print('Highest Scoring Emotion:', similarSentence)
    if entityType == "Persona":
        emotionDatabaseBuffer.append(entityId + "-" + "emotion" + "-" + emotionRankings + "\n")
        if len(emotionDatabaseBuffer) == 100:
            populate_batch_buffer("emotion", "Persona", emotionDatabaseBuffer)
            emotionDatabaseBuffer.clear()
    else:
        emotionDatabaseBuffer.append(entityId + "-" + "emotion" + "-" + emotionRankings + "\n")
        print(entityId + "-" + "emotion" + "-" + emotionRankings + "\n")
        populate_batch_buffer("emotion", "Creative", emotionDatabaseBuffer)
        emotionDatabaseBuffer.clear()
    return similarSentence


# One full line of gender and their scores
# this ensures file is consistant for Process Pool
def computeGenderAffinity(demographicDatabaseBuffer, entityType, entity, entityId, sentence_1_avg_vector,
                          embeddingType):
    print('gender affinity')
    i = 0
    highestScore = 0.0
    similarSentence = -2

    if entityType == "Persona":
        highestScoreEntity = 0.15
    else:
        highestScoreEntity = 0.2

    fname = 'gender.txt'
    with open(fname) as f:
        gender = f.readlines()

    genderRankings = ""
    content1 = [x.strip() for x in gender]
    # global modeldata
    # model = modeldata
    for y in content1:
        try:
            y = y.replace("\n", "")
            # entity = entity.replace("\n","")
            i = i + 1
            if embeddingType == "wiki":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "wiki", windex2word_set,
                                                                            num_features=300))
            if embeddingType == "twitter":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "twitter", tindex2word_set,
                                                                            num_features=400))
            if embeddingType == "conceptnet":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "conceptnet", cindex2word_set,
                                                                            num_features=300))
            genderRankings = genderRankings + y + ":" + str(round(sen1_sen2_similarity, 3) * 1000) + ","
            print("gender:", y, entity, sen1_sen2_similarity)
            if sen1_sen2_similarity > highestScore and sen1_sen2_similarity != 1.0:
                highestScore = sen1_sen2_similarity
                print("Highest Score Gender:", y, entity, highestScore)
                if highestScore > highestScoreEntity:
                    similarSentence = y
        except:
            traceback.print_exc()
            continue
    i = 0
    print('Highest Scoring Gender:', similarSentence)
    if entityType == "Persona":
        demographicDatabaseBuffer.append(entityId + "-" + "gender" + "-" + genderRankings + "\n")
        if len(demographicDatabaseBuffer) == 100:
            populate_batch_buffer("gender", "Persona", demographicDatabaseBuffer)
            demographicDatabaseBuffer.clear()
    else:
        demographicDatabaseBuffer.append(entityId + "-" + "gender" + "-" + genderRankings + "\n")     
        print(entityId + "-" + "gender" + "-" + genderRankings + "\n")
        populate_batch_buffer("gender", "Creative", demographicDatabaseBuffer)
        demographicDatabaseBuffer.clear()

    if similarSentence == "male":
        similarSentence = 1
    else:
        similarSentence = -1

    return similarSentence


# One full line of gender and their scores
# this ensures file is consistant for Process Pool
def computeagegroupAffinity(demographicDatabaseBuffer, entityType, entity, entityId, sentence_1_avg_vector,
                            embeddingType):
    print('Age group affinity')
    i = 0
    highestScore = 0.0
    similarSentence = ""

    if entityType == "Persona":
        highestScoreEntity = 0.15
    else:
        highestScoreEntity = 0.2

    fname = 'agegroup.txt'
    with open(fname) as f:
        agegroup = f.readlines()

    agegroupRankings = ""
    content1 = [x.strip() for x in agegroup]
    # global modeldata
    # model = modeldata
    for y in content1:
        try:
            y = y.replace("\n", "")
            # entity = entity.replace("\n","")
            i = i + 1
            if embeddingType == "wiki":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "wiki", windex2word_set,
                                                                            num_features=300))
            if embeddingType == "twitter":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "twitter", tindex2word_set,
                                                                            num_features=400))
            if embeddingType == "conceptnet":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "conceptnet", cindex2word_set,
                                                                            num_features=300))
            agegroupRankings = agegroupRankings + y + ":" + str(round(sen1_sen2_similarity, 3) * 1000) + ","
            print("ag:", y, entity, sen1_sen2_similarity)
            if sen1_sen2_similarity > highestScore and sen1_sen2_similarity != 1.0:
                highestScore = sen1_sen2_similarity
                print("Highest Score Ag:", y, entity, highestScore)
                if highestScore > highestScoreEntity:
                    similarSentence = y
        except:
            traceback.print_exc()
            continue
    i = 0
    print('Highest Scoring Age Group:', similarSentence)
    if entityType == "Persona":
        demographicDatabaseBuffer.append(entityId + "-" + "agegroup" + "-" + agegroupRankings + "\n")
        if len(demographicDatabaseBuffer) == 100:
            populate_batch_buffer("agegroup", "Persona", demographicDatabaseBuffer)
            demographicDatabaseBuffer.clear()
    else:
        demographicDatabaseBuffer.append(entityId + "-" + "agegroup" + "-" + agegroupRankings + "\n")
        print(entityId + "-" + "agegroup" + "-" + agegroupRankings + "\n")
        populate_batch_buffer("agegroup", "Creative", demographicDatabaseBuffer)
        demographicDatabaseBuffer.clear()
    return similarSentence


# One full line of gender and their scores
# this ensures file is consistant for Process Pool
def computeincomelevelAffinity(demographicDatabaseBuffer, entityType, entity, entityId, sentence_1_avg_vector,
                               embeddingType):
    print('income level affinity')
    i = 0
    highestScore = 0.0
    similarSentence = -2

    if entityType == "Persona":
        highestScoreEntity = 0.15
    else:
        highestScoreEntity = 0.2

    fname = 'income.txt'
    with open(fname) as f:
        income = f.readlines()

    incomelevelRankings = ""
    content1 = [x.strip() for x in income]
    # global modeldata
    # model = modeldata
    for y in content1:
        try:
            y = y.replace("\n", "")
            #  entity = entity.replace("\n","")
            i = i + 1
            if embeddingType == "wiki":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "wiki", windex2word_set,
                                                                            num_features=300))
            if embeddingType == "twitter":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "twitter", tindex2word_set,
                                                                            num_features=400))
            if embeddingType == "conceptnet":
                sen1_sen2_similarity = cosine_similarity(sentence_1_avg_vector,
                                                         avg_feature_vector(y.split(), "conceptnet", cindex2word_set,
                                                                            num_features=300))

            incomelevelRankings = incomelevelRankings + y + ":" + str(round(sen1_sen2_similarity, 3) * 1000) + ","
            print("inc:", y, entity, sen1_sen2_similarity)
            if sen1_sen2_similarity > highestScore and sen1_sen2_similarity != 1.0:
                highestScore = sen1_sen2_similarity
                print("Highest Score Inc:", y, entity, highestScore)
                if highestScore > highestScoreEntity:
                    similarSentence = y
        except:
            traceback.print_exc()
            continue

    i = 0
    print('Highest Scoring IncomeLevel:', similarSentence)
    if entityType == "Persona":
        demographicDatabaseBuffer.append(entityId + "-" + "incomelevel" + "-" + incomelevelRankings + "\n")
        if len(demographicDatabaseBuffer) == 100:
            populate_batch_buffer("incomelevel", "Persona", demographicDatabaseBuffer)
            demographicDatabaseBuffer.clear()
    else:
        demographicDatabaseBuffer.append(entityId + "-" + "incomelevel" + "-" + incomelevelRankings + "\n")
        print(entityId + "-" + "incomelevel" + "-" + incomelevelRankings + "\n")
        populate_batch_buffer("incomelevel", "Creative", demographicDatabaseBuffer)
        demographicDatabaseBuffer.clear()

    if similarSentence == "poor":
        similarSentence = -1
    elif similarSentence == "middle income working class":
        similarSentence = 0
    else:
        similarSentence = 1

    return similarSentence


def generateSegmentsVectorFile(model, dimensions, embeddingtype, filename):
    fname = "segments.txt"
    with open(fname) as f:
        segments = f.readlines()
    segmentVectorBatch = []
    segmentBatch = []
    i = 0
    value = None
    global content
    content1 = [x.strip() for x in segments]
    import memcache
    mc = memcache.Client(['127.0.0.1:11211'], debug=0)
    for y in content1:
        sentence_2 = y
        i = i + 1
        k = y.replace(" ", "").replace(",", "").replace("'", "").replace("\n", "")
        try:
            value = mc.get(embeddingtype + k)
        except:
            continue
        if value is not None:
            # print(k + ": Vector Exist")
            flag = 1
        else:

            segment_vector = avg_feature_vector(sentence_2.split(), model, num_features=dimensions)
            segmentBatch.append(k)
            segmentVectorBatch.append(segment_vector)
            # print(k + " " + np.array_str(topic_vector) + "\n")
            try:
                mc.set(embeddingtype + k, segment_vector)
            except:
                continue

            if len(segmentVectorBatch) == 10:
                print('Segment Vector Batch sent to Emstore')
                populate_batch_buffer_leveldb(segmentBatch, segmentVectorBatch,
                                              '/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusWiki')
                segmentBatch.clear()
                segmentVectorBatch.clear()
    return


# count = multiprocessing.Manager().Value(i, 0)
# lock = multiprocessing.Manager().Lock()
count = 0
topicVectorBatch = []
topicBatch = []


class InitTopicVectorComputationWorker:
    def run(self, dimensions, embeddingType, topicList, segWiki, topicsegAffinityWiki, topicBatch, topicVectorBatch):
        print("ProcessList:", len(topicList))
        global count
        t0 = time.time()
        i = 0
        value = None
        content1 = [x.strip() for x in topicList]
        for y in content1:
            try:
                count = count + 1
                print("Topic Processed:", count)
                sentence_2 = y
                if len(sentence_2) > 120:
                    sentence_2 = sentence_2.split()[0]
                    y = sentence_2
                i = i + 1

                k = y.replace(" ", "").replace(",", "").replace("'", "").replace("\n", "").replace("\r", "").lower()
                if value is not None:
                    flag = 1
                else:
                    t00 = time.time()
                    topic_vector = avg_feature_vector(sentence_2.replace("\r", "").replace("\n", " ").lower().split(),
                                                      "wiki", windex2word_set, num_features=dimensions)
                    for segKey, segValue in segWiki.__iter__():
                        try:
                            topic_seg_similarity = cosine_similarity(np.array(segValue), topic_vector)
                            topicsegAffinityWiki.append(
                                "wiki" + ":" + segKey + ":" + sentence_2.replace("\r", "").replace("\n",
                                                                                                   " ").lower() + ":" + str(
                                    round(topic_seg_similarity, 3) * 1000))
                        except Exception as e:
                            traceback.print_exc()
                            continue
                    t01 = time.time()
                    print("Vector Generation Time", t01 - t00)
                    topicBatch.append(k)
                    topicVectorBatch.append(topic_vector)
                    if len(topicVectorBatch) == 16:
                        print('Topic Vector Batch sent to Emstore')
                        try:
                            populate_batch_buffer_leveldb(topicBatch, topicVectorBatch, 300,
                                                          '/mnt/data/root/logdirectory/IndiaTodayTopicFullCorpusWiki')
                            populate_batch_buffer("topicsegwiki", "affinity", topicsegAffinityWiki)
                            topicBatch.clear()
                            topicVectorBatch.clear()
                            topicsegAffinityWiki.clear()
                            t1 = time.time()
                            print("16 Vector Batch Serialisation + Affinity Computation Time", t1 - t0)
                            t0 = time.time()
                        except Exception as e:
                            traceback.print_exc()
            except Exception as e:
                traceback.print_exc()
                continue
        return


class InitTopicVectorComputationWorkerTwitter:
    def run(self, dimensions, embeddingType, topicList, segTwitter, topicsegAffinityTwitter, topicBatch, topicVectorBatch):
        print("ProcessList:", len(topicList))
        global count
        t0 = time.time()
        i = 0
        value = None
        content1 = [x.strip() for x in topicList]
        for y in content1:
            try:
                count = count + 1
                print("Topic Processed:", count)
                sentence_2 = y
                if len(sentence_2) > 120:
                    sentence_2 = sentence_2.split()[0]
                    y = sentence_2
                i = i + 1

                k = y.replace(" ", "").replace(",", "").replace("'", "").replace("\n", "").replace("\r", "").lower()
                if value is not None:
                    flag = 1
                else:
                    t00 = time.time()
                    topic_vector = avg_feature_vector(sentence_2.replace("\r", "").replace("\n", " ").lower().split(),
                                                      "twitter", tindex2word_set, num_features=400)
                    for segKey, segValue in segTwitter.__iter__():
                        try:
                            topic_seg_similarity = cosine_similarity(np.array(segValue), topic_vector)
                            topicsegAffinityTwitter.append(
                                "twitter" + ":" + segKey + ":" + sentence_2.replace("\r", "").replace("\n",
                                                                                                   " ").lower() + ":" + str(
                                    round(topic_seg_similarity, 3) * 1000))
                        except Exception as e:
                            traceback.print_exc()
                            continue
                    t01 = time.time()
                    print("Vector Generation Time", t01 - t00)
                    topicBatch.append(k)
                    topicVectorBatch.append(topic_vector)
                    if len(topicVectorBatch) == 128:
                        print('Topic Vector Batch sent to Emstore')
                        for i in range(1,4):
                            i=1
                            try:    
                                populate_batch_buffer_leveldb(topicBatch, topicVectorBatch, 400,
                                                          '/mnt/data/root/logdirectory/IndiaTodayTopicFullCorpusTwitter')
                                populate_batch_buffer("topicsegtwitter", "affinity", topicsegAffinityTwitter)
                                topicBatch.clear()
                                topicVectorBatch.clear()
                                topicsegAffinityTwitter.clear()
                                t1 = time.time()
                                print("128 Vector Batch Serialisation + Affinity Computation Time", t1 - t0)
                                t0 = time.time()
                                break
                            except Exception as e:
                                #traceback.print_exc() 
                                print("In Exception")
                                continue
            except Exception as e:
                traceback.print_exc()
                continue
        return

                             


class InitTopicCreativeVectorComputationWorker:
    # To do: process Topic vectors in batches
    def run(self, indiatodaycreativeWiki, creativeWiki, topiccreativeaffinityWiki):
        # wikidb = Emstore('/mnt/data/root/logdirectory/IndiaTodayTopicFullCorpusWiki')
        for topicKey, topicValue in indiatodaycreativeWiki:
            for creativeKey, creativeValue in creativeWiki:
                try:
                  # print(creativeKey, creativeValue)
                    topic_creative_similarity = cosine_similarity(np.array(creativeValue), np.array(topicValue))
                    topiccreativeaffinityWiki.append(
                            "wiki" + ":" + creativeKey + ":" + topicKey + ":" + str(round(topic_creative_similarity, 3) * 1000))
                    print("wiki" + ":" + creativeKey + ":" + topicKey + str(round(topic_creative_similarity, 3) * 1000))
                    if len(topiccreativeaffinityWiki) >= 10:
                       print('Topic Creative Affinity Wiki')
                       try:
                          populate_batch_buffer("topiccreativewiki", "affinity", topiccreativeaffinityWiki)
                          topiccreativeaffinityWiki.clear()
                          print("10 Topic Creative Affinity Computation Time")
                       except Exception as e:
                           traceback.print_exc()
                except Exception as e:
                    traceback.print_exc()
                    continue
        return


class InitSegmentCreativeVectorComputationWorker:
    # To do: process Topic vectors in batches
    def run(self, creativeWiki, segcreativeaffinityWiki):
        wikidb = Emstore('/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusWiki')
        for segKey, segValue in wikidb.__iter__():
            for creativeKey, creativeValue in creativeWiki.__iter__():
                try:
                    seg_creative_similarity = cosine_similarity(np.array(creativeValue), np.array(segValue))
                    segcreativeaffinityWiki.append(
                            "wiki" + ":" + creativeKey + ":" + segKey + ":" + str(round(seg_creative_similarity, 3) * 1000))
                except Exception as e:
                    traceback.print_exc()
                    continue
        if len(segcreativeaffinityWiki) >= 3:
           print('Segment Creative Affinity Wiki')
           try:
               populate_batch_buffer("segcreativewiki", "affinity", segcreativeaffinityWiki)
               segcreativeaffinityWiki.clear()
               print("Seg Creative Affinity Computation")
           except Exception as e:
                 traceback.print_exc()
            
        return


class InitSegmentVectorComputationWorker:
    # To do: process Topic vectors in batches
    def run(self, segmentList, segmentBatch, segmentwikiVectorBatch,
            segmenttwitterVectorBatch, segmentconceptnetVectorBatch):
        print("ProcessList:", len(segmentList))
        global count
        t0 = time.time()
        i = 0
        value = None
        #  global content
        content1 = [x.strip() for x in segmentList]
        for y in content1:
            try:
                # print(y)
                count = count + 1
                print("Segment Processed:", count)
                sentence_2 = y
                y = sentence_2
                # print(sentence_2)
                i = i + 1

                k = y.replace(" ", "").replace(",", "").replace("'", "").replace("\n", "").replace("\r", "").lower()
                if value is not None:
                    flag = 1
                else:
                    t00 = time.time()
                    segment_vector_wiki = avg_feature_vector(
                        sentence_2.replace("\r", "").replace("\n", " ").lower().split(),
                        "wiki", windex2word_set, num_features=300)
                    segment_vector_twitter = avg_feature_vector(
                        sentence_2.replace("\r", "").replace("\n", " ").lower().split(),
                        "twitter", tindex2word_set, num_features=400)
                    segment_vector_concept = avg_feature_vector(
                        sentence_2.replace("\r", "").replace("\n", " ").lower().split(),
                        "conceptnet", cindex2word_set, num_features=300)
                    t01 = time.time()
                    print("Vector Generation Time", t01 - t00)
                    segmentBatch.append(k)
                    segmentwikiVectorBatch.append(segment_vector_wiki)
                    segmenttwitterVectorBatch.append(segment_vector_twitter)
                    segmentconceptnetVectorBatch.append(segment_vector_concept)

                    if len(segmentBatch) == 10:
                        print('Topic Vector Batch sent to Emstore')
                        try:
                            populate_batch_buffer_leveldb(segmentBatch, segmentwikiVectorBatch, 300,
                                                          '/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusWiki')
                            populate_batch_buffer_leveldb(segmentBatch, segmenttwitterVectorBatch, 400,
                                                          '/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusTwitter')
                            populate_batch_buffer_leveldb(segmentBatch, segmentconceptnetVectorBatch, 300,
                                                          '/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusConceptNet')
                            segmentBatch.clear()
                            segmentwikiVectorBatch.clear()
                            segmenttwitterVectorBatch.clear()
                            segmentconceptnetVectorBatch.clear()
                            t1 = time.time()
                            print("10 Vector Batch Serialisation Time", t1 - t0)
                            t0 = time.time()
                        except Exception as e:
                            traceback.print_exec()
            except Exception as e:
                traceback.print_exec()
                continue
        return


class SegmentSegmentAffinityVectorComputationWorker:
    # To do: process Topic vectors in batches
    def run(self, dimensions, segWiki, segmentList, segsegAffinityWiki):
        print("ProcessList:", len(segmentList))
        global count
        i = 0
        value = None
        content1 = [x.strip() for x in segmentList]
        for y in content1:
            segment_vector = avg_feature_vector(y.replace("\r", "").replace("\n", " ").lower().split(),
                                                "wiki", windex2word_set, num_features=dimensions)
            for segKey, segValue in segWiki.__iter__():
                try:
                    seg_seg_similarity = cosine_similarity(np.array(segValue), segment_vector)
                    segsegAffinityWiki.append(
                        "wiki" + ":" + segKey + ":" + y.replace("\r", "").replace("\n", " ").lower() + ":" + str(
                            round(seg_seg_similarity, 3) * 1000))
                except Exception as e:
                    traceback.print_exc()
                    continue
        if len(segsegAffinityWiki) >= 6000:
            print('Affinity Batch sent to Database')
            try:
                populate_batch_buffer("segsegwiki", "affinity", segsegAffinityWiki)
                segsegAffinityWiki.clear()
                print("Affinity Batch Written to Database")
            except Exception as e:
                traceback.print_exec()
        return


def populate_batch_buffer(domain, entityType, vectorList):
    global lock
    buffer = []
    lock.acquire()
    buffer.extend(vectorList)
    create_database(entityType, domain, vectorList)
    buffer.clear()
    print("buffer cleared")
    lock.release()
    print("lock released")
    return


def create_database(entityType, domain, buffer):
    if entityType == "Persona" and domain == "emotion":
        affinitydatabase = epDatabase
    elif entityType == "Creative" and domain == "emotion":
        print("Buffer", buffer)
        affinitydatabase = ecDatabase
        print("CD", entityType, domain)
    elif entityType == "Persona" and domain == "gender":
        affinitydatabase = dpDatabase
    elif entityType == "Creative" and domain == "gender":
        print("Buffer", buffer)
        affinitydatabase = dcDatabase
        print("CD", entityType, domain)
    elif entityType == "Persona" and domain == "agegroup":
        affinitydatabase = dpDatabase
    elif entityType == "Creative" and domain == "agegroup":
        print("Buffer", buffer)
        affinitydatabase = dcDatabase
        print("CD", entityType, domain)
    elif entityType == "Persona" and domain == "incomelevel":
        affinitydatabase = dpDatabase
    elif entityType == "Creative" and domain == "incomelevel":
            affinitydatabase = dcDatabase
    elif entityType == "wiki" and domain == "affinity":
        print("Buffer", buffer)
        affinitydatabase = affinitywiki
    elif entityType == "twitter" and domain == "affinity":
        affinitydatabase = affinitytwitter
        print("Buffer", buffer)
    elif entityType == "concept" and domain == "affinity":
        affinitydatabase = affinityconcept
        print("Buffer", buffer)
    elif entityType == "segsegwiki" and domain == "affinity":
        affinitydatabase = segmentsegmentaffinitywikiDatabase
        print("Buffer", buffer)
    elif entityType == "segcreativewiki" and domain == "affinity":
        affinitydatabase = segmentcreativeaffinitywikiDatabase
        print("Buffer", buffer)
    elif entityType == "topicsegwiki" and domain == "affinity":
        affinitydatabase = topicsegmentaffinitywikiDatabase
        print("Buffer", buffer)
    elif entityType == "topiccreativewiki" and domain == "affinity":
        affinitydatabase = topiccreativeaffinitywikiDatabase
        print("Buffer", buffer)
    elif entityType == "personasegmentwiki" and domain == "affinity":
        affinitydatabase = personasegmentaffinitywikiDatabase
        print("Buffer", buffer)
    elif entityType == "personatopicwiki" and domain == "affinity":
        affinitydatabase = personatopicaffinitywikiDatabase
        print("Buffer", buffer)
    else:
        print("Buffer", buffer)
        affinitydatabase = miscellaneousDatabase
        print("CD", entityType, domain)
    for item in buffer:
        print("Database being written")
        affinitydatabase.write("%s\n" % item)
        print("Database has been written")
    return


personafname = 'PersonaDatabase.txt'
with open(personafname) as f:
    personaStore = f.readlines()

creativefname = 'CreativeDatabase.txt'
with open(creativefname) as f:
    creativeStore = f.readlines()

modeldatawiki = loadModelWikipedia()
modeldatatwitter = loadModelTwitter()
modeldataconceptnet = loadModelConceptNet()

scriptindex = sys.argv[1]
print("Script Index", scriptindex)

if scriptindex == '1':
    print('Persona Databases Generation')
    database = ""
    t0 = time.time()
    pool = ProcessPool(VectorComputationWorker, 128)
    entityList = []
    for entity in personaStore:
        entityList.append(entity)
    #partLen = round(len(entityList) / 16)
    #plol = [entityList[o:o + partLen] for o in range(0, len(entityList), partLen)]
    entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    plol = entitysplit(entityList, 128)
    print(len(plol))
    futures = [pool.submit_job(entity, "Persona") for entity in plol]
    personaVectors = [f.result() for f in futures]
    print('Process Pool has completed its routine')
    t1 = time.time()
    print(t1 - t0)
    pool.join()



elif scriptindex == '1a':
    print('Creative Databases Generation')
    database = ""
    t0 = time.time()
    pool = ProcessPool(VectorComputationWorker, 1)
    entityList = []
    for entity in creativeStore:
        entityList.append(entity)
    #partLen = round(len(entityList) / 16)
    #plol = [entityList[o:o + partLen] for o in range(0, len(entityList), partLen)]
    entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    plol = entitysplit(entityList, 1)
    print(len(plol))
    futures = [pool.submit_job(entity, "Creative") for entity in plol]
    creativeVectors = [f.result() for f in futures]
    print('Process Pool has completed its routine')
    t1 = time.time()
    print(t1 - t0)
    pool.join()



elif scriptindex == '2':
    print('Topic Vector Generation')
    segwikidb = Emstore('/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusWiki')
    segWiki = []
    topicsegAffinityWiki = []
    for ck, cv in segwikidb.__iter__():
        segWiki.append((ck, cv))
    try:
        fname = "Topics2.txt"
        print(fname)
        with open(fname) as f:
            topics = f.readlines()
        entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
        plol = entitysplit(topics, 16)
        print(len(plol))
        pool = ProcessPool(InitTopicVectorComputationWorker, 16)
        print('Process Pool Initialised')
        futures = [pool.submit_job(300, "wiki", entity, segWiki, topicsegAffinityWiki, topicBatch, topicVectorBatch) for entity in plol]
        topicVectors = [f.result for f in futures]
        pool.join()
    except:
        traceback.print_exc()



elif scriptindex == '2a':
    print('Topic Vector Generation')
    segtwitterdb = Emstore('/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusTwitter')
    segTwitter = []
    topicsegAffinityTwitter = []
    for ck, cv in segtwitterdb.__iter__():
        segTwitter.append((ck, cv))
    segtwitterdb.close()
    try:
        fname = "Topicsv10.txt"
        print(fname)
        with open(fname) as f:
            topics = f.readlines()
        entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
        plol = entitysplit(topics, 128)
        print(len(plol))
        pool = ProcessPool(InitTopicVectorComputationWorkerTwitter, 128)
        print('Process Pool Initialised')
        futures = [pool.submit_job(400, "twitter", entity, segTwitter, topicsegAffinityTwitter, topicBatch, topicVectorBatch) for entity in plol]
        topicVectors = [f.result for f in futures]
        pool.join()
    except:
        traceback.print_exc()
         

elif scriptindex == '3':
    print('Segment Vector Computation Worker')
    database = ""
    pool = ProcessPool(InitSegmentVectorComputationWorker, 1)
    fname = "Segments.txt"
    print(fname)
    with open(fname) as f:
        segmentList = f.readlines()
    segmentBatch = []
    segmentwikiVectorBatch = []
    segmenttwitterVectorBatch = []
    segmentconceptnetVectorBatch = []
    futures = [pool.submit_job(segmentList, segmentBatch, segmentwikiVectorBatch,
                               segmenttwitterVectorBatch, segmentconceptnetVectorBatch)]
    segments = [f.result() for f in futures]
    pool.join()

elif scriptindex == '4':
    print('Segment Segment Vector Computation Worker')
    database = ""
    segsegAffinityWiki = []
    segwikidb = Emstore('/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusWiki')
    segWiki = []
    topicsegAffinityWiki = []
    for ck, cv in segwikidb.__iter__():
        segWiki.append((ck, cv))
    pool = ProcessPool(SegmentSegmentAffinityVectorComputationWorker, 1)
    fname = "segments1.txt"
    print(fname)
    with open(fname) as f:
        segmentList = f.readlines()
    futures = [pool.submit_job(300, segWiki, segmentList, segsegAffinityWiki)]
    segments = [f.result() for f in futures]
    pool.join()

elif scriptindex == '5':
    print('Topic Creative Vector Computation Worker')
    database = ""
    segsegAffinityWiki = []
    pool = ProcessPool(InitTopicCreativeVectorComputationWorker, 16)
    fname = "Segments.txt"
    print(fname)
    creativewikidb = Emstore('/mnt/data/root/logdirectory/CreativeWikiFullCorpus')
    creativeWiki = []
    topicsegAffinityWiki = []
    for ck, cv in creativewikidb.__iter__():
        creativeWiki.append((ck, cv))
    indiatodaycreativewiki = []
    topiccreativeaffinityWiki = []
    # creativewikidb = Emstore('/mnt/data/root/logdirectory/CreativeWikiFullCorpus')
    indiatodaycreativewikidb = Emstore('/mnt/data/root/logdirectory/IndiaTodayTopicFullCorpusWiki')
    #for ck, cv in creativewikidb.__iter__():
    #    creativeWiki.append((ck, cv))
    i = 0
    for itk,itv in indiatodaycreativewikidb.__iter__():
        i = i + 1
        print(i)
        indiatodaycreativewiki.append((itk, itv))
    entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    plol = entitysplit(indiatodaycreativewiki, 16)
    print(len(plol))
    futures = [pool.submit_job(entity, creativeWiki, topiccreativeaffinityWiki) for entity in plol]
    segments = [f.result() for f in futures]
    pool.join()

elif scriptindex == '6':
    print('Segment Creative Vector Computation Worker')
    database = ""
    pool = ProcessPool(InitSegmentCreativeVectorComputationWorker, 1)
    segmentcreativeaffinityWiki = []
    creativeWiki = []
    creativewikidb = Emstore('/mnt/data/root/logdirectory/CreativeWikiFullCorpus')
    for ck, cv in creativewikidb.__iter__():
        creativeWiki.append((ck, cv))
    futures = [pool.submit_job(creativeWiki, segmentcreativeaffinityWiki)]
    segments = [f.result() for f in futures]
    pool.join()


elif scriptindex == '7':
    print('Persona Segment Affinity Computation Worker')
    database = ""
    fname = 'split_file_datav00.txt'
    with open(fname) as f:
         content = f.readlines()
    entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    plol = entitysplit(content, 128)
    pool = ProcessPool(PersonaSegmentAffinityComputationWorker, 128)
    futures = [pool.submit_job(entity) for entity in plol]
    segments = [f.result() for f in futures]
    pool.join()

elif scriptindex == '8':
    print('Persona Topic Affinity Computation Worker')
    database = ""
    fname = 'split_file_datav00.txt'
    with open(fname) as f:
         content = f.readlines()
    entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    plol = entitysplit(content, 128)
    pool = ProcessPool(PersonaTopicAffinityComputationWorker, 128)
    futures = [pool.submit_job(entity) for entity in plol]   
    segments = [f.result() for f in futures]
    pool.join()

elif scriptindex == '9':
    database = ""
    wikidb = Emstore('/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusWiki')
    twitterdb = Emstore('/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusTwitter')
    conceptdb = Emstore('/mnt/data/root/logdirectory/IndiaTodaySegmentFullCorpusConceptNet')
    ecWiki = []
    ecTwitter = []
    ecConcept = []
    entityList = []
    for ck, cv in wikidb.__iter__():
        # print(ck,cv)
        ecWiki.append((ck, cv))
    for ck, cv in twitterdb.__iter__():
        ecTwitter.append((ck, cv))
    for ck, cv in conceptdb.__iter__():
        ecConcept.append((ck, cv))
    indiatodaywikidb = Emstore('/mnt/data/root/logdirectory/IndiaTodayWikiFullCorpus')
    indiatodaytwitterdb = Emstore('/mnt/data/root/logdirectory/IndiaTodayTwitterFullCorpus')
    indiatodayconceptdb = Emstore('/mnt/data/root/logdirectory/IndiaTodayConceptNetFullCorpus')
    pcWiki = []
    pcTwitter = []
    pcConcept = []
    for ck, cv in indiatodaywikidb.__iter__():
        pcWiki.append((ck, cv))
    for ck, cv in indiatodaytwitterdb.__iter__():
        pcTwitter.append((ck, cv))
    for ck, cv in indiatodayconceptdb.__iter__():
        pcConcept.append((ck, cv))

    pool = ProcessPool(FullPersonaSegmentAffinityComputationWorker, 1)
    futures = [pool.submit_job(ecWiki, ecTwitter, ecConcept, pcWiki, pcTwitter, pcConcept)]
    pool.join()

elif scriptindex == '10':
    database = ""
    wikidb = Emstore('/mnt/data/root/logdirectory/CreativeWikiFullCorpus')
    twitterdb = Emstore('/mnt/data/root/logdirectory/CreativeTwitterFullCorpus')
    conceptdb = Emstore('/mnt/data/root/logdirectory/CreativeConceptNetFullCorpus')
    ecWiki = []
    ecTwitter = []
    ecConcept = []
    entityList = []
    for ck, cv in wikidb.__iter__():
        # print(ck,cv)
        ecWiki.append((ck, cv))
    for ck, cv in twitterdb.__iter__():
        ecTwitter.append((ck, cv))
    for ck, cv in conceptdb.__iter__():
        ecConcept.append((ck, cv))
    indiatodaywikidb = Emstore('/mnt/data/root/logdirectory/IndiaTodayWikiFullCorpus')
    indiatodaytwitterdb = Emstore('/mnt/data/root/logdirectory/IndiaTodayTwitterFullCorpus')
    indiatodayconceptdb = Emstore('/mnt/data/root/logdirectory/IndiaTodayConceptNetFullCorpus')
    pcWiki = []
    pcTwitter = []
    pcConcept = []
    for ck, cv in indiatodaywikidb.__iter__():
        pcWiki.append((ck, cv))
    for ck, cv in indiatodaytwitterdb.__iter__():
        pcTwitter.append((ck, cv))
    for ck, cv in indiatodayconceptdb.__iter__():
        pcConcept.append((ck, cv))
    pool = ProcessPool(FullPersonaCreativeAffinityComputationWorker, 1)
    futures = [pool.submit_job(ecWiki, ecTwitter, ecConcept, pcWiki, pcTwitter, pcConcept)]
    pool.join()


elif scriptindex == '11':
     loadSegmentSegmentDic()

elif scriptindex == '12':
     loadTopicSegmentDic()

else:
    print("Invalid Argument")

epDatabase.close()
ecDatabase.close()
dpDatabase.close()
dcDatabase.close()
enhancedpersonaDatabase.close()
enhancedcreativeDatabase.close()
topiccreativeaffinitywikiDatabase.close()
segmentcreativeaffinitywikiDatabase.close()
personacreativeaffinitywikiDatabase.close()
topiccreativeaffinitytwitterDatabase.close()
segmentcreativeaffinitytwitterDatabase.close()
personacreativeaffinitywikiDatabase.close()
topiccreativeaffinityconceptnetDatabase.close()
segmentcreativeconceptnetDatabase.close()
personacreativeaffinityconceptnetDatabase.close()
topicsegmentaffinitywikiDatabase.close()
segmentsegmentaffinitywikiDatabase.close()
personasegmentaffinitywikiDatabase.close()
topicsegmentaffinitytwitterDatabase.close()
segmentsegmentaffinitytwitterDatabase.close()
personasegmentaffinitytwitterDatabase.close()
