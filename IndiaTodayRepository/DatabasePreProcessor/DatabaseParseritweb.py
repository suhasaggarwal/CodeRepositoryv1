from cognite.processpool import ProcessPool, WorkerDiedException
import sys
import time
import traceback
#userIdSegmentAffinityMap = {}
#userIdCreativeAffinityMap = {}

#personasegmentaffinityDatabase = open('personasegmentAffinityDatabaseProcessed.txt', "a")
#personacreativeaffinityDatabase = open('personacreativeAffinityDatabaseProcessed.txt', "a")
def PersonaSegmentDatabaseProcessorWorker(entity):
    userIdSegmentAffinityMap = {}
    for z in entity:
        try:
           if z is not None:
              entityParts = z.split(":")
              if len(entityParts) > 1:
                 entityParts[0] = entityParts[0].replace("wiki", "").replace("twitter","").replace("conceptnet","")
                 entityParts[2] = entityParts[2]
                 if entityParts[0] not in userIdSegmentAffinityMap:
                     userIdSegmentAffinityMap[entityParts[0]] = entityParts[2].replace("100.0","0").replace("0.0","0").replace(".0","").replace("\n", "")
                 else:
                     userIdSegmentAffinityMap[entityParts[0]] = userIdSegmentAffinityMap[entityParts[0]] + "," + entityParts[2].replace("100.0","0").replace("0.0","0").replace(".0","").replace("\n", "")
        except Exception:
            traceback.print_exc()
            continue
    return userIdSegmentAffinityMap


def PersonaCreativeDatabaseProcessorWorker(entity):
    userIdCreativeAffinityMap = {}
    for z in entity:
        try:
           if z is not None:
              entityParts = z.split(":")
              if len(entityParts) > 1:
                 entityParts[0] = entityParts[0].replace("wiki", "").replace("twitter","").replace("conceptnet","")
                 entityParts[2] = entityParts[2]
                 if entityParts[0] not in userIdCreativeAffinityMap:
                     userIdCreativeAffinityMap[entityParts[0]] = entityParts[2].replace("100.0","0").replace("0.0","0").replace(".0","").replace("\n", "")
                 else:
                     userIdCreativeAffinityMap[entityParts[0]] = userIdCreativeAffinityMap[entityParts[0]] + "," + entityParts[2].replace("100.0","0").replace("0.0","0").replace(".0","").replace("\n", "")
        except Exception:
            traceback.print_exc()
            continue
    return userIdCreativeAffinityMap

scriptindex = sys.argv[1]
path = sys.argv[2]
print("Script Index", scriptindex)

if scriptindex == '1':
    print('Persona Segment Database Parser')
    database = ""
    personasegmentaffinityDatabase = open(path + 'personasegmentAffinityDatabaseProcesseditweb.txt', "a")
    t0 = time.time()
    #pool = ProcessPool(PersonaSegmentDatabaseProcessorWorker, 1)
    #entityList = []
    fname = path + "segmentPersonaaffinityDatabaseitweb.txt"
    with open(fname) as f:
        content = f.readlines()
    #size = int(len(content)/1)
    # partLen = round(len(entityList) / 16)
    # plol = [entityList[o:o + partLen] for o in range(0, len(entityList), partLen)]
    #entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    #plol = entitysplit(content, size)
    #print(plol)
    #futures = [pool.submit_job(entity) for entity in plol]
    resultantDict = {}
    resultantDict = PersonaSegmentDatabaseProcessorWorker(content)
    #for d in personaSegmentDictionaries:
    #    resultantDict.update(d)
    FinalList = sorted(resultantDict.items(), key=lambda kv: (kv[1], kv[0]))
    for key, value in FinalList:
        #print(key + ":" + value)
        personasegmentaffinityDatabase.write(key.replace(" ","_") + ":" + value + "\n")
    print('Process has completed its routine')
    personasegmentaffinityDatabase.close()
    t1 = time.time()
    print(t1 - t0)
    #pool.join()



elif scriptindex == '2':
    print('Persona Creative Database Parser')
    database = ""
    personacreativeaffinityDatabase = open(path + 'personacreativeAffinityDatabaseProcesseditweb.txt', "a")
    t0 = time.time()
    #pool = ProcessPool(PersonaCreativeDatabaseProcessorWorker, 1)
    #entityList = []
    fname = path + "personacreativeaffinityDatabaseitweb.txt"
    with open(fname) as f:
        content = f.readlines()
    #size = int(len(content) / 1)
    # partLen = round(len(entityList) / 16)
    # plol = [entityList[o:o + partLen] for o in range(0, len(entityList), partLen)]
    #entitysplit = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
    #plol = entitysplit(content, size)
    #print(len(plol))
    #futures = [pool.submit_job(entity) for entity in plol]
    #personaCreativeDictionaries = [f.result() for f in futures]
    resultantDict = {}
    resultantDict = PersonaCreativeDatabaseProcessorWorker(content)
    #for d in personaCreativeDictionaries:
    #    resultantDict.update(d)
    FinalList = sorted(resultantDict.items(), key=lambda kv: (kv[1], kv[0]))
    for key, value in FinalList:
        #print(key + ":" + value)
        personacreativeaffinityDatabase.write(key.replace(" ","_") + ":" + value + "\n")
    print('Process has completed its routine')
    personacreativeaffinityDatabase.close()
    t1 = time.time()
    print(t1 - t0)
    #pool.join()

