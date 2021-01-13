import numpy as np
from cognite.processpool import ProcessPool, WorkerDiedException
import traceback
from elasticsearch import Elasticsearch
from emstore import Emstore

"""
             script = "ctx._source.persona_vector_wiki=personaVector"
             update_body = {
                     "query": {
                         "match": {
                             "cookie_id": personaKey
                         }
                     },
                     "script": {
                       "source": script,
                       "lang": "painless"
                  }
             }
          """


class VectorIngestionWorker:

    def run(self, pcWiki):
        for personaKey, personaValue in pcWiki:
            try:
                personaVector = np.array(personaValue).tolist()
                personaKey = personaKey.replace("wiki", "").replace(" ", "_")
                print("Cookie_Id: ", personaKey)
                query = {
                        "script": {

                        # scripting language name
                        "lang": "painless",

                        # source object (or document ID) as well as the operations performed by the script
                        "source": "ctx._source.persona_vector_wiki = params.persona_vector_wiki",

                        # the parameters or values for the script's operations
                        "params": {
                            "persona_vector_wiki" : personaVector
                        }
                   },

                   "query": {
                    "match": {
                        "cookie_id": personaKey
                    }
                  }
                }
                client = Elasticsearch()
                client.update_by_query(body=query, index='personaaffinity')
        # print("Value updated")
            except:
              traceback.print_exc()
              continue


        return

pcWiki = []

indiatodaywikidb = Emstore('../data/IndiaTodayWikiFullCorpus')
for ck, cv in indiatodaywikidb.__iter__():
    pcWiki.append((ck, cv))
pool = ProcessPool(VectorIngestionWorker, 1)
futures = [pool.submit_job(pcWiki)]
pool.join()
