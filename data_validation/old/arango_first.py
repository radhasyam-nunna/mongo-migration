from pyArango.connection import *
from pymongo import MongoClient
from arango import ArangoClient
from validator_depth import compare_document_sets
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
import sys
import threading
from gc import collect

lock = threading.Lock()

env = sys.argv[1]
dbName = sys.argv[2]
collectionName = sys.argv[3]
logging.info("Validation for env: %s, dbName:%s, collectionName:%s"%(env,dbName,collectionName))

ARANGO_HOST = '****' #ip
ARANGO_HOST_PROD = "****" #ip
ARANGO_DB_SERVICE = "refurb-service-in"
ARANGO_DB_SCHEMA = "refurb-schema-in"
ARANGO_USER = "****" #username
ARANGO_PASS = "****" #password

MONGO_URI = "****"
MONGO_URI_PROD_SCHEMA = "*****"
MONGO_URI_PROD = "*****"
MONGO_DB_SERVICE = "refurb-service"
MONGO_DB_SCHEMA = "refurb-schema"

BATCH_SIZE = 500
NUM_THREADS = 5

arangoHost = ARANGO_HOST
mongoUri = MONGO_URI
if env == "prod":
    arangoHost = ARANGO_HOST_PROD
    if "service" in dbName:
        mongoUri = MONGO_URI_PROD
    else:
        mongoUri = MONGO_URI_PROD_SCHEMA

mongoDbName = MONGO_DB_SERVICE
arangoDbName = ARANGO_DB_SERVICE
if "schema" == dbName:
    mongoDbName = MONGO_DB_SCHEMA
    arangoDbName = ARANGO_DB_SCHEMA

logging.info(f"monogDb:{mongoDbName}, arangoDb:{arangoDbName}, arangoHost:{arangoHost}, mongoHost:{mongoUri}")


logging.info("Started...")
arango_client = ArangoClient(hosts=arangoHost,verify_override=False)
arango_db = arango_client.db(arangoDbName, username=ARANGO_USER, password=ARANGO_PASS)
arango_collection = arango_db.collection(collectionName)

query = f"FOR e IN {collectionName} RETURN e"
arango_cursor = arango_db.aql.execute(
    query=query,
    batch_size=BATCH_SIZE,
    stream=True
)

mongo_client = MongoClient(mongoUri,datetime_conversion="DATETIME_AUTO")
mongo_db = mongo_client[mongoDbName]
mongo_collection = mongo_db[collectionName]

arango_count = arango_collection.count()
mongo_count = mongo_collection.count_documents({})
missing_in_arango = max(0,mongo_count-arango_count)
logging.info(f"Total documents in arango:{arango_collection.count()}, mongo:{mongo_collection.count_documents({})}")


batch = []
summary = {
    "total_count": 0,
    "missing_in_mongo": 0,
    "missing_in_arango": 0,
    "field_mismatches": 0
}


def process_batch(arango_batch):
    doc_ids = [doc["_id"] for doc in arango_batch]

    mongo_batch = list(mongo_collection.find({"_id":{"$in":doc_ids}}))

    arango_batch_map = {doc["_key"]: doc for doc in arango_batch}
    mongo_batch_map = {doc["_id"]: doc for doc in mongo_batch}
    del arango_batch, mongo_batch
    collect()

    mongo_mismatch_count, arango_mismatch_count, field_mismatch_count = compare_document_sets(arango_batch_map, mongo_batch_map)

    return {
        "count": len(doc_ids),
        "missing_in_mongo": mongo_mismatch_count,
        "missing_in_arango": arango_mismatch_count,
        "field_mismatches": field_mismatch_count
    }

# queue threadSize +2
curr_time = datetime.datetime.now()

def waitAndClearFutures(futures ):
    for future in as_completed(futures):
        try:
            result = future.result() 
            summary["total_count"] += result["count"]
            summary["missing_in_mongo"] += result["missing_in_mongo"]
            summary["missing_in_arango"] += result["missing_in_arango"]
            summary["field_mismatches"] += result["field_mismatches"]
            # load the queue +1
        except Exception as e:
            logging.error("Batch execution failed",e)
    futures.clear()

with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    futures = []

    for arango_doc in arango_cursor:
        batch.append(arango_doc)
        if len(batch) >= BATCH_SIZE:
            futures.append(executor.submit(process_batch, batch.copy()))
            batch.clear()

        if len(futures) >= NUM_THREADS * 2:
            waitAndClearFutures(futures)

    # Submit any remaining docs
    if batch:
        futures.append(executor.submit(process_batch, batch.copy()))

    waitAndClearFutures(futures)

summary["missing_in_arango"] +=missing_in_arango
logging.info("summary: %s", summary)
logging.info("Total time taken: %s", datetime.datetime.now()-curr_time )


