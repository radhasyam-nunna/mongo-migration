from pyArango.connection import *
from pymongo import MongoClient
from arango import ArangoClient
from validator_depth import compare_document_sets
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
import sys
import threading

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
client = ArangoClient(hosts=arangoHost,verify_override=False)
sys_db = client.db(arangoDbName, username=ARANGO_USER, password=ARANGO_PASS)
arango_collection = sys_db.collection(collectionName)

client = MongoClient(mongoUri,datetime_conversion="DATETIME_AUTO")
mongo_db = client[mongoDbName]
mongo_collection = mongo_db[collectionName]

logging.info(f"Total documents in arango:{arango_collection.count()}, mongo:{mongo_collection.count_documents({})}")

BATCH_SIZE = 500
NUM_THREADS = 5
batch = []
count=0
mismatched_count=0
total_missing_in_mongo = 0
total_missing_in_arango = 0
total_field_mismatches = 0

def process_batch(batch):
    doc_ids = [doc["_id"] for doc in batch]
    # print(doc_ids)

    # Fetch Arango docs for the batch
    arango_cursor = sys_db.aql.execute(
        f"FOR doc IN {collectionName} FILTER doc._key IN @keys RETURN doc",
        bind_vars={"keys": doc_ids}
    )
    arango_batch_map = {doc["_key"]: doc for doc in arango_cursor}
    mongo_batch_map = {doc["_id"]: doc for doc in batch}
    mongo_mismatch_count, arango_mismatch_count, field_mismatch_count = compare_document_sets(arango_batch_map, mongo_batch_map)

    with lock:
        global count,total_missing_in_mongo,total_missing_in_arango,total_field_mismatches
        count+=len(doc_ids)
        total_missing_in_mongo += mongo_mismatch_count
        total_missing_in_arango += arango_mismatch_count
        total_field_mismatches += field_mismatch_count


curr_time = datetime.datetime.now()

def waitAndClearFutures(futures ):
    for future in as_completed(futures):
        try:
            future.result() 
        except Exception as e:
            logging.error("Batch execution failed",e)
    futures.clear()

with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    futures = []

    for mongo_doc in mongo_collection.find({}, batch_size=BATCH_SIZE):
        batch.append(mongo_doc)

        if len(batch) >= BATCH_SIZE:
            # Submit batch to thread pool
            futures.append(executor.submit(process_batch, batch.copy()))
            batch.clear()
        
        if len(futures) >= NUM_THREADS * 2:
            waitAndClearFutures(futures)


    # Submit any remaining docs
    if batch:
        futures.append(executor.submit(process_batch, batch.copy()))

    waitAndClearFutures(futures)
    # # Optional: wait for all threads to finish
    # for future in as_completed(futures):
    #     future.result()  

logging.info("Missing in Mongo: %d", total_missing_in_mongo)
logging.info("Missing in Arango: %d", total_missing_in_arango)
logging.info("Documents with field mismatches: %d", total_field_mismatches)

logging.info(f"data validation completed for the collection:{collectionName} count:{count}")
logging.info("Total time taken: %s", datetime.datetime.now()-curr_time )


