from pyArango.connection import *
from pymongo import MongoClient
from arango import ArangoClient
from data_validation.validate_date import compare_document_sets,EnhancedJSONEncoder
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
import sys
from gc import collect
from concurrent.futures import wait, FIRST_COMPLETED
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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

BATCH_SIZE = 100
NUM_THREADS = 5
BUFFER = 2

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

# mongo_cursor = mongo_collection.find({}, batch_size=BATCH_SIZE)

arango_count = arango_collection.count()
mongo_count = mongo_collection.count_documents({})
missing_in_arango = max(0,mongo_count-arango_count)
logging.info(f"Total documents in arango:{arango_count}, mongo:{mongo_count}")


batch = []
summary = {
    "total_count": 0,
    "missing_in_mongo": 0,
    "missing_in_arango": missing_in_arango,
    "field_mismatches": 0
}


def process_batch(arango_batch):
    doc_ids = [doc["_key"] for doc in arango_batch]
    count = len(doc_ids)
    mongo_batch = list(mongo_collection.find({"_id":{"$in":doc_ids}}))

    arango_batch_map = {doc["_key"]: doc for doc in arango_batch}
    mongo_batch_map = {doc["_id"]: doc for doc in mongo_batch}
    del arango_batch, mongo_batch, doc_ids
    collect()

    mongo_mismatch_count, arango_mismatch_count, field_mismatch_count = compare_document_sets(arango_batch_map, mongo_batch_map)

    return {
        "count": count,
        "missing_in_mongo": mongo_mismatch_count,
        "missing_in_arango": arango_mismatch_count,
        "field_mismatches": field_mismatch_count
    }

curr_time = datetime.datetime.now()

def batch_generator(cursor, batch_size):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

executor = ThreadPoolExecutor(max_workers=NUM_THREADS)

def wait_and_fill_futures(executor, futures, batch_gen, summary, max_futures):
    processed_batches = 0
    
    while futures:
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)

        for future in done:
            try:
                result = future.result()
                summary["total_count"] += result["count"]
                summary["missing_in_mongo"] += result["missing_in_mongo"]
                summary["missing_in_arango"] += result["missing_in_arango"]
                summary["field_mismatches"] += result["field_mismatches"]
                processed_batches += 1
                # if processed_batches % 100 == 0:
                logging.info("Processed %d batches...", processed_batches)
            except Exception as e:
                logging.exception("Batch execution failed", exc_info=e)

            # Immediately replace finished future with a new batch
            try:
                next_batch = next(batch_gen)
                new_future = executor.submit(process_batch, next_batch)
                not_done.add(new_future)
            except StopIteration:
                pass  # No more batches to submit

        futures = list(not_done)

batch_gen = batch_generator(arango_cursor, BATCH_SIZE)
futures = []

# Initially fill the queue with NUM_THREADS + 2
for _ in range(NUM_THREADS + 2):
    try:
        batch = next(batch_gen)
        futures.append(executor.submit(process_batch, batch))
    except StopIteration:
        break

wait_and_fill_futures(executor, futures, batch_gen, summary, NUM_THREADS + 2)


logging.info("summary: %s", summary)
logging.info("Total time taken: %s", datetime.datetime.now()-curr_time )


