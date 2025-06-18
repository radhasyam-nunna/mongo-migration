from pyArango.connection import *
from pymongo import MongoClient
from arango import ArangoClient
#from validate_date import compare_document_sets,EnhancedJSONEncoder
from validator_depth import compare_document_sets,EnhancedJSONEncoder
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
import sys
from gc import collect
from concurrent.futures import wait, FIRST_COMPLETED
import json
import queue
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bson import ObjectId
from bson.errors import InvalidId

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

BATCH_SIZE = 300
NUM_THREADS = 10
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

query = f"FOR e IN {collectionName}  RETURN e"
#query = f"FOR e IN inspection_view_desc limit 0,5000  RETURN e"
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
logging.info(f"Total documents in arango:{arango_count}, mongo:{mongo_count}")


batch = []
summary = {
    "total_count": 0,
    "missing_in_mongo": 0,
    "missing_in_arango": missing_in_arango,
    "field_mismatches": 0
}

def to_object_id_safe(value):
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        return value

def process_batch(arango_batch):
    doc_ids = [to_object_id_safe(doc["_key"]) for doc in arango_batch]
    count = len(doc_ids)
    mongo_batch = list(mongo_collection.find({"_id":{"$in":doc_ids}}))

    arango_batch_map = {doc["_key"]: doc for doc in arango_batch}
    mongo_batch_map = {str(doc["_id"]): doc for doc in mongo_batch}
    # del arango_batch, mongo_batch, doc_ids
    # collect()

    mongo_mismatch_count, arango_mismatch_count, field_mismatch_count, field_mismatches = compare_document_sets(arango_batch_map, mongo_batch_map)

    return {
        "count": count,
        "missing_in_mongo": mongo_mismatch_count,
        "missing_in_arango": arango_mismatch_count,
        "field_mismatches_count": field_mismatch_count,
        "field_mismatches":field_mismatches
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

def wait_and_fill_futures(executor, futures, batch_gen, summary):
    processed_batches = 0
    
    first_entry = True
    output_path = f"field_mismatches_af_{collectionName}.json"
    with open(output_path, "w") as f:
        f.write("[\n")
        while futures:
            done, not_done = wait(futures, return_when=FIRST_COMPLETED)

            for future in done:
                try:
                    result = future.result()
                    summary["total_count"] += result["count"]
                    summary["missing_in_mongo"] += result["missing_in_mongo"]
                    summary["missing_in_arango"] += result["missing_in_arango"]
                    summary["field_mismatches"] += result["field_mismatches_count"]
                    processed_batches += 1
                    # if processed_batches % 100 == 0:
                    for mismatch in result.get("field_mismatches", []):
                        if not first_entry:
                            f.write(",\n")
                        json.dump(mismatch, f, cls=EnhancedJSONEncoder)
                        # logging.info("written in file")
                        first_entry = False
                    #logging.info("Processed %d batches...", processed_batches)
                    if processed_batches%100==0:
                        logging.info("Processed %d batches***", processed_batches)
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
        f.write("\n]\n") 

batch_gen = batch_generator(arango_cursor, BATCH_SIZE)
futures = []

# logging.info("initial batches %d",len(batch_gen))

# batcheQueue = queue.Queue()
# Initially fill the queue with NUM_THREADS + 2
# for _ in range(NUM_THREADS+BUFFER):
#     batcheQueue.put(next(batch_gen))


for _ in range(NUM_THREADS):
    try:
        futures.append(executor.submit(process_batch, next(batch_gen)))
    except StopIteration:
        break
    logging.info("initial futures loaded: %d",len(futures))

wait_and_fill_futures(executor, futures, batch_gen, summary)


logging.info("summary: %s", summary)
logging.info("Total time taken: %s", datetime.datetime.now()-curr_time )


