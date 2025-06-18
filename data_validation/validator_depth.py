import json
from datetime import datetime
import logging
from datetime import datetime
from bson import ObjectId
from bson.timestamp import Timestamp
from bson.decimal128 import Decimal128
from bson.int64 import Int64
import os

EXCLUDE_KEYS = {"_key", "_id","_rev","_class","revision","rev","comment","inspectionId"}

class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime, Timestamp)):
            return o.isoformat()
        elif isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, Decimal128):
            return float(o.to_decimal())
        elif isinstance(o, Int64):
            return int(o)
        elif hasattr(o, 'isoformat'):
            try:
                return o.isoformat()
            except Exception:
                return str(o)  # fallback if isoformat fails
        return str(o)

def is_empty_like(x):
    return x is None or x == {} or x == ""

def deep_compare(a, b, path=""):
    mismatches = []

    if isinstance(a, dict) and isinstance(b, dict):
        keys = set(a.keys()).union(b.keys())
        for key in keys:
            if key in EXCLUDE_KEYS:
                continue
            sub_path = f"{path}.{key}" if path else key
            if key not in a:
                mismatches.append({"path": sub_path, "type": "missing_in_arango", "mongo_value": b[key]})
            elif key not in b:
                arango_value = a[key]
                if arango_value in (None,[],{},""):
                    continue
                mismatches.append({"path": sub_path, "type": "missing_in_mongo", "arango_value": arango_value})
            else:
                mismatches.extend(deep_compare(a[key], b[key], sub_path))
    elif isinstance(a, list) and isinstance(b, list):
        for i, (item_a, item_b) in enumerate(zip(a, b)):
            sub_path = f"{path}[{i}]"
            mismatches.extend(deep_compare(item_a, item_b, sub_path))
        if len(a) != len(b):
            mismatches.append({"path": path, "type": "list_length_mismatch", "arango_value": len(a), "mongo_value": len(b)})
    else:
        if isinstance(a,datetime) or isinstance(b,datetime):
           return mismatches
        
        # Skip if one is None/null and the other is missing
        if (a is None and b is None) or (a is None and b == {}) or (a == {} and b is None):
            return mismatches

        #if is_empty_like(a) and is_empty_like(b):
         #   return mismatches
        # Skip if one is None/null and the other is missing from a parent dict
        if a is None and b is not None and b != {}:
            return mismatches
        if b is None and a is not None and a != {}:
            return mismatches
        if a != b:
            mismatches.append({"path": path, "type": "value_mismatch", "arango_value": a, "mongo_value": b})

    return mismatches


def compare_document_sets(arango_map, mongo_map):
    # logging.info("Starting document comparison...")

    missing_in_mongo = []
    missing_in_arango = []
    field_mismatches = []
    
    all_ids = set(arango_map.keys()).union(mongo_map.keys())
    for doc_id in all_ids:
        try:
            a_doc = arango_map.get(doc_id)
            m_doc = mongo_map.get(doc_id)

            if a_doc and not m_doc:
                missing_in_mongo.append(doc_id)
            elif m_doc and not a_doc:
                missing_in_arango.append(doc_id)
            else:
                mismatches = deep_compare(a_doc, m_doc)
                if mismatches:
                    field_mismatches.append({
                        "_id": doc_id,
                        "mismatches": mismatches
                    })
        except Exception as e:
            logging.error("Error while comparing id:%s",doc_id);

    if missing_in_mongo:
        logging.info("Missing in Mongo: %d, ids:%s", len(missing_in_mongo),missing_in_mongo)
    #if missing_in_arango:    
     #   logging.info("Missing in Arango: %d, ids:%s", len(missing_in_arango), missing_in_arango)
    # logging.info("Documents with field mismatches: %d", len(field_mismatches))
    if field_mismatches:
        logging.info("Documents with field mismatches: len:%d ids:%s",len(field_mismatches),[doc["_id"] for doc  in field_mismatches])



    # with open("field_mismatches.json", "w") as f:
    #     json.dump(field_mismatches, f, indent=2, cls=EnhancedJSONEncoder)

    # output_path = os.path.abspath("field_mismatches.json")
    # try:
    #     with open(output_path, "w") as f:
    #         logging.info("writine to json")
    #         json.dump(field_mismatches, f, indent=2, cls=EnhancedJSONEncoder)
    #         f.flush()
    #         os.fsync(f.fileno())
    #     logging.info("Field mismatches written to: %s", output_path)
    # except TypeError as e:
    #     logging.error("Failed to write mismatches to JSON: %s", e)

    return len(missing_in_mongo) ,len(missing_in_arango) ,len(field_mismatches), field_mismatches
    # return missing_in_mongo, missing_in_arango, field_mismatches


logging.basicConfig(
    level=logging.INFO,  # Show INFO and above
    format='%(asctime)s - %(levelname)s - %(message)s'
)




