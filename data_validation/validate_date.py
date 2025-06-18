import json
from datetime import datetime
import logging
from datetime import datetime
from bson import ObjectId
from bson.timestamp import Timestamp
from bson.decimal128 import Decimal128
from bson.int64 import Int64
import os
import re
from dateutil.parser import parse
from datetime import datetime,timezone


EXCLUDE_KEYS = {"_key", "_id","_rev","_class","revision","rev","archived","appointmentId","partNumber","label","make","inspectionId","workOrderId","vendorInvoiceNo","invoiceId"}

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



DATE_REGEX = re.compile(
    r"""^(
        \d{4}-\d{2}-\d{2}             # YYYY-MM-DD
        [T\s]
        \d{2}:\d{2}:\d{2}             # HH:MM:SS
        (?:\.\d+)?                    # optional milliseconds
        Z?                            # optional Z
        (\[UTC\])?                    # optional [UTC]
    |
        (1\d{9}(?:\.\d{1,12})?|2\d{9}(?:\.\d{1,12})?)
        # \d+(\.\d+)?                   # Unix timestamp (string)
    )$""",
    re.VERBOSE
)

def looks_like_arango_date(value: str) -> bool:
    return isinstance(value, str) and DATE_REGEX.match(value)

def try_parse_datetime(value):
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        cleaned = value.replace("[UTC]", "").strip()

        # Try parsing as ISO 8601 string
        try:
            return parse(cleaned)
        except Exception:
            pass

        # Try parsing as numeric Unix timestamp (in string form)
        try:
            float_val = float(cleaned)
            # Heuristic: if too big, it's likely in milliseconds
            if float_val > 1e12:
                return datetime.fromtimestamp(float_val / 1000)
            else:
                return datetime.fromtimestamp(float_val)
        except Exception:
            return None

    if isinstance(value, (int, float)):
        # Handle numeric Unix timestamp
        return datetime.fromtimestamp(value)

    return None

def dates_equal(dt1, dt2, tolerance_ms=2000):
    if dt1.tzinfo is None:
        dt1 = dt1.replace(tzinfo=timezone.utc)
    if dt2.tzinfo is None:
        dt2 = dt2.replace(tzinfo=timezone.utc)
    # logging.info(f"dates equal arango:{dt1}, mongo:{dt2}")
    return abs((dt1 - dt2).total_seconds() * 1000) <= tolerance_ms

def isDateMatched(arango_value,mongo_value):
    # logging.info("arango: %s, mongo: %s", arango_value, mongo_value)
    if arango_value and looks_like_arango_date(arango_value) :
        #logging.info("its a date")
        # logging.info(f"looks like date arango: {arango_value} mongo:{mongo_value}")
        # return True
        if not mongo_value or isinstance(mongo_value, str):
            # logging.info(f"invalid mongo dates, arango:{arango_value} mongo:{mongo_value}")
            return False
        else:
            parsed = try_parse_datetime(arango_value)
            if parsed and not dates_equal(parsed, mongo_value):
                return False
        # logging.info(f"valid dates, arango:{arango_value} mongo:{mongo_value}")
    return True



def deep_compare(a, b, path=""):
    mismatches = []

    if isinstance(a, dict) and isinstance(b, dict):
        keys = set(a.keys()).union(b.keys())
        for key in keys:
            if key in EXCLUDE_KEYS:
                continue
            sub_path = f"{path}.{key}" if path else key
            if key not in a:
                continue
                # mismatches.append({"path": sub_path, "type": "missing_in_arango", "mongo_value": b[key]})
            elif key not in b:
                continue
                arango_value = a[key]
                if arango_value in (None,[],{}):
                    continue
                mismatches.append({"path": sub_path, "type": "missing_in_mongo", "arango_value": arango_value})
            else:
                mismatches.extend(deep_compare(a[key], b[key], sub_path))
    elif isinstance(a, list) and isinstance(b, list):
        for i, (item_a, item_b) in enumerate(zip(a, b)):
            sub_path = f"{path}[{i}]"
            mismatches.extend(deep_compare(item_a, item_b, sub_path))
        # if len(a) != len(b):
        #     mismatches.append({"path": path, "type": "list_length_mismatch", "arango_value": len(a), "mongo_value": len(b)})
    else:
        if not isDateMatched(a,b):
            # logging.info(f"invalid dates, arango:{a} mongo:{b}")
            mismatches.append({"path": path, "type": "value_mismatch", "arango_value": a, "mongo_value": b})
        # if isinstance(a,datetime) or isinstance(b,datetime):
        #    return mismatches
        
        # # Skip if one is None/null and the other is missing
        # if (a is None and b is None) or (a is None and b == {}) or (a == {} and b is None):
        #     return mismatches

        # # Skip if one is None/null and the other is missing from a parent dict
        # if a is None and b is not None and b != {}:
        #     return mismatches
        # if b is None and a is not None and a != {}:
        #     return mismatches
        # if a != b:
        #     mismatches.append({"path": path, "type": "value_mismatch", "arango_value": a, "mongo_value": b})

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


    # logging.info("Missing in Mongo: %d, ids:%s", len(missing_in_mongo),missing_in_mongo)
    # logging.info("Missing in Arango: %d, ids:%s", len(missing_in_arango), missing_in_arango)
    # logging.info("Documents with field mismatches: %d", len(field_mismatches))
    if field_mismatches:
        logging.info("Documents with field mismatches: len:%d ids:%s",len(field_mismatches),[doc["_id"] for doc  in field_mismatches])



    return len(missing_in_mongo) ,len(missing_in_arango) ,len(field_mismatches), field_mismatches
    # return missing_in_mongo, missing_in_arango, field_mismatches


logging.basicConfig(
    level=logging.INFO,  # Show INFO and above
    format='%(asctime)s - %(levelname)s - %(message)s'
)




