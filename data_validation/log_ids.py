import re
import json

all_ids = []
fileName="logs/validation_inspection_dump.log"
#fileName="logs/validation_work_order.log"
print("file name:",fileName)
with open(fileName, 'r') as file:
    for line in file:
        match = re.search(r"ids:\[(.*?)\]", line)
        if match:
            # Remove quotes and split by comma
            ids = match.group(1).replace("'", "").split(", ")
            all_ids.extend(ids)

#print(f"Total extracted IDs: {len(all_ids)}")
print(all_ids)
#print(" ")# Show first 10 for verification

with open("ids.txt", "w") as f:
    json.dump(all_ids, f)

