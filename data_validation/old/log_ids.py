import re

all_ids = []
fileName="field_mismatches_inspection_3.json"

import json

# Load JSON data from a file
with open(fileName, "r") as f:
    data = json.load(f)

# Extract all _id values
ids = [f'"{item["_id"]}"' for item in data]

# Print the _ids
print("Extracted _ids:")
print(", ".join(ids))

# with open(fileName, 'r') as file:
#     for line in file:
#         match = re.search(r"ids:\[(.*?)\]", line)
#         if match:
#             # Remove quotes and split by comma
#             ids = match.group(1).replace("'", "").split(", ")
#             all_ids.extend(ids)

# print(f"Total extracted IDs: {len(all_ids)}")
# print(all_ids)  # Show first 10 for verification

