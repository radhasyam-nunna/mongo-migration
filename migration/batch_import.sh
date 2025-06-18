#!/bin/bash

# Configurations
collectionName="inspection"
dbName="refurb-service"
dirBase="/mnt/disk-1/mdb/$collectionName"
mongoUri="****"
arangodbDockerImage="arangodb/arangodb:3.10.10"
arangodbEndpoint="ssl://172.16.2.12:8529"
arangodbUser="****"
arangodbPass='****'

# Input file with JSON array of IDs (single line)
inputFile="./data_validation/ids.txt"

# Temp directory to hold batches
tmpDir="./batch_ids"
mkdir -p "$tmpDir"

echo "Starting batch export/import process..."

# Step 1: Convert JSON array single line to line-separated IDs
echo "Converting JSON array to line-separated IDs..."
cat "$inputFile" | tr -d '[]"' | tr ',' '\n' > "$tmpDir/ids_line_separated.txt"

# Step 2: Split IDs into batches of 1000 lines
echo "Splitting IDs into batches of 1000..."
split -l 1000 "$tmpDir/ids_line_separated.txt" "$tmpDir/batch_"

dir="/mnt/disk-1/mdb/$collectionName"
# Step 3: Loop over batches and run export + import
batchNumber=1
for batchFile in "$tmpDir"/batch_*; do
  echo "Processing batch number: $batchNumber"
#   echo "Processing batch file: $batchFile"

  # Create JSON array string from batchFile
  ids_json=$(sed 's/^/"/; s/$/"/' "$batchFile" | paste -sd "," -)
  ids_json="[$ids_json]"

  # Prepare output directory for this batch
  batchDir="$dirBase/$(basename $batchFile)"
  mkdir -p "$batchDir"

  # Arango export command
  sudo docker run --rm -v /mnt/disk-1:/mnt/disk-1/ "$arangodbDockerImage" \
    arangoexport \
    --server.endpoint "$arangodbEndpoint" \
    --server.database "${dbName}-in" \
    --server.username "$arangodbUser" \
    --server.password "$arangodbPass" \
    --custom-query "FOR doc IN $collectionName FILTER doc._key IN $ids_json LET docKey = doc._key RETURN MERGE(UNSET(doc, '_id','_rev','_class','_key'), { _id: docKey })" \
    --type json \
    --output-directory "$dir" \
    --overwrite true \
    --documents-per-batch 300 \
    --progress true

  # Mongo import command
  mongoimport --uri "$mongoUri" \
    --db "$dbName" \
    --collection "$collectionName" \
    --file "$dir/query.json" \
    --jsonArray \
    --upsert \
    --numInsertionWorkers=8

  echo "Batch $batchNumber processed."
  ((batchNumber++))
done

echo "All batches processed."

# Optional: call date conversion service once at the end (uncomment if needed)
# echo "Starting date conversion script"
# curl --location "http://localhost:7091/test/migrate/$dbName/$collectionName?applyDateFilter=true"
