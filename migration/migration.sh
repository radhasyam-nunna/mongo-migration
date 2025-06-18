#update user-name, password

# nohup ./run_migration.sh &> migration.log &

collectionName=$1
dbName=$2
mongoUri=$3
dir="/mnt/disk-1/mdb/$collectionName"
testCollectionName="${collectionName}"

echo "Using collection: $collectionName | database: $dbName | URI: $mongoUri"

mkdir -p "$dir"

#arangoexport 
sudo docker run -v /mnt/disk-1:/mnt/disk-1/ arangodb/arangodb:3.10.10 \
  arangoexport \
  --server.endpoint ssl://172.16.2.12:8529 \
  --server.database "$dbName"-in \
  --server.username user-name \
  --server.password passsword \
  --custom-query "FOR doc IN $collectionName LET docKey = doc._key RETURN MERGE(UNSET(doc, '_id','_rev','_class','_key'), { _id: docKey })" \
  --type json \
  --output-directory "$dir" \
  --overwrite true \
  --documents-per-batch 300 \
  --progress true  
  # &> "$dir/${collectionName}.log" &

jsonFile="query.json"

#if [[ ! -f "$jsonFile" ]]; then
  #echo "Export file not found. Migration failed."
 # exit 1
#fi

# mongo import
#mongoimport --uri "$mongoUri"  --db "$dbName" --collection "$testCollectionName" --file "${dir}/query.json" --jsonArray --numInsertionWorkers=8
mongoimport --uri "$mongoUri"  \
	--db "$dbName" \
	--collection "$testCollectionName" \
	--file "${dir}/query.json" \
	--jsonArray \
	--numInsertionWorkers=8 


# run date conversion script
curl --location "http://localhost:7091/test/migrate/$dbName/$testCollectionName"
