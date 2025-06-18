env="dev"
dbName="inspection"
# dbName="refurb-service"
collectionName="inspection"
# collectionName="inspection"
echo $collectionName
nohup python3 mongo_first.py $env $dbName $collectionName &> "./logs/validation_${collectionName}.log" 2>&1 