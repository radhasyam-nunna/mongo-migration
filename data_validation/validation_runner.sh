env="prod"
dbName="schema"
#dbName="refurb-service"
#collections=("inspection_dump") 
#collections=$1
#collections=("attachment_details_audit_log" "basic_details_audit_log" "checklist_details_audit_log" "checkpoint_master_audit_log" "loose_items_audit_log" "sf_checkpoint_details_audit_log" "warranty_details_audit_log")
collections=("attachment_details" "basic_details" "checklist_details" "checkpoint_master" "loose_items" "sf_checkpoint_details" "warranty_booklet" "warranty_details" "schema")
#collections=("archived_inspection" "inspection_dump" "invoice" "property" "work_order" "work_order_execution")
for collectionName in "${collections[@]}"; do
  echo "Starting data validation for $collectionName"
  python3 app.py $env $dbName $collectionName &> "./logs/validation_${collectionName}.log" 2>&1
  echo "Starting mongo first $collectionName"
  python3 mongo_first.py $env $dbName $collectionName &> "./logs/validation_mongo_${collectionName}.log" 2>&1
  echo "Starting dateV for $collectionName"
  python3 date.py $env $dbName $collectionName &> "./logs/validation_date_${collectionName}.log" 2>&1
  echo "Finished migration for $collectionName"
  echo "---------------------------------------"
done

wait
echo "All data validations are completed"




