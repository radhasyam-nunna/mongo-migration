# nohup ./run_all.sh &> run_all_migrations.log &
  
# List of collection names
# collections=("attachment_details" "basic_details" "checklist_details" "checkpoint_master" "loose_items" "schema" "sf_checkpoint_details" "warranty_booklet" "warranty_details")  
#collections=("archived_inspection" "invoice" "property" "work_order" "work_order_execution" "inspection_dump" "inspection") 

#collections=("attachment_details_audit_log" "basic_details_audit_log" "checklist_details_audit_log" "checkpoint_master_audit_log" "loose_items_audit_log" "sf_checkpoint_details_audit_log" "warranty_details_audit_log")
collections=("work_order_audit_log" "invoice_audit_log")
dbName="refurb-service"
schemaUri="****" #mongoUri
serviceUri="****" #mongoUri

for collectionName in "${collections[@]}"; do
  echo "Starting migration for $collectionName"
  
  ./migration.sh "$collectionName" "$dbName" "$serviceUri" &> "logs/migration_${collectionName}.log" 2>&1 

  echo "Finished migration for $collectionName"
  echo "---------------------------------------"
done

wait
echo "All migrations finished"
