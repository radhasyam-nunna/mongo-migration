#!/bin/bash

PID=1291989  # Replace with actual PID

# Wait for process to end
while kill -0 "$PID" 2> /dev/null; do
    echo "$(date): Waiting for PID $PID to finish..."
    sleep 1800    
#sleep 1800  # sleep for 30 minutes
done

echo "$(date): PID $PID completed. Running script..."
nohup bash validation_runner.sh &> dv_sch.log 2>&1 &
