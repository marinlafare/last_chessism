#!/bin/bash

# This script starts a background monitor for your NVIDIA GPUs.
# It logs the timestamp, index, name, temperature, utilization, and clock speed
# to a CSV file every 60 seconds.

LOG_FILE="gpu_temp_log.csv"
POLL_INTERVAL_SECONDS=60

echo "Starting GPU monitor..."
echo "Logging to: $LOG_FILE"
echo "Polling every: $POLL_INTERVAL_SECONDS seconds"

# Check if the log file is new. If it is, add a header row.
if [ ! -f "$LOG_FILE" ]; then
    echo "Creating new log file with headers."
    # This first command runs once to create the file and add the CSV header
    nvidia-smi \
        --query-gpu=timestamp,index,name,temperature.gpu,utilization.gpu,clocks.gr \
        --format=csv,noheader,nounits \
        > $LOG_FILE
fi

# 'nohup' ensures the command keeps running even if you close your terminal.
# '>>' appends to the file.
# '&' runs the command in the background.
nohup nvidia-smi \
    --query-gpu=timestamp,index,name,temperature.gpu,utilization.gpu,clocks.gr \
    --format=csv,noheader,nounits \
    --loop=$POLL_INTERVAL_SECONDS \
    >> $LOG_FILE &

# Get the Process ID (PID) of the background job
MONITOR_PID=$!

echo "Monitor started in background with PID: $MONITOR_PID"
echo "You can now close this terminal. The log will be saved to $LOG_FILE"
echo "----------------------------------------------------------------"
echo "To watch the log in real-time, run:"
echo "tail -f $LOG_FILE"
echo ""
echo "To stop the monitor later, run:"
echo "kill $MONITOR_PID"
echo "(If you lose the PID, run 'pkill -f \"nvidia-smi --loop\"')"