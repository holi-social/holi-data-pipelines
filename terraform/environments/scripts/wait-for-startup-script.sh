#!/bin/sh

# Default configuration values
TIMEOUT=300       # Timeout in seconds
INTERVAL=10       # Polling interval in seconds

# Function to display usage information
usage() {
    echo "Usage: $0 -i INSTANCE_NAME -z ZONE [-t TIMEOUT] [-p INTERVAL]"
    echo
    echo "Options:"
    echo "  -i INSTANCE_NAME   Name of the Google Compute instance"
    echo "  -z ZONE            Zone of the Google Compute instance"
    echo "  -t TIMEOUT         Timeout in seconds (default: 300)"
    echo "  -p INTERVAL        Polling interval in seconds (default: 10)"
    exit 1
}

# Parse options
while getopts "i:z:t:p:" opt; do
    case "$opt" in
        i) INSTANCE_NAME="$OPTARG" ;;
        z) ZONE="$OPTARG" ;;
        t) TIMEOUT="$OPTARG" ;;
        p) INTERVAL="$OPTARG" ;;
        *) usage ;;
    esac
done

# Check if mandatory options are set
if [ -z "$INSTANCE_NAME" ] || [ -z "$ZONE" ]; then
    echo "Error: INSTANCE_NAME and ZONE are required."
    usage
fi

set -e

# Function to check if "done" is at the end of /var/log/startup-script.log
check_log() {
  gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --tunnel-through-iap --command="ls /etc/startup_script_was_run_successfully > /dev/null 2>&1"
  echo $?
}

# Start polling
echo "Polling for completion of startup script on instance '$INSTANCE_NAME' in zone '$ZONE'..."
START_TIME=$(date +%s)

while true; do
#  if check_log; then
  if [ "$(check_log)" = "0" ] ; then
    echo "Startup script completed successfully on $INSTANCE_NAME."
    exit 0
  fi

  # Check if we've exceeded the timeout
  CURRENT_TIME=$(date +%s)
  ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
  
  if [ "$ELAPSED_TIME" -ge "$TIMEOUT" ]; then
    echo "Timeout reached. Startup script did not complete within $TIMEOUT seconds."
    exit 1
  fi

  # Wait and poll again
  echo "Startup script not yet complete. Waiting for $INTERVAL seconds before polling again..."
  sleep "$INTERVAL"
done
