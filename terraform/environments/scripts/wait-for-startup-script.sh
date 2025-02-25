#!/bin/sh

# Default configuration values
TIMEOUT=300       # Timeout in seconds
INTERVAL=10       # Polling interval in seconds

# Function to display usage information
usage() {
    echo "Usage: $0 -p PROJECT -n NAME -z ZONE [-t TIMEOUT] [-i INTERVAL]"
    echo
    echo "Options:"
    echo "  -p PROJECT         Name of the Google Cloud Project"
    echo "  -n NAME            Name of the Google Compute instance"
    echo "  -z ZONE            Zone of the Google Compute instance"
    echo "  -t TIMEOUT         Timeout in seconds (default: 300)"
    echo "  -i INTERVAL        Polling interval in seconds (default: 10)"
    exit 1
}

# Parse options
while getopts "p:n:z:t:i:" opt; do
    case "$opt" in
        p) PROJECT="$OPTARG" ;;
        n) NAME="$OPTARG" ;;
        z) ZONE="$OPTARG" ;;
        t) TIMEOUT="$OPTARG" ;;
        p) INTERVAL="$OPTARG" ;;
        *) usage ;;
    esac
done

# Check if mandatory options are set
if [ -z "$PROJECT" ] || [ -z "$NAME" ] || [ -z "$ZONE" ]; then
    echo "Error: PROJECT NAME and ZONE are required."
    usage
fi

set -e

# Function to check if "done" is at the end of /var/log/startup-script.log
check_log() {
  gcloud compute ssh "$NAME" --project "$PROJECT" --zone="$ZONE" --tunnel-through-iap --command="ls /etc/startup_script_was_run_successfully > /dev/null 2>&1"
  echo $?
}

# Start polling
echo "Polling for completion of startup script on instance '$NAME' in zone '$ZONE'..."
START_TIME=$(date +%s)

while true; do
#  if check_log; then
  if [ "$(check_log)" = "0" ] ; then
    echo "Startup script completed successfully on $NAME."
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
