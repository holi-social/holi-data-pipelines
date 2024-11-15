#!/bin/sh

# Default configuration values
ENABLE=0       # should the unauthenticated endpoint be dis- or enabled?

# Function to display usage information
usage() {
    echo "Usage: $0 -d DOMAIN -p PORT -i INSTANCE_NAME -z ZONE [-o]"
    echo
    echo "Options:"
    echo "  -d DOMAIN          Domain of the Google Compute instance's HTTP server"
    echo "  -p PORT            Port of the Google Compute instance's HTTP server"
    echo "  -i INSTANCE_NAME   Name of the Google Compute instance"
    echo "  -z ZONE            Zone of the Google Compute instance"
    echo "  -o                 Instead of closing the unauthenticated endpoint, open it"
    exit 1
}

# Parse options
while getopts "d:p:i:z:o" opt; do
    case "$opt" in
        d) DOMAIN="$OPTARG" ;;
        p) PORT="$OPTARG" ;;
        i) INSTANCE_NAME="$OPTARG" ;;
        z) ZONE="$OPTARG" ;;
        o) ENABLE=1 ;;
        *) usage ;;
    esac
done

# Check if mandatory options are set
if [ -z "$INSTANCE_NAME" ] || [ -z "$ZONE" ]; then
    echo "Error: INSTANCE_NAME and ZONE are required."
    usage
fi

set -e

if [ "$ENABLE" -eq 1 ]; then
  echo "Opening unauthenticated endpoint on instance '$INSTANCE_NAME' in zone '$ZONE'..."
  gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --command="sudo ln -fs /etc/nginx/sites-available/prefect-server-unauthenticated /etc/nginx/sites-enabled/prefect-server-unauthenticated && sudo systemctl reload nginx"
  nc -z "$DOMAIN" "$PORT" || (echo "Failure: Unauthenticated endpoint is not open as expected." && exit 1)
  echo "Will auto-close the unauthenticated endpoint after 60 seconds..."
  gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --command="nohup sudo sh -c 'sleep 60 && rm /etc/nginx/sites-enabled/prefect-server-unauthenticated && systemctl reload nginx' > nohup.log 2> nohup.err < /dev/null &"
else
  echo "Closing unauthenticated endpoint on instance '$INSTANCE_NAME' in zone '$ZONE'..."
  gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --command="sudo rm -f /etc/nginx/sites-enabled/prefect-server-unauthenticated && sudo systemctl reload nginx"
  nc -z "$DOMAIN" "$PORT" && (echo "Failure: Unauthenticated endpoint is not closed as expected." && exit 1)
fi

echo "Done."