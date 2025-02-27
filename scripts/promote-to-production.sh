#!/usr/bin/env sh

# Promotes the given sha to production by merging it into the production branch and pushing to origin

usage() {
    echo "Usage: $0 <SHA>"
    echo
    echo "Options:"
    echo "  SHA         The git sha of the commit that shall be merged to the production branch"
}

SHA_TO_PROMOTE=$1

# if no sha passed, print usage and exit
if [ "$SHA_TO_PROMOTE" = "" ]; then
  usage
  exit 1
fi

# validate that sha exists
if ! git rev-list "$SHA_TO_PROMOTE" > /dev/null 2>&1 ; then
  echo "Invalid sha: '$SHA_TO_PROMOTE'" >&2
  usage
  exit 1
fi

# validate there's no uncommitted changes
if ! git diff-index --quiet HEAD; then
    echo "There are uncommitted changes. Please stash/commit/revert them before executing this script."
    exit 1
fi

CURRENT_SHA=$(git rev-parse --abbrev-ref HEAD)

set -eux

echo "Promoting $SHA_TO_PROMOTE to production"
# merge the sha into production and push it to origin
git checkout production
git pull --ff-only
git merge "$SHA_TO_PROMOTE"

git diff origin/production..main requirements.txt|grep +prefect==
retval=$?
if [ $retval -eq 0 ]; then
    echo "prefect version changed, you now need to do some manual changes:"
    echo '* Check if there are any running flows in production, potentially wait for them to finish ( http://prefect-server-production.holi.internal:8420/runs )'
    # shellcheck disable=SC2016
    echo '* Switch to the terraform environments directory `cd terraform/environments`. You might need to run `terraform init -upgrade`, too.'
    # shellcheck disable=SC2016
    echo '* Ensure you'"'"'re in the right terraform workspace `terraform workspace select production`'
    # shellcheck disable=SC2016
    echo '* execute `terraform destroy -target=google_compute_instance.prefect_server_green`'
    # shellcheck disable=SC2016
    echo '* execute `terraform apply -target=google_compute_instance.prefect_server_green -target=google_dns_record_set.prefect_server_holi_internal`'
    # shellcheck disable=SC2016
    echo '* execute `terraform apply -target=null_resource.prefect_server_available` (repeat once if this fails)'
    # shellcheck disable=SC2016
    echo '* you probably will need to execute \`direnv reload\` now in the project root for re-enabling the ssh tunnels'
    # shellcheck disable=SC2016
    echo '* execute `terraform apply`'
    echo
    while true; do
        read -r "Did you execute these steps, can I continue and push to production? (y/n)" yn
        case $yn in
            [Yy]* ) break;;
            [Nn]* ) exit;;
            * ) echo "Please answer (y)es or (n)no.";;
        esac
    done
fi



git push
# reset the git repository
git checkout "$CURRENT_SHA"

echo done
