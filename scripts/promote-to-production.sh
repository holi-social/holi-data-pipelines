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
git push
# reset the git repository
git checkout "$CURRENT_SHA"
