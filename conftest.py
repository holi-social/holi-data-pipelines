# this is a monorepo in which pubsub_typesense_updater contains its own venv with its own dependencies
# executing pytest in the repo root shall not execute tests in pubsub_typesense_updater or they will probably fail
# due to missing dependencies
collect_ignore = ["pubsub_typesense_updater"]
