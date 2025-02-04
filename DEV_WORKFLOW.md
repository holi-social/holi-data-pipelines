# Development Workflow Documentation

This document describes the process we agreed upon for developing and deploying/releasing changes in Data Pipelines.

## Goals

We want to be able to develop new pipelines and change existing pipelines as freely as possible while maintaining a
stable production environment. The following documentation describes the current state of the contract we developed to
achieve this goal.

## Local Development

While developing locally, you can and should work against the staging prefect environment. This should be handled
automatically for you by direnv via `.envrc.local.template`.

### Working with Prefect Variables and Secrets

This means that any prefect variables and secrets you access during local development will be resolved against the
staging environment (deployed from the main branch and therefore only aware of configuration defined in the main
branch). So when developing new pipelines or changing existing pipelines (on feature branches), when you create or
change configuration, you need to be able to locally override values in secrets and variables.

For this we defined helpers for accessing variables and secrets in `src/common/variables.py`. These helpers resolve
values by first reading from local environment variables, and falls back to prefect variables and secrets. So while
developing locally, you can set and override values via local environment variables, e.g. in `.envrc.local`.

#### Examples

Let's say you're developing a new pipeline that needs access to a variable with key `my_variable`. On your feature
branch, you will define the new variable via terraform in `terraform/environments/prefect-configuration.tf` or similar
files. In order to make this variable available for local development without changing the staging environment or the
main branch, you can export an environment variable:

```shell
export MY_VARIABLE='"variable value"'
```

Note that the variable needs to be defined as a JSON compatible value, so strings need to be enclosed in double quotes.
Therefore, it is best practice to quote the whole environment variable with single quotes.

A more complex JSON value could look like this:

```shell
export JOB_DATABASE_CONFIG='{"host": "localhost", "port": 5432, "database": "my_database"}'
```

This will allow access to the variable in the local prefect context.

```shell
$ python
Python 3.12.8 (main, Dec  5 2024, 13:51:28) [GCC 12.2.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from src.common.variables import variable_value
>>> variable_value("job_database_config")
{'host': 'localhost', 'port': 5432, 'database': 'my_database'}
```

The same principles apply to Secrets, with the main differences that Secret keys use dash instead of underscore as
separator, and secrets always are strings, not JSON objects.

### Testing

When developing new pipelines or changing existing pipelines, you should write tests for your code. We work with
different types of tests:

#### Unit Tests

A pipeline consists of complex behaviour, which is hard to unit test. But it is made up of building blocks that can be
designed to be unit testable. Whenever possible, write good unit tests for your pipeline building blocks.

#### Data Quality Tests

Your pipeline is working with input data and is outputting data. You should write tests that check the quality of both
these data types.

##### Output Data

On every pipeline run, after your pipeline has finished processing, it should execute a data quality check on the output
data. Define this check as an extra task in your pipeline, and make the pipeline fail if the check fails. As naming
convention, we use `validate_output` as the task function's name.

##### Input Data

If your pipeline is reading data from a source, you should write tests that validate your expectations about the input.
Define the tests next to your flow function, one per input source. In the tests, you read your input data like you do
during a pipeline run and perform any assertions you expect or see as a prerequisite on the data.

You then add these functions to the pipeline(s) producing your input data by calling them from the pipeline's
`validate_output` task. This way, the source pipeline is aware of any expectations it's code must meet and it will fail
if not all of them are met.

### Merge Requests

#### Terraform Changes

If you developed changes to the infrastructure, they will not be checked by CI as part of the MR build pipeline.
Therefore it's good practice to not only `terraform plan` your changes against the staging environment, but to also
`terraform apply` them and see whether it succeeds.

This can of course lead to issues on staging, because an apply from a branch other than main changes the staging
environment to a state that might be unexpected by merged pipelines, leading to pipeline failures, and also by other
developers, leading to confusion and waste of time. Therefore, after testing your terraform changes, reapply terraform
against the staging environment/workspace from the freshly pulled main branch.

Be especially careful with changes to existing variables and secrets, as they might break existing pipelines.

#### Variable/Secret Changes

When you're done with your feature, you should create a merge request. The reviewer needs to know about new
Secrets/Variables. So an MR's description should contain a list of new Secrets/Variables and their values as
copy-pasteable shell snippet next to other valuable information for the reviewer:

```markdown
* New Variables:
    * my_variable - explain what it's for
    * job_database_config - contains database connection information
* New Secrets:
    * job-database-password - password for the database

Local development environment variables:
export MY_VARIABLE='"variable value"'
export JOB_DATABASE_CONFIG='{"host": "localhost", "port": 5432, "database": "my_database"}'
export JOB_DATABASE_PASSWORD='password'
```

## Going Live - Promoting Changes to Production

Our aim is to have a stable production environment, so we need to be careful when promoting changes to production.

After an MR has been merged, it is automatically deployed to the staging environment. There, we need to verify whether
the code is ready to be published to production.

### JIRA Workflow

In JIRA, every task or story creating or changing a pipeline should contain a task for releasing the task/story to
production, so we don't forget and accumulate large changesets in staging.

### Verification

Currently, we don't have an automated way to verify whether changes on staging are ready for production. We therefore
rely on manual or scheduled execution of deployments/flows in the staging environment.

Of course, in order to guarantee a change to be safe, especially if it has touched common code, all pipelines need to
have run successfully. But sometimes only pipeline specific code is touched and only running the changed pipeline is a
good verification. There's no strict rules here, we appeal to you to do the right thing to keep production stable.

### Promoting Changes

When you're confident that the changes are ready for production, you can promote them to production via a git merge.
This means that we can only easily promote changes up to a certain commit on the main branch. A description of such a
simple promotion follows:

#### Promoting to a certain commit

Prerequisite to this strategy is that all commits up to the commit you want to promote are verified. There can be newer
commits on staging that are yet unverified, which will not be promoted.

Identify the git sha of the commit. Then, you can execute the release script with the commit sha as argument:

```shell
./scripts/promote-to-production.sh <commit-sha>
```

This will merge all changes up to the specified commit to the production branch and push it to the remote. CI will then
deploy the changes to production.

#### More complex Promotions

If you need to promote changes that have unverified changes as ancestors, or any other more complex promotion, you're on
your own. Please follow good practices and common sense.


