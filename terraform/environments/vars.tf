locals {
  # predefined environment names are staging, production, and <branchname>
  environment      = terraform.workspace
  environment_name = trim(substr(local.environment, 0, 29), ".-") # limits the length of the name that it fits constraints google imposes for some resources, e.g. project ids
  environment_type = contains(["staging", "production"], local.environment) ? local.environment : "review"

  # the dns name can be at most 64 chars in total
  prefect_dns_domain = "${local.environment_name}.${data.terraform_remote_state.holi_data_pipeline_common_state.outputs.dns_prefect_domain}"

  default_region  = data.terraform_remote_state.holi_infra_state.outputs.default_region
  default_project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
}
