locals {
  default_region  = data.terraform_remote_state.holi_infra_state.outputs.default_region
  default_project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
}
