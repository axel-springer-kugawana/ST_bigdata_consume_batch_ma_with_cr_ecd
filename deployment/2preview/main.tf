module "main" {
  source     = "../../deployment"
  account_id = local.account_id
  env        = local.env
}