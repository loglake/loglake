provider "aws" {
  region = var.region

  default_tags {
    tags = merge(
      {
        Project   = "loglake"
        ManagedBy = "terraform"
      },
      var.tags,
    )
  }
}
