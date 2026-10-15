terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "s3" {
    bucket       = "btp-tfstate-patrizio"
    key          = "pipeline/terraform.tfstate"
    region       = "eu-north-1"
    profile      = "portfolio"
    use_lockfile = true
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile
  default_tags {
    tags = { Project = "brisbane-transit-pipeline", ManagedBy = "terraform" }
  }
}

# --- Data lake ---
resource "aws_s3_bucket" "lake" {
  bucket = "${var.project}-lake-${var.suffix}"
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 non ha vere cartelle: si creano oggetti vuoti che finiscono con "/"
resource "aws_s3_object" "prefixes" {
  for_each = toset(["raw/", "archive/"])
  bucket   = aws_s3_bucket.lake.id
  key      = each.value
  content  = ""
}

# --- Utente per la pipeline ---
resource "aws_iam_user" "ingestion" {
  name = "${var.project}-ingestion"
}

data "aws_iam_policy_document" "ingestion" {
  statement {
    sid       = "ListOnlyLakePrefixes"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "archive/*"]
    }
  }
  statement {
    sid     = "ReadWriteLakeObjects"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/raw/*",
      "${aws_s3_bucket.lake.arn}/archive/*",
    ]
  }
}

resource "aws_iam_user_policy" "ingestion" {
  name   = "lake-access"
  user   = aws_iam_user.ingestion.name
  policy = data.aws_iam_policy_document.ingestion.json
}