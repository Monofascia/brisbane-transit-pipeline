output "lake_bucket" { value = aws_s3_bucket.lake.bucket }
output "ingestion_user" { value = aws_iam_user.ingestion.name }