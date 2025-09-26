provider "aws" {
  region = var.aws_region
}

# ============================================
# DATA SOURCES - Reference Existing Resources
# ============================================

data "aws_iam_role" "lambda_execution" {
  name = var.lambda_iam_role_name
}

data "aws_s3_bucket" "trigger_bucket" {
  bucket = var.s3_bucket_name
}

# Archive the Lambda function code
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "./src/lambda_function.py"
  output_path = "./lambda_function.zip"
}

# ============================================
# LAMBDA FUNCTION
# ============================================

resource "aws_lambda_function" "main" {
  function_name = var.lambda_function_name
  description   = "Terraform managed Lambda function for external fetch and process"

  role          = data.aws_iam_role.lambda_execution.arn
  handler       = var.lambda_handler
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory
  architectures = ["x86_64"]

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = {
      DO_SECRET_NAME = var.db_secret_name
      REGION         = var.aws_region
      ENV            = var.environment
      JOB_NAME       = var.job_name
      JOB_CLASS      = var.job_class
    }
  }

  layers = var.lambda_layers

}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.main.arn
}

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.main.function_name
}
