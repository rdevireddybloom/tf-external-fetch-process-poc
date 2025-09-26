# Variable Definitions

variable "aws_region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "us-west-2"
}

variable "environment" {
  description = "The deployment environment (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "lambda_function_name" {
  description = "The name of the Lambda function"
  type        = string
}

variable "lambda_iam_role_name" {
  description = "The IAM role name for the Lambda function"
  type        = string
  default     = "testPostgresConnection-role-zccjgjtd"
}

variable "lambda_handler" {
  description = "The handler for the Lambda function"
  type        = string
  default     = "lambda_function.lambda_handler"
}

variable "lambda_runtime" {
  description = "The runtime for the Lambda function"
  type        = string
  default     = "python3.11"
}

variable "lambda_memory" {
  description = "The memory size for the Lambda function"
  type        = number
  default     = 128
}

variable "lambda_timeout" {
  description = "The timeout for the Lambda function"
  type        = number
  default     = 30
}

variable "vpc_subnet_ids" {
  description = "The VPC subnet IDs for the Lambda function"
  type        = list(string)
}

variable "vpc_security_group_ids" {
  description = "The VPC security group IDs for the Lambda function"
  type        = list(string)
}

variable "db_secret_name" {
  description = "The name of the database secret"
  type        = string
}

variable "job_name" {
  description = "The name of the job"
  type        = string
}

variable "job_class" {
  description = "The class of the job"
  type        = string
}

variable "lambda_layers" {
  description = "The Lambda layers to include"
  type        = list(string)
}

variable "s3_bucket_name" {
  description = "The name of the S3 bucket"
  type        = string
  default     = "bloom-dev-data-team"
}

variable "s3_prefix" {
  description = "The S3 prefix (folder path)"
  type        = string
}
