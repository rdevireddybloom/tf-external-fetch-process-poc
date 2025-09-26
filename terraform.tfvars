# ============================================
# GENERAL CONFIGURATION
# ============================================

aws_region  = "us-west-2" # Your AWS region
environment = "Prod"      # dev, staging, or prod

# ============================================
# LAMBDA CONFIGURATION
# ============================================

lambda_function_name = "tf-extract-fetch-process" # Name of the Lambda function

lambda_iam_role_name = "testPostgresConnection-role-zccjgjtd" # IAM role for Lambda

# Lambda settings
lambda_handler = "lambda_function.lambda_handler"
lambda_runtime = "python3.11"
lambda_memory  = 256 # in MB, Default is 128 MB
lambda_timeout = 300 # in seconds

# ============================================
# VPC CONFIGURATION
# ============================================

vpc_subnet_ids         = ["subnet-88fe04f1", "subnet-d8ce0e91"]
vpc_security_group_ids = ["sg-c39750bc", "sg-08d131086aac5ab1c", "sg-0952d40614dbba40e"]

# ============================================
# ENVIRONMENT VARIABLES
# ============================================

db_secret_name = "etluser/dev/rds"

job_name  = "External-Fetch-Process"
job_class = "Ingest"

# ============================================
# LAMBDA LAYERS
# ============================================

lambda_layers = [
  "arn:aws:lambda:us-west-2:511539536780:layer:psycopg2-layer-x86-64:6",
  "arn:aws:lambda:us-west-2:511539536780:layer:paramiko-layer:1",
  "arn:aws:lambda:us-west-2:511539536780:layer:requests-layer:1",
  "arn:aws:lambda:us-west-2:511539536780:layer:de_aws_utils-library:8"
]

# ============================================
# S3 TRIGGER CONFIGURATION
# ============================================

s3_bucket_name = "bloom-dev-data-team"
s3_prefix      = "test/IaCTesting/external-fetch-process-testing/"

