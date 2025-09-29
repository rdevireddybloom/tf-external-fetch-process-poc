# external_fetch_handler.py

import os
import json
import boto3
import logging
import tempfile
import hashlib
import psycopg2
import paramiko
import requests
import io
import time
import zipfile
import textwrap
from ftplib import FTP
import datetime
from io import StringIO
from botocore.exceptions import ClientError
from de_aws_utils.logging.cloudwatchsnslogger import CloudWatchSNSLogger

# ------------------ Logger Setup ------------------
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

aws_region = os.environ.get("REGION", "us-west-2")
do_secret_name = os.environ.get("DO_SECRET_NAME", "etluser/dev/rds")
env = os.environ.get("ENV", "Prod")
job_name = os.environ.get("JOB_NAME", "External-Fetch-Process")
job_class = os.environ.get("JOB_CLASS", "Ingest")

logger = CloudWatchSNSLogger()
logger.process_name = job_name
logger.env = env
logger.job_class = job_class
logger.service = "lambda"
# ------------------ AWS Clients ------------------
# SECRET_REGION = os.getenv("AWS_REGION", "us-west-2")
s3 = boto3.client("s3")
SECRETS_MANAGER = boto3.client("secretsmanager", region_name=aws_region)

# ------------------ Helper Functions ------------------


# This function retrieves a secret from AWS Secrets Manager.
# The secret usually contains credentials like usernames and passwords.
def get_secret(secret_name):
    try:
        # Request the secret value using the secret name provided
        response = SECRETS_MANAGER.get_secret_value(SecretId=secret_name)

        # The secret is returned as a JSON string — parse it into a Python dictionary
        return json.loads(response["SecretString"])
    except ClientError as e:
        logger.error(f"Unable to retrieve secret {secret_name}: {e}")
        return None


# This function establishes a PostgreSQL database connection using credentials
# fetched from AWS Secrets Manager.
def get_db_connection(secret_name):
    # First, retrieve the database credentials (host, dbname, user, etc.)
    secret = get_secret(secret_name)

    if not secret:
        return None

    try:
        # Use the retrieved secret values to create a new database connection
        return psycopg2.connect(
            host=secret["host"],  # Database host (e.g., endpoint URL)
            dbname=secret["dbname"],  # Name of the database
            user=secret["username"],  # Database username
            password=secret["password"],  # Database password
            port=secret.get(
                "port", 5432
            ),  # Optional port (defaults to 5432 if not provided)
        )

    except Exception as e:
        logger.error(f"DB connection error: {e}")
        return None


# This function retrieves all active file fetch configurations from the database.
# Only configurations that are currently valid (based on start and end dates) are returned.
def fetch_all_configs(conn):
    try:
        # Create a database cursor using the provided connection
        with conn.cursor() as cur:

            # Run a SQL query to fetch active fetch_config records
            # These include details needed to pull files: source type, paths, credentials, etc.
            cur.execute(
                """
                SELECT external_fetch_config_id, file_name_mask, external_fetch_type, fetch_location, access_secret,
                       target_s3_bucket, target_s3_location, extract_fetched_file_flag, add_date_to_file_name_flag,
                       source_file_archive_bucket, source_file_archive_location
                FROM data_operations.external_fetch_config
                WHERE CURRENT_DATE BETWEEN effective_start_date AND effective_end_date
                AND (date_part('day', current_date)::varchar = any(string_to_array(schedule_days_of_month_list, ','))
                or schedule_days_of_month_list = 'any')
                and (to_char(current_date, 'Dy') = any(string_to_array(schedule_days_of_week_list, ','))
                or schedule_days_of_week_list = 'any')
                and (date_part('hour', current_timestamp)::varchar = any(string_to_array(schedule_hours_of_day_list, ','))
                or schedule_hours_of_day_list = 'any');
                """
            )
            return cur.fetchall()

    except Exception as e:
        logger.exception(f"Error fetching fetch_config list: {e}")
        raise e  # Re-raise the exception to be handled by the caller


# This function takes a file name pattern (mask) and replaces any date placeholders
# like 'mmddyyyy' or '????????' with today's actual date in the matching format.
def resolve_filename_from_mask(file_name_mask):
    today = datetime.datetime.today()

    # Define supported date format patterns and what they resolve to using today’s date
    preferred_patterns = {
        "mmddyyyy": today.strftime("%m%d%Y"),  # e.g., 05122025
        "ddmmyyyy": today.strftime("%d%m%Y"),  # e.g., 12052025
        "yyyymmdd": today.strftime("%Y%m%d"),  # e.g., 20250512
        "yyyyddmm": today.strftime("%Y%d%m"),  # e.g., 20251205
    }

    # Start with the original file name mask
    resolved = file_name_mask

    # First, look for any known pattern (e.g., mmddyyyy) in the mask and replace it
    for pattern, value in preferred_patterns.items():
        if pattern in resolved:
            resolved = resolved.replace(pattern, value)
            break  # Stop after replacing the first matched pattern

    # If the pattern "????????" exists, try replacing it with each supported date format
    if "????????" in resolved:
        for value in preferred_patterns.values():
            test = resolved.replace("????????", value)
            if value in test:
                resolved = test
                break  # Use the first format that works

    return resolved


# This function constructs the final HTTPS URL to download a file.
# It combines the base URL and a dynamically resolved file name based on today's date.
def resolve_web_url(web_base_url, file_name_mask):
    # Generate the actual file name by replacing any date placeholders in the mask
    file_name = resolve_filename_from_mask(file_name_mask)

    # Ensure the base URL ends with a slash so we don’t get malformed URLs
    if not web_base_url.endswith("/"):
        web_base_url += "/"

    # Combine the base URL and resolved file name to create the full download URL
    return web_base_url + file_name


# This function calculates the MD5 checksum of a file held in memory (as bytes).
# It's used to verify the integrity of the file before and after uploading to S3.
def calculate_md5_bytes(data_bytes):
    md5 = hashlib.md5()
    # Feed the binary data into the hash object
    md5.update(data_bytes)
    # Return the hexadecimal digest string (e.g., '9a0364b9e99bb480dd25e1f0284c8555')
    return md5.hexdigest()


# This function uploads a file to S3 and verifies that the upload was successful
# by comparing the file’s MD5 checksum before and after the upload.


def upload_to_s3_and_validate(file_data, bucket, key, expected_md5):
    try:
        print(f"Uploading to S3: s3://{bucket}/{key}")
        # Step 1: Upload the file to the specified S3 bucket and key (path)
        s3.upload_fileobj(io.BytesIO(file_data), bucket, key)

        # Step 2: After upload, download the same file from S3 to verify its contents
        print("Verifying upload...")
        s3_object = s3.get_object(Bucket=bucket, Key=key)
        s3_data = s3_object["Body"].read()

        # Step 3: Calculate the MD5 checksum of the downloaded file
        actual_md5 = calculate_md5_bytes(s3_data)

        # Step 4: Compare the pre-upload and post-upload checksums
        if actual_md5 == expected_md5:
            print("Checksum validation passed!")  # Data integrity confirmed
            return True
        else:
            print(f"Checksum mismatch! Expected: {expected_md5}, Found: {actual_md5}")
            return False  # Data mismatch detected

    except Exception as e:
        logger.exception(f"S3 upload/validation error: {e}")
        raise e  # Re-raise the exception to be handled by the caller


# This function logs details of a file fetch operation into the external_fetch_log table.
# It tracks the file name, checksum, status, error (if any), file size, and timestamps.
def log_fetch_event(
    conn,  # Database connection object
    fetched_file_name,  # Original file name before processing
    file_name,  # Name of the file that was fetched or processed
    archived_file_name,  # Name of the file after archiving
    config_id,  # The fetch_config_id from fetch_config table
    status,  # Status of the operation: 'Success' or 'Failed'
    error_msg,  # Any error message if the operation failed
    start_time,  # Timestamp when the operation started
    notes,
):
    try:
        with conn.cursor() as cur:
            # Insert a new row into the external_fetch_log table with fetch details
            cur.execute(
                """
                INSERT INTO data_operations.external_fetch_log (
                    external_fetch_config_id, fetched_file_name, 
                    target_file_name, archived_file_name, process_status,
                    start_datetime, end_datetime, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, now(), %s);
            """,
                (
                    config_id,
                    fetched_file_name,
                    file_name,
                    archived_file_name,
                    status,
                    # error_msg,
                    start_time,
                    notes,
                ),
            )

            conn.commit()

    except Exception as e:
        logger.exception(f"Logging error: {e}")
        raise e  # Re-raise the exception to be handled by the caller


# This function extracts all non-directory files from a ZIP archive stored in memory.
# It returns a list of tuples: (file_name, file_content) for each extracted file.
def extract_zip_file(file_bytes):
    extracted_files = []
    try:
        # Wrap the incoming bytes in a BytesIO stream so zipfile can read it as a file
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:

            # Loop through each file name inside the ZIP archive
            for name in z.namelist():

                # Skip directories (names that end with '/')
                if not name.endswith("/"):
                    # Read the content of the file and add it to the list
                    extracted_files.append((name, z.read(name)))

        # Return the list of (file_name, file_data) tuples
        return extracted_files

    except Exception as e:
        logger.exception(f"Error extracting ZIP: {e}")
        raise e  # Re-raise the exception to be handled by the caller


def apply_date_to_file_name(file_name_mask):
    name, ext = os.path.splitext(file_name_mask)
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return f"{name}_{timestamp}{ext}"


def archive_file_to_s3(
    file_name,  # Name of the file being archived
    s3_bucket,  # Target S3 bucket for archiving
    s3_path,  # S3 folder/path where file should be archived
    file_data,  # Raw byte content of the file
):
    try:
        file_name = apply_date_to_file_name(file_name)
        s3_key = f"{s3_path.rstrip('/')}/{file_name}"
        # Upload the file to S3 for archiving
        s3.upload_fileobj(io.BytesIO(file_data), s3_bucket, s3_key)
        print(f"Archived file to S3: s3://{s3_bucket}/{s3_key}")
        return file_name  # Return the name of the archived file

    except Exception as e:
        logger.exception(f"Error archiving file to S3: {e}")
        raise e  # Re-raise the exception to be handled by the caller


# This function handles the logic for uploading a file (or ZIP contents) to S3.
# It validates the file using an MD5 checksum and logs the upload result.
# If the file is a ZIP archive, it extracts and processes each file individually.
def process_and_upload(
    file_name,  # Name of the file being processed
    file_data,  # Raw byte content of the file
    s3_bucket,  # Target S3 bucket name
    s3_path,  # S3 folder/path where file should be uploaded
    archive_bucket,  # Optional archive bucket for source files
    archive_location,  # Optional archive path for source files
    config_id,  # ID from the fetch_config row
    extract_flag,
    add_date_flag,
    conn,  # Database connection (used for logging)
    start_time,  # Timestamp when the fetch operation started
):
    try:
        fetched_file_name = file_name  # Store the original file name for logging
        archived_file_name = archive_file_to_s3(
            file_name, archive_bucket, archive_location, file_data
        )
        if add_date_flag:
            # If the flag is set, apply the current date to the file name
            file_name = apply_date_to_file_name(file_name)
        print(f"Processing file {fetched_file_name}: {file_name}")
        # If the file is a ZIP archive, and extract flag is true, then extract it and process each inner file
        if file_name.endswith(".zip") and extract_flag == True:
            extracted = extract_zip_file(file_data)  # Extracts inner files
            success = True  # Flag to track if all inner files are uploaded successfully

            for inner_name, inner_bytes in extracted:
                # Create the full S3 key/path for the extracted file
                if add_date_flag:
                    inner_name = apply_date_to_file_name(inner_name)

                inner_key = f"{s3_path.rstrip('/')}/{inner_name}"

                # Generate checksum for the inner file
                checksum = calculate_md5_bytes(inner_bytes)

                # Upload the file and validate its integrity
                if upload_to_s3_and_validate(
                    inner_bytes, s3_bucket, inner_key, checksum
                ):
                    # If successful, log the event in the database
                    log_fetch_event(
                        conn,
                        fetched_file_name,  # We log the original ZIP file name here
                        inner_name,  # Use the inner file name for target filename logging
                        archived_file_name,  # Archive file name
                        config_id,
                        "Success",
                        None,
                        start_time,
                        "File extracted from ZIP and uploaded",
                    )
                else:
                    success = False  # If any file fails to upload/validate, mark as unsuccessful

            return success  # Return True only if all extracted files succeeded

        else:

            s3_key = f"{s3_path.rstrip('/')}/{file_name}"

            # Calculate checksum
            checksum = calculate_md5_bytes(file_data)

            # Upload and validate
            if upload_to_s3_and_validate(file_data, s3_bucket, s3_key, checksum):
                # Log if successful
                log_fetch_event(
                    conn,
                    fetched_file_name,  # Original file name for logging
                    file_name,  # Target file name in S3
                    archived_file_name,  # Archive file name
                    config_id,
                    "Success",
                    None,  # No error message if successful
                    start_time,
                    "File uploaded to S3 successfully",
                )
                return True

            # Return False if upload or validation failed
            return False
    except Exception as e:
        logger.exception(f"Error processing file {fetched_file_name}: {e}")
        raise e  # Re-raise the exception to be handled by the caller


# This function connects to an FTP server using the provided credentials,
# fetches all files from the specified remote directory,
# and returns a list of (filename, file_content) tuples.
def fetch_from_ftp(creds, remote_dir):
    files = []
    try:
        # Connect to the FTP server using the host provided in the secret
        ftp = FTP(creds["host"])

        # Login using username and password from the credentials
        ftp.login(user=creds["username"], passwd=creds["password"])

        # Change to the specified remote directory
        ftp.cwd(remote_dir)

        # List all files in the current directory on the FTP server
        for filename in ftp.nlst():
            bio = io.BytesIO()  # Create an in-memory buffer to store file data

            # Download the file and write its content into the in-memory buffer
            ftp.retrbinary(f"RETR {filename}", bio.write)

            # Store the file name and its byte content in the results list
            files.append((filename, bio.getvalue()))

        # Close the FTP connection
        ftp.quit()

    except Exception as e:
        logger.exception(f"FTP fetch error: {e}")

    return files


def resolve_private_key(key_string, passphrase=None):
    try:
        # Clean up the key string - handle keys with spaces in header/footer
        key_string = key_string.replace(
            "-----BEGIN OPENSSH PRIVATE KEY----- ",
            "-----BEGIN OPENSSH PRIVATE KEY-----\n",
        )
        key_string = key_string.replace(
            " -----END OPENSSH PRIVATE KEY-----", "\n-----END OPENSSH PRIVATE KEY-----"
        )

        lines = key_string.split("\n")
        if len(lines) == 1:  # Key is all on one line with spaces
            if "-----BEGIN" in key_string and "-----END" in key_string:
                parts = key_string.split("-----")
                header = "-----" + parts[1] + "-----"
                footer = "-----" + parts[3] + "-----"
                body = parts[2].strip()
                # Split body by spaces and rejoin with newlines
                body_parts = body.split(" ")
                key_string = header + "\n" + "\n".join(body_parts) + "\n" + footer
        key_file = StringIO(key_string)
        key_types = [
            ("Ed25519", paramiko.Ed25519Key),
            ("RSA", paramiko.RSAKey),
            ("ECDSA", paramiko.ECDSAKey),
            ("DSS", paramiko.DSSKey),
        ]

        for key_name, key_class in key_types:
            try:
                key_file.seek(0)  # Reset file pointer
                print(f"{key_name}")
                return key_class.from_private_key(key_file)
            except (paramiko.SSHException, ValueError):
                continue
    except:
        raise ValueError(
            "Unable to parse private key. Unsupported key type or invalid format."
        )


# This function connects to an SFTP server using provided credentials,
# retrieves all files from a specified remote directory,
# and returns a list of (filename, file_content, remote_path) tuples along with the sftp client.
# The remote_path is included so files can be deleted after processing if needed.
def fetch_from_sftp(creds, remote_dir):
    files = []
    transport = None
    sftp = None
    try:
        host = creds.get("host")
        username = creds.get("username")
        port = int(creds.get("port", 22))

        if not host or not username:
            raise ValueError("Missing required SFTP credentials: host or username")
        # Set up an SFTP connection using paramiko.Transport
        transport = paramiko.Transport((host, port))  # Connect to SFTP on port 22

        # Auto-detect authentication method based on available credentials
        auth_method = None

        if "password" in creds and creds["password"]:
            auth_method = "password"
            logger.debug("Using password-based authentication for SFTP")
            try:
                transport.connect(
                    username=creds["username"], password=creds["password"]
                )  # Authenticate
                logger.debug("SFTP password authentication successful")
            except Exception as e:
                logger.exception(f"Error during SFTP password authentication: {e}")
                raise e
        elif "private_key" in creds and creds["private_key"]:
            # If no password is provided, assume key-based authentication
            auth_method = "ssh_key"
            logger.debug("Using key-based authentication for SFTP")
            passphrase = creds.get("passphrase", None)

            private_key = resolve_private_key(
                creds["private_key"], passphrase=passphrase
            )

            # Connect using the private key
            transport.connect(username=creds["username"], pkey=private_key)
            logger.debug("SFTP key-based authentication successful")
            # private_key = paramiko.RSAKey.from_private_key_file(
            #     creds["private_key_path"]
            # )
        else:
            available_fields = list(creds.keys())
            raise ValueError(
                f"No valid authentication method found in credentials. Available fields: {available_fields}"
            )
        # Initialize the SFTP client from the transport channel
        sftp = paramiko.SFTPClient.from_transport(transport)

        # List all files in the given remote directory
        for filename in sftp.listdir(remote_dir):
            remote_path = (
                f"{remote_dir}/{filename}"  # Full path to the file on the SFTP server
            )

            try:
                # Open the file in binary read mode
                with sftp.open(remote_path, "rb") as f:
                    file_bytes = f.read()  # Read file content into memory

                    # Append (file name, content, remote path) to the result list
                    files.append((filename, file_bytes, remote_path))

            except Exception as fe:
                # If reading a specific file fails, log the error and continue
                print(f"Failed to fetch SFTP file {filename}: {fe}")

        # Return the collected files and the open sftp client for future actions (like deletion)
        return files, sftp

    except Exception as e:
        logger.exception(f"SFTP fetch error: {e}")
        raise e  # Re-raise the exception to be handled by the caller
        # return [], None


# ------------------ Lambda Handler ------------------


# This main entry point of the code, connects to the database, retrieves all active fetch configurations,
# downloads files from various sources (HTTP, SFTP, FTP), uploads them to S3,
# validates them, logs the result, and cleans up if necessary.
def lambda_handler(event, context):
    # Step 1: Connect to the PostgreSQL database using the RDS secret
    conn = get_db_connection(do_secret_name)
    fetched_files = []  # List to keep track of fetched files
    # Record the start time to include in fetch logs
    start_time = datetime.datetime.now(datetime.UTC)  # Capture start time for logging
    if not conn:
        logger.exception("Database connection failed")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"Database connection error": "Failed to connect to database"}
            ),
        }

    try:
        # Step 2: Fetch all valid fetch_config records for today
        configs = fetch_all_configs(conn)
        if not configs:
            # print("No active fetch_config records found")
            logger.info("No active fetch_config records found")
            # If no configurations are found, return a message indicating no work to do
            return {
                "statusCode": 200,
                "body": json.dumps("No active fetch_config records found"),
            }
        # Step 3: Process each configuration one by one
        for cfg in configs:
            (
                config_id,  # Unique ID for this config
                file_name_mask,  # File name pattern to resolve
                fetch_type,  # Type of source (HTTP, FTP, or SFTP)
                location,  # Path or URL (depends on type)
                secret_name,  # Secret name for credentials
                s3_bucket,  # Target S3 bucket
                s3_path,  # S3 key prefix (folder)
                extract_fetched_file_flag,  # Flag to extract files from ZIP
                add_date_to_file_name_flag,  # Flag to add date to file name
                archive_bucket,  # Archive bucket for source files
                archive_location,  # Archive path for source files
            ) = cfg
            # logger.info(
            #     f"Processing fetch_config_id: {config_id} ({fetch_type}) ({file_name_mask})"
            # )
            # Step 4: Handle HTTP file fetch
            if fetch_type.upper() == "HTTP" and location:
                final_url = resolve_web_url(location, file_name_mask)
                logger.debug(f"Resolved download URL: {final_url}")
                try:
                    response = requests.get(final_url)  # Attempt to download the file
                    if response.status_code == 200:
                        file_name = os.path.basename(final_url)
                        file_data = response.content
                        # Upload and log the file
                        process_and_upload(
                            file_name,
                            file_data,
                            s3_bucket,
                            s3_path,
                            archive_bucket,
                            archive_location,
                            config_id,
                            extract_fetched_file_flag,
                            add_date_to_file_name_flag,
                            conn,
                            start_time,
                        )
                        fetched_files.append(file_name)  # Track fetched files
                        continue  # Skip to next config
                    else:
                        logger.error(
                            f"Failed to download file from HTTP. Status code: {response.status_code}"
                        )
                except Exception as e:
                    logger.exception(f"HTTP request failed: {e}")
                    continue  # Skip to next config
                    # Uncomment the following lines to return an error response
                    # return {
                    #     "statusCode": 500,
                    #     "body": json.dumps({"HTTP request error": str(e)}),
                    # }

            # Step 5: Handle SFTP and FTP file fetch
            # For SFTP and FTP, we need to fetch credentials from AWS Secrets Manager
            creds = get_secret(secret_name)
            if not creds:
                logger.error(f"Missing credentials for secret: {secret_name}")
                continue  # Skip to next config if credentials are missing

            # Step 6: Fetch files from SFTP or FTP
            # For SFTP, we also need to delete files after processing
            if fetch_type.upper() == "SFTP":
                sftp_files, sftp = fetch_from_sftp(creds, location)
                for file_name, file_data, remote_path in sftp_files:
                    success = process_and_upload(
                        file_name,
                        file_data,
                        s3_bucket,
                        s3_path,
                        archive_bucket,
                        archive_location,
                        config_id,
                        extract_fetched_file_flag,
                        add_date_to_file_name_flag,
                        conn,
                        start_time,
                    )
                    fetched_files.append(file_name)  # Track fetched files
                    # If upload succeeded, delete file from SFTP
                    if success:
                        try:
                            sftp.remove(remote_path)
                            logger.debug(f"Deleted from SFTP: {remote_path}")
                        except Exception as de:
                            logger.exception(
                                f"Failed to delete from SFTP: {remote_path}: {de}"
                            )
                            continue  # Skip to next config
                if sftp:
                    sftp.close()
            # Step 7: Handle FTP file fetch
            # For FTP, we don't delete files after processing
            elif fetch_type.upper() == "FTP":
                ftp_files = fetch_from_ftp(creds, location)
                for file_name, file_data in ftp_files:
                    process_and_upload(
                        file_name,
                        file_data,
                        s3_bucket,
                        s3_path,
                        archive_bucket,
                        archive_location,
                        config_id,
                        extract_fetched_file_flag,
                        add_date_to_file_name_flag,
                        conn,
                        start_time,
                    )
                    fetched_files.append(file_name)  # Track fetched files

    except Exception as e:
        logger.exception(f"Unhandled exception during fetch run: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
    finally:
        # Step 9: closing DB connection
        conn.close()
        logger.info(
            f"External Fetch process completed successfully, the fetched files are: {fetched_files}"
        )
        logger.debug("Database connection closed")
        logger.debug("Lambda function completed successfully")
    return {
        "statusCode": 200,
        "body": json.dumps("External Fetch process completed successfully"),
    }


"""
# Uncomment the following lines to test the function locally

lambda_handler(None, None)
"""
