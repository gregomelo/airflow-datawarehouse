"""
Module for handling AWS services.

This module provides functions to interact with AWS services.
"""

import os
import sys
from io import BytesIO
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from loguru import logger

# Load environment variables from a .env file
load_dotenv()


class S3Client:
    """Client for interacting with AWS S3 or LocalStack."""

    def __init__(self, s3_bucket: str):
        """
        Initialize the S3 client with environment variables.

        Parameters
        ----------
        s3_bucket : str
            Name of the S3 bucket to be used.
        """
        self._envs = self._load_env_vars()
        self.endpoint_url = os.getenv("AWS_S3_ENDPOINT", "").strip() or None
        self.s3_bucket = s3_bucket

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self._envs["aws_access_key_id"],
            aws_secret_access_key=self._envs["aws_secret_access_key"],
            region_name=self._envs["aws_default_region"],
            endpoint_url=self.endpoint_url,  # None = Real AWS, LocalStack if set
        )

        logger.info(
            f"Connected to {'LocalStack' if self.endpoint_url else 'AWS'} "
            f"S3 - Bucket: {self.s3_bucket}"
        )

    @staticmethod
    def _load_env_vars() -> Dict[str, str]:
        """
        Load and validate required environment variables.

        Returns
        -------
        dict
            Dictionary containing AWS credentials and region.

        Raises
        ------
        SystemExit
            If any required environment variable is missing.
        """
        required_vars: Dict[str, str] = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "aws_default_region": os.getenv("AWS_REGION", "us-east-1"),
        }

        missing_vars: List[str] = [
            key for key, value in required_vars.items() if value == ""
        ]
        if missing_vars:
            logger.error(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
            sys.exit(1)

        return required_vars

    def upload_file(self, data: BytesIO, s3_key: str) -> bool:
        """
        Upload a file-like object to S3.

        Parameters
        ----------
        data : BytesIO
            The data to upload.
        s3_key : str
            The S3 object key (destination path in S3).

        Returns
        -------
        bool
            True if upload succeeds, False otherwise.
        """
        try:
            self.s3.put_object(Body=data.getvalue(), Bucket=self.s3_bucket, Key=s3_key)
            logger.info(f"File uploaded successfully: {s3_key}")
            return True
        except NoCredentialsError:
            logger.error(
                "AWS credentials not found. "
                "Check your environment variables or AWS config."
            )
        except ClientError as e:
            logger.error(f"S3 upload failed: {e}")
        return False

    def download_file(self, s3_key: str) -> Optional[dict]:
        """
        Download a file from S3.

        Parameters
        ----------
        s3_key : str
            The S3 object key (path in S3).

        Returns
        -------
        dict or None
            The S3 object data if successful, None otherwise.
        """
        try:
            file = self.s3.get_object(Bucket=self.s3_bucket, Key=s3_key)
            return file
        except NoCredentialsError:
            logger.error(
                "AWS credentials not found."
                "Check your environment variables or AWS config."
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.error(f"File not found in S3: {s3_key}")
            else:
                logger.error(f"S3 download failed: {e}")
        return None

    def list_objects(self, prefix: str) -> List[dict]:
        """
        List objects in S3 under a specific prefix.

        Parameters
        ----------
        prefix : str
            The prefix to filter objects in the bucket.

        Returns
        -------
        list of dict
            List of S3 objects, or an empty list if none found.
        """
        try:
            response = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
            if "Contents" in response:
                return response["Contents"]
            logger.info("No objects found in the specified prefix.")
            return []
        except NoCredentialsError:
            logger.error(
                "AWS credentials not found."
                "Check your environment variables or AWS config."
            )
        except ClientError as e:
            logger.error(f"S3 list objects failed: {e}")
        return []


# Example Usage:
if __name__ == "__main__":
    s3_bucket_test: str = os.getenv("AWS_S3_BUCKET", "test-bucket")
    s3_client = S3Client(s3_bucket=s3_bucket_test)

    # Example: Upload a simple text file
    test_data = BytesIO(b"Hello, S3!")
    s3_client.upload_file(test_data, "test-folder/hello.txt")

    # Example: List objects in a folder
    objects = s3_client.list_objects("test-folder/")
    logger.info(f"Objects in folder: {objects}")

    # Example: Download a file
    file_content = s3_client.download_file("test-folder/hello.txt")
    if file_content:
        logger.info(f"File content: {file_content['Body'].read().decode()}")
