"""
Module for handling AWS services.

This module provides functions to interact with AWS S3, including
file uploads, downloads, and listing objects.
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

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
        self._endpoint_url = os.getenv("AWS_S3_ENDPOINT", "").strip() or None
        self.s3_bucket = s3_bucket

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self._envs["aws_access_key_id"],
            aws_secret_access_key=self._envs["aws_secret_access_key"],
            region_name=self._envs["aws_default_region"],
            endpoint_url=self._endpoint_url,  # None = Real AWS, LocalStack if set
        )

    @staticmethod
    def _load_env_vars() -> Dict[str, str]:
        """
        Load and validate required environment variables.

        Returns
        -------
        Dict[str, str]
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
            key for key, value in required_vars.items() if not value
        ]
        if missing_vars:
            logger.error(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
            sys.exit(1)

        return required_vars

    def upload_file(self, upload_file_path: Union[str, Path], load_folder: str) -> bool:
        """
        Upload a file to S3.

        Parameters
        ----------
        upload_file_path : Union[str, Path]
            Path of the file to be uploaded.
        load_folder : str
            Destination folder in S3 where the file will be stored.

        Returns
        -------
        bool
            True if upload succeeds, False otherwise.
        """
        try:
            filename: str = Path(upload_file_path).name
            s3_key: str = f"{load_folder}/{filename}"
            with open(file=upload_file_path, mode="rb") as data:
                self.s3.put_object(Body=data, Bucket=self.s3_bucket, Key=s3_key)
            return True
        except NoCredentialsError:
            logger.error(
                "AWS credentials not found. "
                "Check your environment variables or AWS config."
            )
        except ClientError as e:
            logger.error(f"S3 upload failed: {e}")
        return False

    def download_file(
        self, s3_key: str, local_path: Optional[str] = None
    ) -> Optional[bytes]:
        """
        Download a file from S3 and optionally save it locally.

        Parameters
        ----------
        s3_key : str
            The S3 object key (path in S3).
        local_path : str, optional
            The local file path where the file should be saved.

        Returns
        -------
        Optional[bytes]
            The file content in bytes if successful, None otherwise.
        """
        try:
            response = self.s3.get_object(Bucket=self.s3_bucket, Key=s3_key)
            file_content = response["Body"].read()

            if local_path:
                with open(file=local_path, mode="wb") as download_file:
                    download_file.write(file_content)

            return file_content

        except NoCredentialsError:
            logger.error(
                "AWS credentials not found. "
                "Check your environment variables or AWS config."
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.error(f"File not found in S3: {s3_key}")
            else:
                logger.error(f"S3 download failed: {e}")

        return None

    def list_objects(self, prefix: str) -> List[Dict[str, Any]]:
        """
        List objects in S3 under a specific prefix.

        Parameters
        ----------
        prefix : str
            The prefix to filter objects in the bucket.

        Returns
        -------
        List[Dict[str, Any]]
            List of dictionaries containing S3 object metadata.
            Returns an empty list if no objects are found.
        """
        try:
            response = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
            if "Contents" in response:
                return response["Contents"]
            return []
        except NoCredentialsError:
            logger.error(
                "AWS credentials not found. "
                "Check your environment variables or AWS config."
            )
        except ClientError as e:
            logger.error(f"S3 list objects failed: {e}")
        return []
