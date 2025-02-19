"""
Module for handling AWS services.

This module provides functions to interact with AWS S3, including
file uploads, downloads, and listing objects.
"""

import functools
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

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

        Raises
        ------
        RuntimeError
            If AWS credentials are missing or S3 initialization fails.
        """
        self._envs = self._load_env_vars()
        self._endpoint_url = os.getenv("AWS_S3_ENDPOINT", "").strip() or None
        self.s3_bucket = s3_bucket

        try:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=self._envs["aws_access_key_id"],
                aws_secret_access_key=self._envs["aws_secret_access_key"],
                region_name=self._envs["aws_default_region"],
                endpoint_url=self._endpoint_url,  # None = Real AWS, LocalStack if set
            )
        except (NoCredentialsError, ClientError) as e:
            raise RuntimeError("Failed to initialize AWS S3 client.") from e

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
        ValueError
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
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        return required_vars

    @staticmethod
    def _handle_s3_errors(func):
        """Handle S3-related errors in a consistent way."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as e:
                raise FileNotFoundError(str(e)) from e
            except NoCredentialsError as e:
                raise RuntimeError(
                    "AWS credentials not found. Check environment variables or AWS config."
                ) from e
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "NoSuchKey":
                    raise FileNotFoundError(
                        "The specified file was not found in S3."
                    ) from e
                raise RuntimeError(f"S3 operation failed: {e}") from e

        return wrapper

    @_handle_s3_errors
    def upload_file(self, upload_file_path: Union[str, Path], load_folder: str) -> None:
        """
        Upload a file to S3.

        Parameters
        ----------
        upload_file_path : Union[str, Path]
            Path of the file to be uploaded.
        load_folder : str
            Destination folder in S3 where the file will be stored.

        Raises
        ------
        FileNotFoundError
            If the file does not exist locally.
        RuntimeError
            If authentication fails or the upload fails.
        """
        filename = Path(upload_file_path).name
        s3_key = f"{load_folder}/{filename}"

        with open(upload_file_path, "rb") as data:
            self.s3.put_object(Body=data, Bucket=self.s3_bucket, Key=s3_key)

    @_handle_s3_errors
    def download_file(self, s3_key: str, local_path: Optional[str] = None) -> bytes:
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
        bytes
            The file content.

        Raises
        ------
        FileNotFoundError
            If the file does not exist in S3.
        RuntimeError
            If authentication fails or the download fails.
        """
        response = self.s3.get_object(Bucket=self.s3_bucket, Key=s3_key)
        file_content = response["Body"].read()

        if local_path:
            with open(local_path, "wb") as download_file:
                download_file.write(file_content)

        return file_content

    @_handle_s3_errors
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

        Raises
        ------
        RuntimeError
            If authentication fails or the listing operation fails.
        """
        response = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
        return response.get("Contents", [])
