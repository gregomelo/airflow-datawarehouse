"""
Module for handling Azure services.

This module provides functions to interact with Azure Blob Storage,
including file uploads, downloads, and listing objects.
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Union

from azure.core.exceptions import AzureError, ClientAuthenticationError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from loguru import logger

# Load environment variables from a .env file
load_dotenv()


class AzureBlobClient:
    """Client for interacting with Azure Blob Storage or Azurite."""

    def __init__(self, blob_container: str):
        """
        Initialize the Azure Blob client with environment variables.

        Parameters
        ----------
        blob_container : str
            Name of the Azure Blob Storage container to be used.
        """
        self._envs = self._load_env_vars()
        self.blob_container = blob_container

        try:
            self._blob_service_client = BlobServiceClient.from_connection_string(
                conn_str=self._envs["azure_storage_connection_string"]
            )
        except ClientAuthenticationError:
            logger.error("Authentication failed. Check your Azure Storage credentials.")
            sys.exit(1)
        except AzureError as e:
            logger.error(f"Failed to initialize Azure Blob client: {e}")
            sys.exit(1)

    @staticmethod
    def _load_env_vars() -> Dict[str, str]:
        """
        Load and validate required environment variables.

        Returns
        -------
        Dict[str, str]
            A dictionary containing the required Azure Blob Storage credentials.

        Raises
        ------
        SystemExit
            If any required environment variable is missing.
        """
        required_vars: Dict[str, str] = {
            "azure_storage_connection_string": os.getenv(
                "AZURE_STORAGE_CONNECTION_STRING", ""
            )
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
        Upload a file to Azure Blob Storage.

        Parameters
        ----------
        upload_file_path : Union[str, Path]
            The local file path of the file to be uploaded.
        load_folder : str
            The destination folder in Azure Blob Storage where the file will be stored.

        Returns
        -------
        bool
            True if the upload is successful, False otherwise.
        """
        try:
            filename: str = Path(upload_file_path).name
            blob_path: str = f"{load_folder}/{filename}"
            blob_client = self._blob_service_client.get_blob_client(
                container=self.blob_container, blob=blob_path
            )

            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            return True
        except FileNotFoundError:
            logger.error(f"File not found: {upload_file_path}")
        except ClientAuthenticationError:
            logger.error("Authentication failed. Check your Azure credentials.")
        except AzureError as e:
            logger.error(f"Azure Blob upload failed: {e}")
        return False

    def download_file(
        self, blob_path: str, local_path: Optional[str] = None
    ) -> Optional[bytes]:
        """
        Download a file from Azure Blob Storage and optionally save it locally.

        Parameters
        ----------
        blob_path : str
            The path of the file in Azure Blob Storage.
        local_path : str, optional
            The local file path where the downloaded file should be saved.
            If None, the file content is returned as bytes.

        Returns
        -------
        Optional[bytes]
            The file content in bytes if successful, or None if an error occurs.
        """
        try:
            blob_client = self._blob_service_client.get_blob_client(
                container=self.blob_container, blob=blob_path
            )

            download_stream = blob_client.download_blob().readall()

            if local_path:
                with open(local_path, "wb") as download_file:
                    download_file.write(download_stream)

            return download_stream
        except ClientAuthenticationError:
            logger.error("Authentication failed. Check your Azure credentials.")
        except AzureError as e:
            logger.error(f"Azure Blob download failed: {e}")
        return None

    def list_objects(self, prefix: str) -> List[Dict[str, str]]:
        """
        List objects in Azure Blob Storage under a specific prefix.

        Parameters
        ----------
        prefix : str
            The prefix used to filter objects in the container.

        Returns
        -------
        List[Dict[str, str]]
            A list of dictionaries containing metadata of the found blob objects.
            Returns an empty list if no objects match the prefix.
        """
        try:
            container_client = self._blob_service_client.get_container_client(
                self.blob_container
            )
            blobs = container_client.list_blobs(name_starts_with=prefix)

            object_list = list(blobs)

            return object_list
        except ClientAuthenticationError:
            logger.error("Authentication failed. Check your Azure credentials.")
        except AzureError as e:
            logger.error(f"Azure Blob list operation failed: {e}")
        return []
