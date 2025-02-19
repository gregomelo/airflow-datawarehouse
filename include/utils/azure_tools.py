"""
Module for handling Azure services.

This module provides functions to interact with Azure Blob Storage,
including file uploads, downloads, and listing objects.
"""

import functools
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

from azure.core.exceptions import AzureError, ClientAuthenticationError
from azure.storage.blob import BlobServiceClient


class AzureBlobClient:
    """Client for interacting with Azure Blob Storage or Azurite."""

    def __init__(self, blob_container: str):
        """
        Initialize the Azure Blob client with environment variables.

        Parameters
        ----------
        blob_container : str
            Name of the Azure Blob Storage container to be used.

        Raises
        ------
        RuntimeError
            If authentication fails or initialization fails.
        """
        self._envs = self._load_env_vars()
        self.blob_container = blob_container

        try:
            self._blob_service_client = BlobServiceClient.from_connection_string(
                conn_str=self._envs["azure_storage_connection_string"]
            )
        except (ClientAuthenticationError, AzureError) as e:
            raise RuntimeError("Failed to initialize Azure Blob client") from e

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
        ValueError
            If any required environment variable is missing.
        """
        env_var = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not env_var:
            raise ValueError(
                "Missing required environment variable: AZURE_STORAGE_CONNECTION_STRING"
            )
        return {"azure_storage_connection_string": env_var}

    @staticmethod
    def _handle_azure_errors(func):
        """Handle Azure-related errors in a consistent way."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as e:
                raise FileNotFoundError(str(e)) from e
            except ClientAuthenticationError as e:
                raise RuntimeError(
                    "Authentication failed. Check your Azure credentials."
                ) from e
            except AzureError as e:
                raise RuntimeError(f"Azure Blob operation failed: {e}") from e

        return wrapper

    @_handle_azure_errors
    def upload_file(self, upload_file_path: Union[str, Path], load_folder: str) -> None:
        """Upload a file to Azure Blob Storage."""
        filename = Path(upload_file_path).name
        blob_path = f"{load_folder}/{filename}"

        blob_client = self._blob_service_client.get_blob_client(
            container=self.blob_container, blob=blob_path
        )

        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

    @_handle_azure_errors
    def download_file(self, blob_path: str, local_path: Optional[str] = None) -> bytes:
        """Download a file from Azure Blob Storage."""
        blob_client = self._blob_service_client.get_blob_client(
            container=self.blob_container, blob=blob_path
        )

        download_stream = blob_client.download_blob().readall()

        if local_path:
            with open(local_path, "wb") as download_file:
                download_file.write(download_stream)

        return download_stream

    @_handle_azure_errors
    def list_objects(self, prefix: str) -> List[Dict[str, str]]:
        """List objects in Azure Blob Storage under a specific prefix."""
        container_client = self._blob_service_client.get_container_client(
            self.blob_container
        )
        return list(container_client.list_blobs(name_starts_with=prefix))
