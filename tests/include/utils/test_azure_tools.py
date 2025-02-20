import tempfile
from pathlib import Path
from typing import Generator, Union

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from include.utils.azure_tools import AzureBlobClient


@pytest.fixture
def azure_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Mock Azure credentials for testing.

    Sets an environment variable with a dummy Azure connection string.
    """
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=test;"
        "AccountKey=test;EndpointSuffix=core.windows.net",
    )


@pytest.fixture
def azure_mock(
    azure_credentials: None, mocker
) -> Generator[BlobServiceClient, None, None]:
    """
    Mock an Azure Blob Storage service.

    Yields
    ------
    BlobServiceClient
        A mocked Azure Blob service client.
    """
    mock_blob_service_client = mocker.Mock(spec=BlobServiceClient)
    yield mock_blob_service_client


@pytest.fixture
def azure_client(azure_mock: BlobServiceClient, mocker) -> AzureBlobClient:
    """
    Create a mocked instance of AzureBlobClient.

    Parameters
    ----------
    azure_mock : BlobServiceClient
        The mocked Blob service client fixture.
    mocker : pytest-mock.MockerFixture
        The pytest-mock fixture for creating mocks.

    Returns
    -------
    AzureBlobClient
        An instance of AzureBlobClient for testing.
    """
    mock_container_client = mocker.Mock()
    mock_blob_service_client = mocker.Mock(spec=BlobServiceClient)
    mock_blob_service_client.get_container_client.return_value = mock_container_client

    mocker.patch(
        "include.utils.azure_tools.BlobServiceClient.from_connection_string",
        return_value=mock_blob_service_client,
    )

    return AzureBlobClient(blob_container="test-container")


class TestAzureBlobClient:
    """Test suite for the AzureBlobClient class."""

    def test_upload_file(self, azure_client: AzureBlobClient, mocker) -> None:
        """
        Test uploading a file to Azure Blob Storage.

        Mocks the file upload process and verifies that the upload method
        is called correctly.
        """
        mock_blob_client = mocker.Mock()
        mocker.patch.object(
            azure_client._blob_service_client,
            "get_blob_client",
            return_value=mock_blob_client,
        )

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"test data")
            temp_file.close()

            assert azure_client.upload_file(temp_file.name, "test-folder") is None
            mock_blob_client.upload_blob.assert_called_once()

    def test_upload_files(self, azure_client: AzureBlobClient, mocker) -> None:
        """
        Test uploading multiple files to Azure Blob Storage.

        Mocks the file upload process and verifies that the upload method
        is called correctly for each file in the provided list.
        """
        mock_blob_client = mocker.Mock()
        mocker.patch.object(
            azure_client._blob_service_client,
            "get_blob_client",
            return_value=mock_blob_client,
        )

        with (
            tempfile.NamedTemporaryFile(delete=False) as temp_file1,
            tempfile.NamedTemporaryFile(delete=False) as temp_file2,
        ):
            temp_file1.write(b"test data 1")
            temp_file1.close()
            temp_file2.write(b"test data 2")
            temp_file2.close()

            file_list: list[Union[str, Path]] = [temp_file1.name, temp_file2.name]
            azure_client.upload_files(file_list, "test-folder")

            assert mock_blob_client.upload_blob.call_count == len(file_list)

    def test_download_file(self, azure_client: AzureBlobClient, mocker) -> None:
        """
        Test downloading an existing file from Azure Blob Storage.

        Mocks the file download process and verifies that the returned
        file content matches the expected data.
        """
        mock_blob_client = mocker.Mock()
        mock_blob_client.download_blob.return_value.readall.return_value = b"test data"
        mocker.patch.object(
            azure_client._blob_service_client,
            "get_blob_client",
            return_value=mock_blob_client,
        )

        file_content = azure_client.download_file("test-folder/test-file.txt")
        assert file_content == b"test data"

    def test_download_file_not_found(
        self, azure_client: AzureBlobClient, mocker
    ) -> None:
        """
        Test handling of a missing file in Azure Blob Storage.

        Mocks a scenario where a requested file does not exist, ensuring
        that a RuntimeError is raised with the expected error message.
        """
        mock_blob_client = mocker.Mock()
        mock_blob_client.download_blob.side_effect = ResourceNotFoundError(
            "Blob not found"
        )
        mocker.patch.object(
            azure_client._blob_service_client,
            "get_blob_client",
            return_value=mock_blob_client,
        )

        with pytest.raises(
            RuntimeError, match="Azure Blob operation failed: Blob not found"
        ):
            azure_client.download_file("non-existent-file.txt")

    def test_list_objects(self, azure_client: AzureBlobClient, mocker) -> None:
        """
        Test listing objects in Azure Blob Storage with a prefix.

        Mocks the listing of blobs under a specific prefix and verifies
        that the correct number of objects is returned.
        """
        mock_container_client = mocker.Mock()
        mock_container_client.list_blobs.return_value = [
            {"name": "test-folder/file1.txt"},
            {"name": "test-folder/file2.txt"},
        ]
        mocker.patch.object(
            azure_client._blob_service_client,
            "get_container_client",
            return_value=mock_container_client,
        )

        objects = azure_client.list_objects("test-folder/")
        assert len(objects) == 2
        assert objects[0]["name"].startswith("test-folder/")

    def test_list_objects_no_results(
        self, azure_client: AzureBlobClient, mocker
    ) -> None:
        """
        Test listing objects when no matching files exist.

        Mocks a scenario where no files exist under a given prefix and
        ensures that an empty list is returned.
        """
        mock_container_client = mocker.Mock()
        mock_container_client.list_blobs.return_value = []
        mocker.patch.object(
            azure_client._blob_service_client,
            "get_container_client",
            return_value=mock_container_client,
        )

        assert azure_client.list_objects("empty-folder/") == []

    def test_azure_client_initialization(self, azure_client: AzureBlobClient) -> None:
        """
        Test the initialization of AzureBlobClient.

        Verifies that the AzureBlobClient is initialized with the expected
        attributes.
        """
        assert isinstance(azure_client, AzureBlobClient)
        assert azure_client.blob_container == "test-container"
