import tempfile
from typing import Generator

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from include.utils.azure_tools import AzureBlobClient


@pytest.fixture
def azure_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Mock Azure credentials for testing.

    This fixture sets an environment variable with a dummy Azure connection string.
    """
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
    )


@pytest.fixture
def azure_mock(
    azure_credentials: None, mocker
) -> Generator[BlobServiceClient, None, None]:
    """
    Start a mocked Azure Blob Storage service.

    Yields
    ------
    BlobServiceClient
        Mocked Blob service client.
    """
    mock_blob_service_client = mocker.Mock(spec=BlobServiceClient)
    yield mock_blob_service_client


@pytest.fixture
def azure_client(azure_mock: BlobServiceClient, mocker) -> AzureBlobClient:
    """
    Create an instance of AzureBlobClient using the mocked Azure service.

    Parameters
    ----------
    azure_mock : BlobServiceClient
        Mocked Blob service client fixture.
    mocker : pytest-mock.MockerFixture
        Fixture for creating mocks.

    Returns
    -------
    AzureBlobClient
        Instance of AzureBlobClient for testing.
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

        Ensures that the file is correctly uploaded and handled.
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

            assert azure_client.upload_file(temp_file.name, "test-folder") is True
            mock_blob_client.upload_blob.assert_called_once()

    def test_download_file(self, azure_client: AzureBlobClient, mocker) -> None:
        """
        Test downloading an existing file from Azure Blob Storage.

        Ensures the file content matches what was uploaded.
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
        Test downloading a non-existing file from Azure Blob Storage.

        Ensures None is returned when trying to download a missing file.
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

        assert azure_client.download_file("non-existent-file.txt") is None

    def test_list_objects(self, azure_client: AzureBlobClient, mocker) -> None:
        """
        Test listing objects in Azure Blob Storage with a specific prefix.

        Ensures the correct number of objects are returned under a given prefix.
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
        Test listing objects in Azure Blob Storage when no files match the prefix.

        Ensures an empty list is returned when no objects exist under the prefix.
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
        Test AzureBlobClient initialization.

        Ensures the AzureBlobClient is initialized correctly with the expected attributes.
        """
        assert isinstance(azure_client, AzureBlobClient)
        assert azure_client.blob_container == "test-container"
