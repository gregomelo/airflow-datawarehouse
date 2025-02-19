import tempfile
from pathlib import Path
from typing import Generator

import boto3
import pytest
from moto import mock_aws

from include.utils.aws_tools import S3Client


@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Mock AWS credentials for testing.

    This fixture sets environment variables to provide dummy AWS credentials.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_REGION", "us-east-1")


@pytest.fixture
def s3_mock(aws_credentials: None) -> Generator[boto3.client, None, None]:
    """
    Start and stop the moto AWS mock for S3.

    Yields
    ------
    boto3.client
        Mocked S3 client.
    """
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_client(s3_mock: boto3.client, monkeypatch: pytest.MonkeyPatch) -> S3Client:
    """
    Create an instance of S3Client using the mocked S3 service.

    Parameters
    ----------
    s3_mock : boto3.client
        Mocked S3 client fixture.
    monkeypatch : pytest.MonkeyPatch
        Fixture for modifying environment variables.

    Returns
    -------
    S3Client
        Instance of S3Client for testing.
    """
    bucket_name: str = "test-bucket"
    s3_mock.create_bucket(Bucket=bucket_name)

    # Ensure the S3Client doesn't use any real AWS endpoint
    monkeypatch.delenv("AWS_S3_ENDPOINT", raising=False)

    return S3Client(s3_bucket=bucket_name)


class TestS3Client:
    """Test suite for the S3Client class."""

    def test_upload_file(self, s3_client: S3Client) -> None:
        """
        Test uploading a file to S3.

        Ensures that the file is correctly uploaded and can be retrieved.
        """

        # Ensure the bucket exists in the mocked S3 service
        s3_client.s3.create_bucket(Bucket=s3_client.s3_bucket)

        # Create a temporary file with test data
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"test data")
            temp_file.close()

            # Upload file to S3
            assert s3_client.upload_file(temp_file.name, "test-folder") is None

            # Extract only the filename (not the full path)
            s3_key: str = f"test-folder/{Path(temp_file.name).name}"

            # Verify file exists in S3 using the correct S3 client instance
            response = s3_client.s3.get_object(Bucket=s3_client.s3_bucket, Key=s3_key)
            assert response["Body"].read() == b"test data"

    def test_download_file(self, s3_client: S3Client) -> None:
        """
        Test downloading an existing file from S3.

        Ensures the file content matches what was uploaded.
        """
        s3_key: str = "test-folder/test-file.txt"
        s3_client.s3.put_object(
            Bucket=s3_client.s3_bucket, Key=s3_key, Body=b"test data"
        )

        file_content = s3_client.download_file(s3_key)
        assert file_content is not None
        assert file_content == b"test data"

    def test_download_file_not_found(self, s3_client: S3Client) -> None:
        """
        Test downloading a non-existing file from S3.

        Ensures None is returned when trying to download a missing file.
        """
        with pytest.raises(
            FileNotFoundError, match="The specified file was not found in S3."
        ):
            s3_client.download_file("non-existent-file.txt")

    def test_list_objects(self, s3_client: S3Client) -> None:
        """
        Test listing objects in S3 with a specific prefix.

        Ensures the correct number of objects are returned under a given prefix.
        """
        prefix: str = "test-folder/"
        s3_client.s3.put_object(
            Bucket=s3_client.s3_bucket, Key=f"{prefix}file1.txt", Body=b"data1"
        )
        s3_client.s3.put_object(
            Bucket=s3_client.s3_bucket, Key=f"{prefix}file2.txt", Body=b"data2"
        )

        objects = s3_client.list_objects(prefix)
        assert len(objects) == 2
        assert objects[0]["Key"].startswith(prefix)

    def test_list_objects_no_results(self, s3_client: S3Client) -> None:
        """
        Test listing objects in S3 when no files match the prefix.

        Ensures an empty list is returned when no objects exist under the prefix.
        """
        assert s3_client.list_objects("empty-folder/") == []

    def test_s3_client_initialization(self, s3_client: S3Client) -> None:
        """
        Test S3Client initialization.

        Ensures the S3 client is initialized correctly with the expected attributes.
        """
        assert isinstance(s3_client, S3Client)
        assert s3_client.s3_bucket == "test-bucket"
