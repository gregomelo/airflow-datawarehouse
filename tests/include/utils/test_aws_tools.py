from io import BytesIO
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
        data: BytesIO = BytesIO(b"test data")
        s3_key: str = "test-folder/test-file.txt"

        assert s3_client.upload_file(data, s3_key) is True

        # Verify file exists in S3
        s3: boto3.client = boto3.client("s3")
        response = s3.get_object(Bucket=s3_client.s3_bucket, Key=s3_key)
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

        file = s3_client.download_file(s3_key)
        assert file is not None
        assert file["Body"].read() == b"test data"

    def test_download_file_not_found(self, s3_client: S3Client) -> None:
        """
        Test downloading a non-existing file from S3.

        Ensures None is returned when trying to download a missing file.
        """
        assert s3_client.download_file("non-existent-file.txt") is None

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
