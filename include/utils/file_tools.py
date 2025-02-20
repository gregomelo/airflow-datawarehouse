"""
Module for handling temporary directories.

This module provides functions to create and delete temporary directories
using the `tempfile.TemporaryDirectory` class.
"""

from os import listdir
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import List, Union

from .log_tools import logger


def create_temp_folder(temp_name: str) -> TemporaryDirectory:
    """
    Create a temporary directory with a given suffix.

    Parameters
    ----------
    temp_name : str
        The suffix to append to the temporary directory's name.

    Returns
    -------
    temp_folder
        An instance of `TemporaryDirectory` representing the created temporary folder.
    """
    temp_folder = TemporaryDirectory(suffix=f"_{temp_name}")

    logger.info(f"Create temporary folder {temp_folder.name}")

    return temp_folder


def delete_temp_folder(temp_folder: str) -> None:
    """
    Delete a temporary directory.

    This function ensures that the temporary directory and its contents
    are properly removed.

    Parameters
    ----------
    temp_folder : str
        The temporary directory to be deleted.
    """
    rmtree(temp_folder, ignore_errors=True)
    logger.info(f"Remove temporary folder {temp_folder}")


def list_temp_folder(temp_folder: Union[TemporaryDirectory, Path, str]) -> List[str]:
    """
    List files in a temporary directory.

    This function lists all files in a temporary directory.

    Parameters
    ----------
    temp_folder : Union[TemporaryDirectory, Path]
        The temporary directory instance or a Path object.

    Returns
    -------
    listdir : List[str]
        A list with all files in the directory.
    """
    return listdir(str(temp_folder))  # Ensure it's a string path


def storage_path(layer: str, source_name: str, source_surname: str) -> str:
    """
    Generate a storage path based on the provided layer, source name, and source surname.

    Parameters
    ----------
    layer : str
        The storage layer (e.g., "bronze", "silver", "gold").
    source_name : str
        The main name of the data source.
    source_surname : str
        The secondary name of the data source.

    Returns
    -------
    str
        The formatted storage path in the format "{layer}/{source_name}/{source_surname}".
    """
    return f"{layer}/{source_name}/{source_surname}"


if __name__ == "__main__":
    my_temp_folder = create_temp_folder("teste")

    input("Press Enter to delete the temporary folder...")

    print(list_temp_folder(my_temp_folder.name))

    delete_temp_folder(my_temp_folder.name)
