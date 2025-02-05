"""
Module for handling temporary directories.

This module provides functions to create and delete temporary directories
using the `tempfile.TemporaryDirectory` class.
"""

from tempfile import TemporaryDirectory

from loguru import logger


def create_temp_folder(temp_name: str) -> TemporaryDirectory:
    """
    Create a temporary directory with a given suffix.

    Parameters
    ----------
    temp_name : str
        The suffix to append to the temporary directory's name.

    Returns
    -------
    TemporaryDirectory
        An instance of `TemporaryDirectory` representing the created temporary folder.
    """
    temp_folder = TemporaryDirectory(suffix=f"_{temp_name}")

    logger.info(f"Create temporary folder {temp_folder.name}")

    return temp_folder


def delete_temp_folder(temp_folder: TemporaryDirectory) -> None:
    """
    Delete a temporary directory.

    This function ensures that the temporary directory and its contents
    are properly removed.

    Parameters
    ----------
    temp_folder : TemporaryDirectory
        The temporary directory instance to be deleted.
    """
    temp_folder.cleanup()
    logger.info(f"Remove temporary folder {temp_folder.name}")


if __name__ == "__main__":
    my_temp_folder = create_temp_folder("teste")

    input("Press Enter to delete the temporary folder...")

    delete_temp_folder(my_temp_folder)
