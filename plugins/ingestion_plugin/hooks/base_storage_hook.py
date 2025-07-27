from abc import ABC, abstractmethod

class BaseStorageHook(ABC):

    @abstractmethod
    def copy_file(self, source_identifier: str, destination_identifier: str, new_name: str = None):
        """
        Copy a file from source to destination.

        Args:
            source_identifier (str): Source file ID or path.
            destination_identifier (str): Destination folder ID or path.
            new_name (str, optional): New name for the copied file.

        Returns:
            Any: Result of the copy operation.
        """
        pass

    @abstractmethod
    def list_files(self, folder_id: str):
        """
        List files in a folder.

        Args:
            folder_id (str): Folder ID.

        Returns:
            list: List of files in the folder.
        """
        pass

    @abstractmethod
    def file_exists(self, file_name: str, folder_id: str) -> bool:
        """
        Check if a file exists in a folder.

        Args:
            file_name (str): Name of the file to check.
            folder_id (str): Folder ID.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        pass

    @abstractmethod
    def get_or_create_folder(self, folder_name: str, parent_folder_id: str):
        """
        Get a folder by name or create it if it doesn't exist.

        Args:
            folder_name (str): Name of the folder to get or create.
            parent_folder_id (str): ID of the parent folder.

        Returns:
            str: ID of the folder.
        """
        pass

    @abstractmethod
    def get_file_by_name(self, file_name: str, folder_id: str):
        """
        Get a file by name from a specific folder.

        Args:
            file_name (str): Name of the file to retrieve.
            folder_id (str): ID of the folder to search within.

        Returns:
            dict: File metadata if found, None otherwise.
        """
        pass
