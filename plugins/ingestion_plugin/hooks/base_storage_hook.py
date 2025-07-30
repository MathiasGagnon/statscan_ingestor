from abc import ABC, abstractmethod

class BaseStorageHook(ABC):

    @abstractmethod
    def copy_file(self, source_identifier: str, destination_identifier: str, new_name: str = None):
        """
        Copies a file to a new parent folder in Google Drive.

        :param source_identifier: ID of the source file.
        :param destination_identifier: ID of the destination folder.
        :param new_name: Optional new name for the copied file.
        :return: Metadata dictionary of the copied file.
        """
        pass

    @abstractmethod
    def list_files(self, folder_id: str) -> list:
        """
        List files in a folder.

        :param folder_id: Folder ID.

        :return: List of files in the folder.
        """
        pass

    @abstractmethod
    def file_exists(self, file_name: str, folder_id: str) -> bool:
        """
        Check if a file exists in a folder.

        :param file_name: Name of the file to check.
        :param folder_id: Folder ID.

        :return: True if the file exists, False otherwise.
        """
        pass

    @abstractmethod
    def get_or_create_folder(self, folder_name: str, parent_folder_id: str) -> str:
        """
        Get a folder by name or create it if it doesn't exist.

        :param folder_name: Name of the folder to get or create.
        :param parent_folder_id: ID of the parent folder.

        :return: ID of the folder.
        """
        pass

    @abstractmethod
    def get_file_by_name(self, file_name: str, folder_id: str) -> str:
        """
        Get a file by name from a specific folder.

        :param file_name: Name of the file to retrieve.
        :param folder_id: ID of the folder to search within.

        :return: File metadata if found, None otherwise.
        """
        pass
