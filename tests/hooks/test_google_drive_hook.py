import pytest
from plugins.ingestion_plugin.hooks.google_drive_hook import GoogleDriveHook
from googleapiclient.errors import HttpError

GOOGLEDRIVEHOOK_PATH = "plugins.ingestion_plugin.hooks.google_drive_hook"

#TODO: Ajouter la doc string, faire des tests parametises, assert les logs, autospec = true, assert param values, cover edge cases, 

@pytest.fixture
def mock_credentials(mocker):
    return mocker.Mock(name="CredentialsMock")


@pytest.fixture
def mock_service(mocker):
    return mocker.Mock(name="GoogleDriveServiceMock")

@pytest.fixture
def hook():
    return object.__new__(GoogleDriveHook)

def make_http_error(mocker):
    fake_resp = mocker.Mock(name="BadRequestResponseMock")
    fake_resp.reason = "Bad Request"
    fake_resp.status = 400
    return HttpError(resp=fake_resp, content=b"error")


def test_google_drive_hook_initializes_service(mocker, mock_service):
    """
    Ensure __init__ triggers _get_service and sets self.service.
    """
    mocker.patch(
        f"{GOOGLEDRIVEHOOK_PATH}.GoogleDriveHook._get_service",
        return_value=mock_service
    )

    hook = GoogleDriveHook()

    assert hook.service == mock_service


def test_google_drive_hook_get_service_success(mocker, mock_credentials, mock_service):
    """
    Verify _get_service builds the correct Google Drive service.
    """
    mock_creds_from_file = mocker.patch(
        f"{GOOGLEDRIVEHOOK_PATH}.service_account.Credentials.from_service_account_file",
        return_value=mock_credentials
    )

    mock_build = mocker.patch(
        f"{GOOGLEDRIVEHOOK_PATH}.build",
        return_value=mock_service,
        autospec=True
    )

    hook = object.__new__(GoogleDriveHook)

    service = hook._get_service()

    mock_creds_from_file.assert_called_once()
    mock_build.assert_called_once_with("drive", "v3", credentials=mock_credentials)
    assert service == mock_service


def test_google_drive_hook_get_service_failure_logs_and_raises(mocker, caplog):
    """
    If credentials loading fails, _get_service should log and raise.
    """
    caplog.set_level("ERROR")

    mocker.patch(
        f"{GOOGLEDRIVEHOOK_PATH}.service_account.Credentials.from_service_account_file",
        side_effect=Exception("Invalid credentials")
    )

    mocker.patch(f"{GOOGLEDRIVEHOOK_PATH}.build")

    hook = object.__new__(GoogleDriveHook)  # Skip __init__

    with pytest.raises(Exception, match="Invalid credentials"):
        hook._get_service()

    assert "Failed to initialize Google Drive service: Invalid credentials" in caplog.text

def test_list_files_returns_files(mocker, hook, mock_service):
    folder_id = "folder123"
    expected_files = [{"id": "1", "name": "test.txt"}]

    mock_list = mocker.Mock()
    mock_list.execute.return_value = {"files": expected_files}
    mock_service.files.return_value.list.return_value = mock_list
    hook.service = mock_service

    result = hook.list_files(folder_id)

    mock_service.files.assert_called_once()
    assert result == expected_files


def test_list_files_returns_none_if_empty(mocker, hook, mock_service):
    mock_list = mocker.Mock()
    mock_list.execute.return_value = {"files": []}
    mock_service.files.return_value.list.return_value = mock_list
    hook.service = mock_service

    result = hook.list_files("folder123")

    assert result is None


def test_list_files_raises_on_error(mocker, hook, mock_service):
    mock_service.files.return_value.list.side_effect = mock_service.files.return_value.list.side_effect = make_http_error(mocker)

    hook.service = mock_service

    with pytest.raises(HttpError):
        hook.list_files("folder123")


def test_copy_file_success(mocker, hook, mock_service):
    expected_file = {"id": "copy123"}
    mock_copy = mocker.Mock()
    mock_copy.execute.return_value = expected_file
    mock_service.files.return_value.copy.return_value = mock_copy
    hook.service = mock_service

    result = hook.copy_file("source123", "dest456", "newname")

    mock_service.files.return_value.copy.assert_called_once()
    assert result == expected_file


def test_copy_file_raises_on_error(mocker, hook, mock_service):
    mock_service.files.return_value.copy.side_effect = mock_service.files.return_value.list.side_effect = make_http_error(mocker)
    hook.service = mock_service

    with pytest.raises(HttpError):
        hook.copy_file("source", "dest")


def test_get_or_create_folder_returns_existing(mocker, hook, mock_service):
    folder_id = "folderXYZ"
    mock_list = mocker.Mock()
    mock_list.execute.return_value = {"files": [{"id": folder_id, "name": "existing"}]}
    mock_service.files.return_value.list.return_value = mock_list
    hook.service = mock_service

    result = hook.get_or_create_folder("existing", "parent123")

    assert result == folder_id


def test_get_or_create_folder_creates_new(mocker, hook, mock_service):
    new_folder_id = "new-folder-001"

    mock_list = mocker.Mock()
    mock_list.execute.return_value = {"files": []}
    mock_service.files.return_value.list.return_value = mock_list

    mock_create = mocker.Mock()
    mock_create.execute.return_value = {"id": new_folder_id}
    mock_service.files.return_value.create.return_value = mock_create

    hook.service = mock_service

    result = hook.get_or_create_folder("new-folder", "parent123")

    assert result == new_folder_id


def test_get_or_create_folder_raises_on_error(mocker, hook, mock_service):
    mock_service.files.return_value.list.side_effect = mock_service.files.return_value.list.side_effect = make_http_error(mocker)
    hook.service = mock_service

    with pytest.raises(HttpError):
        hook.get_or_create_folder("fail-folder", "parent")


def test_get_file_by_name_found(mocker, hook, mock_service):
    expected_file = {"id": "file001", "name": "target.txt"}
    mock_list = mocker.Mock()
    mock_list.execute.return_value = {"files": [expected_file]}
    mock_service.files.return_value.list.return_value = mock_list
    hook.service = mock_service

    result = hook.get_file_by_name("target.txt", "parent123")

    assert result == expected_file


def test_get_file_by_name_not_found(mocker, hook, mock_service):
    mock_list = mocker.Mock()
    mock_list.execute.return_value = {"files": []}
    mock_service.files.return_value.list.return_value = mock_list
    hook.service = mock_service

    result = hook.get_file_by_name("missing.txt", "parent123")

    assert result is None


def test_get_file_by_name_raises_on_error(mocker, hook, mock_service):
    mock_service.files.return_value.list.side_effect = mock_service.files.return_value.list.side_effect = make_http_error(mocker)
    hook.service = mock_service

    with pytest.raises(HttpError):
        hook.get_file_by_name("file.txt", "parent")


def test_file_exists_true(mocker, hook):
    mocker.patch.object(hook, "get_file_by_name", return_value={"id": "abc"})

    assert hook.file_exists("file", "parent") is True


def test_file_exists_false(mocker, hook):
    mocker.patch.object(hook, "get_file_by_name", return_value=None)

    assert hook.file_exists("file", "parent") is False


def test_file_exists_raises(mocker, hook):
    mocker.patch.object(hook, "get_file_by_name", side_effect=Exception("fail"))

    with pytest.raises(Exception, match="fail"):
        hook.file_exists("file", "parent")
