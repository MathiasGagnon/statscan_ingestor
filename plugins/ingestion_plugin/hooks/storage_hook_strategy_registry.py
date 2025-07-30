from typing import Dict, Type

from base_storage_hook import BaseStorageHook
from google_drive_hook import GoogleDriveHook

STORAGE_HOOK_STRATEGY_REGISTRY: Dict[str, Type[BaseStorageHook]] = {
    'google_drive': GoogleDriveHook,
}
