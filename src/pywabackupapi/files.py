from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path

from .errors import FileCopyError


FilenameAndHash = tuple[str, str]


class FileUtils:
    @staticmethod
    def latest_file(
        prefix_filename: str,
        file_extension: str,
        files: list[FilenameAndHash],
    ) -> FilenameAndHash | None:
        latest: FilenameAndHash | None = None
        latest_timestamp = 0
        for item in files:
            timestamp = FileUtils.extract_time_suffix(prefix_filename, file_extension, item[0])
            if timestamp is not None and timestamp > latest_timestamp:
                latest_timestamp = timestamp
                latest = item
        return latest

    @staticmethod
    def extract_time_suffix(
        prefix_filename: str,
        file_extension: str,
        file_name: str,
    ) -> int | None:
        pattern = re.compile(re.escape(prefix_filename) + r"-(\d+)\." + re.escape(file_extension))
        match = pattern.search(file_name)
        if match is None:
            return None
        return int(match.group(1))


@dataclass(slots=True)
class MediaCopier:
    backup: "IPhoneBackup"
    delegate: object | None = None

    def copy(self, hash_file: str, file_name: str, directory: Path | None) -> str:
        if directory is not None:
            target_url = directory / file_name
            self._copy_if_needed(hash_file, target_url)

        if self.delegate is not None and hasattr(self.delegate, "didWriteMediaFile"):
            self.delegate.didWriteMediaFile(file_name)
        return file_name

    def _copy_if_needed(self, hash_file: str, target_url: Path) -> None:
        source_url = self.backup.getUrl(hash_file)
        if target_url.exists():
            return

        try:
            shutil.copy2(source_url, target_url)
        except Exception as error:  # pragma: no cover - exercised through public API tests
            raise FileCopyError(str(source_url), str(target_url), error) from error
