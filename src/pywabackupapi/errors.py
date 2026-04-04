from __future__ import annotations


class BackupError(Exception):
    """Base class for backup discovery and file-copy errors."""


class DirectoryAccessError(BackupError):
    def __init__(self, underlying: Exception):
        self.underlying = underlying
        super().__init__(f"Failed to access backup directory: {underlying}")


class InvalidBackupError(BackupError):
    def __init__(self, url: str, reason: str):
        self.url = url
        self.reason = reason
        super().__init__(f"Invalid backup at {url}: {reason}")


class FileCopyError(BackupError):
    def __init__(self, source: str, destination: str, underlying: Exception):
        self.source = source
        self.destination = destination
        self.underlying = underlying
        super().__init__(f"Failed to copy {source} to {destination}: {underlying}")


class DatabaseErrorWA(Exception):
    """Base class for WhatsApp SQLite access errors."""


class DatabaseConnectionError(DatabaseErrorWA):
    def __init__(self, underlying: Exception):
        self.underlying = underlying
        super().__init__(f"Database connection failed: {underlying}")


class UnsupportedSchemaError(DatabaseErrorWA):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Unsupported database schema: {reason}")


class RecordNotFoundError(DatabaseErrorWA):
    def __init__(self, table: str, record_id: object):
        self.table = table
        self.record_id = record_id
        super().__init__(f"Record not found in {table} with id {record_id}")


class DomainError(Exception):
    """Base class for higher-level WhatsApp interpretation errors."""


class MediaNotFoundError(DomainError):
    def __init__(self, path: str):
        self.path = path
        super().__init__(f"Media not found at {path}")


class OwnerProfileNotFoundError(DomainError):
    def __init__(self):
        super().__init__("Owner profile not found in database")


class UnexpectedDomainError(DomainError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Unexpected error: {reason}")
