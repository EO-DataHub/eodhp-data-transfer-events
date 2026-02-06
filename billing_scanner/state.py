import fcntl
import json
import os
import types
from typing import Self


class ScannerState:
    """A context manager for managing the state of processed log files.

    This class locks the state file upon entry, loads the processed file keys
    into a set, and then writes the updated state back to the file upon exit.
    """

    def __init__(self, file_location: str) -> None:
        self.file_location = file_location
        self.last_processed = ""

    def __enter__(self) -> Self:
        if not os.path.exists(self.file_location):
            with open(self.file_location, "w") as f:
                json.dump({"last_processed": ""}, f)

        self.f = open(self.file_location, "r+")
        fcntl.flock(self.f, fcntl.LOCK_EX)
        try:
            self.f.seek(0)
            data = json.load(self.f)
            self.last_processed = data.get("last_processed", "")
        except json.JSONDecodeError:
            self.last_processed = ""
        return self

    def get_last_processed_key(self) -> str:
        """Return the last processed."""
        return self.last_processed

    def mark_last_processed(self, file_key: str) -> None:
        """Mark a file key as processed."""
        self.last_processed = file_key

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.f.seek(0)
        json.dump({"last_processed": self.last_processed}, self.f, indent=2)
        self.f.truncate()
        fcntl.flock(self.f, fcntl.LOCK_UN)
        self.f.close()
