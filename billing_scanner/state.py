import fcntl
import json
import os


class ScannerState:
    """
    A context manager for managing the state of processed log files.

    This class locks the state file upon entry, loads the processed file keys
    into a set, and then writes the updated state back to the file upon exit.
    """

    def __init__(self, file_location: str):
        self.file_location = file_location
        self.last_processed = ""

    def __enter__(self):
        # Open the file in r+ mode; if file does not exist, create it.
        if not os.path.exists(self.file_location):
            # Create an empty state file if it doesn't exist.
            with open(self.file_location, "w+") as f:
                json.dump({"last_processed": ""}, f)

        # Open the file in read-write mode.
        self.f = open(self.file_location, "r+")
        # Acquire an exclusive lock for the entire duration of the context.
        fcntl.flock(self.f, fcntl.LOCK_EX)
        try:
            self.f.seek(0)
            data = json.load(self.f)
            self.last_processed = data.get("last_processed", "")
        except json.JSONDecodeError:
            self.last_processed = set()
        return self

    def get_last_processed_key(self) -> str:
        """Return the last processed."""
        return self.last_processed

    def mark_last_processed(self, file_key: str):
        """Mark a file key as processed."""
        self.last_processed = file_key

    def __exit__(self, exc_type, exc_value, traceback):
        # Write the updated state back to the file.
        self.f.seek(0)
        json.dump({"last_processed": list(self.last_processed)}, self.f, indent=2)
        self.f.truncate()
        # Release the lock and close the file.
        fcntl.flock(self.f, fcntl.LOCK_UN)
        self.f.close()
