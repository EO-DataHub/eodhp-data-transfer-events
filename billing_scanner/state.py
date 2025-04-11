import fcntl
import json
import os


def load_state(state_file: str) -> set:
    if not os.path.exists(state_file):
        with open(state_file, "w+") as f:
            json.dump({"processed": []}, f)
    with open(state_file, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            data = json.load(f)
            processed = set(data.get("processed", []))
        except json.JSONDecodeError:
            processed = set()
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
    return processed


def save_state(state_file: str, processed_set: set) -> None:
    with open(state_file, "w+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump({"processed": list(processed_set)}, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)
