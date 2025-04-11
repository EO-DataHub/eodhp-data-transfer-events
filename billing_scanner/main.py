import logging
import sys

from eodhp_utils.runner import setup_logging

from billing_scanner.scanner import main

setup_logging(verbosity=1)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Unexpected error in main:")
        sys.exit(1)
