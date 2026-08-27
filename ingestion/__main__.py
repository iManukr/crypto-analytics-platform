"""Allow `python -m ingestion`."""

import sys

from ingestion.service import main

if __name__ == "__main__":
    sys.exit(main())
