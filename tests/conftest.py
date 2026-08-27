"""Shared pytest configuration.

Puts the repository root on sys.path so `import ingestion` works without an
editable install, and registers the markers used to keep the fast unit suite
separate from the integration suite that needs the compose stack running.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for extra in (ROOT, ROOT / "scripts"):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))
