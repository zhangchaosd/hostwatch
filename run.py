"""Development and deployment entry point which works directly from the source tree."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from hostwatch.main import run  # noqa: E402


if __name__ == "__main__":
    run()
