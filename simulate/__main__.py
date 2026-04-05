"""Allow running simulators with ``python -m simulate``."""

from __future__ import annotations

import sys


def main():
    print("Usage:")
    print("  python -m simulate.stream_simulator --file <path> --protocol tcp --port 9100 --rate 10")
    print("  python -m simulate.file_simulator --source-dir <path> --target-dir <path> --interval 10")
    sys.exit(0)


if __name__ == "__main__":
    main()
