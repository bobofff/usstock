"""Command line entry points."""

from __future__ import annotations

import sys

from usstock.admin.app import main as admin_main
from usstock.data import finnhub
from usstock.data import gdelt
from usstock.data import sec
from usstock.db.migrations import main as migrations_main


def main() -> int:
    argv = sys.argv[1:]
    if argv and argv[0] == "admin":
        return admin_main(argv[1:])
    if argv and argv[0] == "sec":
        return sec.main(argv[1:])
    if argv and argv[0] == "gdelt":
        return gdelt.main(argv[1:])
    if argv and argv[0] == "finnhub":
        return finnhub.main(argv[1:])

    return migrations_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
