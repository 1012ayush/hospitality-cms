#!/usr/bin/env python3
"""Create a portable ZIP of the Hotel Channel Manager workspace."""

from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "outputs" / "channel-manager-scaffold.zip"
ARCHIVE_ROOT = "channel-manager"

EXCLUDED_PARTS = {
    ".git",
    "node_modules",
    ".turbo",
    ".next",
    "dist",
    "coverage",
    "__pycache__",
    "work",  # Codex scratch files, not part of the repository.
}


def is_included(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    if any(part in EXCLUDED_PARTS for part in relative.parts):
        return False
    if path == ARCHIVE:
        return False
    # Never package local secrets; keep the shareable example configuration.
    if path.name == ".env" or (path.name.startswith(".env.") and path.name != ".env.example"):
        return False
    return path.is_file()


def main() -> None:
    ARCHIVE.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(path for path in ROOT.rglob("*") if is_included(path))

    with ZipFile(ARCHIVE, mode="w", compression=ZIP_DEFLATED, compresslevel=9) as bundle:
        for path in files:
            archive_path = Path(ARCHIVE_ROOT, path.relative_to(ROOT))
            bundle.write(path, archive_path.as_posix())

    print(f"Created {ARCHIVE}")
    print(f"Included {len(files)} files")
    print(f"Archive size: {ARCHIVE.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
