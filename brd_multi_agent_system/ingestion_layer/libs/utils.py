from __future__ import annotations
import hashlib
import os
from datetime import datetime
from typing import Optional


def file_sha256(path: str, chunk_size: int = 65536) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha.update(chunk)
    return sha.hexdigest()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def month_from_date(value: str | datetime) -> str:
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            # try common formats
            for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                try:
                    dt = datetime.strptime(value, fmt)
                    break
                except ValueError:
                    dt = None  # type: ignore
            if dt is None:
                raise
    else:
        dt = value
    return dt.strftime("%Y-%m")


def safe_colname(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )
