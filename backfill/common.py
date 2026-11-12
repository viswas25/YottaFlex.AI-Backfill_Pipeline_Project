"""Small shared helpers: paths, hashing, deterministic IDs."""
from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path


def partition_path(source_dir: Path, partition: date | str) -> Path:
    p = partition.isoformat() if isinstance(partition, date) else partition
    return Path(source_dir) / "orders" / f"dt={p}" / "orders.csv"


def file_checksum(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(chunk):
            h.update(block)
    return h.hexdigest()


def reject_id(dataset: str, partition: str | None, source_file: str, line: int,
              reason: str, raw: str) -> str:
    """Deterministic: re-processing the same bad row always yields the same id."""
    key = f"{dataset}|{partition or ''}|{source_file}|{line}|{reason}|{raw}"
    return "REJ-" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
