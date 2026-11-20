"""Immutable outputs with atomic local catalog publication, never destructive overwrite."""
import hashlib
import json
import os
import re
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


def month(value):
    if not re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", value):
        raise ValueError("Month must use YYYY-MM (2000–2099)")
    return value


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_json(path, default=None):
    return json.loads(Path(path).read_text()) if Path(path).exists() else default


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, default=str)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


@contextmanager
def writer_lock(root):
    """Linux advisory lock released by the kernel on process exit; Docker/CI runtime only."""
    import fcntl
    path = Path(root) / "catalog" / "writer.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another publisher holds the catalog writer lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def catalog(root):
    return read_json(Path(root) / "catalog" / "partitions.json", {})


def published_trips(spark, root):
    snapshot = catalog(root)
    if not snapshot:
        raise ValueError("No successfully published partitions; run ingest first")
    paths = [str(Path(root) / item["accepted_path"]) for item in snapshot.values()]
    result = spark.read.parquet(paths[0])
    for path in paths[1:]:
        result = result.unionByName(spark.read.parquet(path))
    return result, snapshot
