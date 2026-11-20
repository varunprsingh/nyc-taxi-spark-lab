"""Copy existing local TLC files, or download official files without changing the Airflow repository."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import urllib.request
from uuid import uuid4


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1048576), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--from-existing", type=Path, help="Existing Airflow project's data directory; read only")
    choice.add_argument("--download", action="store_true")
    parser.add_argument("--months", nargs="+", default=["2024-01", "2024-02", "2024-03"])
    args = parser.parse_args()
    import re
    if not all(re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", value) for value in args.months):
        raise ValueError("Use YYYY-MM months")
    target = Path(__file__).resolve().parents[1] / "data" / "input"
    target.mkdir(parents=True, exist_ok=True)
    items = [(f"yellow_tripdata_{value}.parquet", "raw/year="+value[:4]+"/month="+value[5:]+f"/yellow_tripdata_{value}.parquet",
              f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{value}.parquet") for value in args.months]
    items.append(("taxi_zone_lookup.csv", "reference/taxi_zone_lookup.csv",
                  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"))
    manifest = []
    for name, relative, url in items:
        destination = target / name
        source = args.from_existing / relative if args.from_existing else None
        if source is not None and source.resolve() == destination.resolve():
            raise ValueError("Source and destination must differ")
        if destination.exists():
            if source is not None and checksum(source) != checksum(destination):
                raise ValueError(f"Existing input differs; preserve it and explicitly choose a new input directory: {name}")
        else:
            temporary = target / (name + "." + uuid4().hex + ".part")
            try:
                if source is not None:
                    shutil.copyfile(source, temporary)
                else:
                    with urllib.request.urlopen(url, timeout=60) as response, temporary.open("wb") as output:
                        shutil.copyfileobj(response, output, length=1048576)
                if name.endswith(".parquet"):
                    with temporary.open("rb") as handle:
                        header = handle.read(4)
                        handle.seek(-4, 2)
                        if header != b"PAR1" or handle.read(4) != b"PAR1":
                            raise ValueError("Downloaded input is not Parquet")
                os.replace(temporary, destination)
            finally:
                if temporary.exists():
                    temporary.unlink()
        manifest.append({"file": name, "bytes": destination.stat().st_size, "sha256": checksum(destination),
                         "official_url": url, "acquisition": "copied existing local artifact; original download time unknown"
                         if source is not None else "official download or reused existing local artifact"})
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
