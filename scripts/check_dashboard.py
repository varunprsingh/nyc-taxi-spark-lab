"""Verify the real dashboard API against published aggregate evidence."""
import json
from pathlib import Path
import urllib.request


def main():
    root = Path(__file__).resolve().parents[1] / "data"
    manifest = json.loads((root / "catalog/reporting.json").read_text())
    with urllib.request.urlopen("http://127.0.0.1:8502/api/dashboard", timeout=10) as response:
        payload = json.load(response)
    assert payload["data"]["trips"] == manifest["trip_count"]
    assert payload["data"]["total_collected_amount"] == manifest["total_collected_amount"]
    assert not payload["stale"], "Reporting is stale; rebuild reporting"
    assert sum(r["trips"] for r in payload["data"]["daily"]) == manifest["trip_count"]
    assert sum(r["trips"] for r in payload["data"]["hourly"]) == manifest["trip_count"]
    with urllib.request.urlopen("http://127.0.0.1:8502/api/benchmark", timeout=10) as response:
        benchmark = json.load(response)
    assert benchmark["exact_multiset_equality"]
    print("Dashboard API reconciled:", manifest["trip_count"], "trips;", manifest["total_collected_amount"], "passenger charges")


if __name__ == "__main__":
    main()
