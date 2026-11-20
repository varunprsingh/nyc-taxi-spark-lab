"""Host orchestration: bounded fresh applications, rotated order, no manufactured speedups."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess


def run(arguments):
    subprocess.run(["docker", "compose", "run", "--rm", "--no-deps", "spark", *arguments], check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--month", default="2024-01")
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 10:
        raise ValueError("Choose 1–10 measured repetitions")
    root = Path(__file__).resolve().parents[1]
    import os
    os.chdir(root)
    suite = datetime.now(timezone.utc).strftime("suite-%Y%m%dT%H%M%SZ")
    names = ["baseline", "broadcast_only", "tuned"]
    result = {"suite": suite, "month": args.month, "measured_runs": [], "warmup_runs": [],
              "method": "One discarded full workload warmup per configuration; fresh JVM per run; rotated order; OS cache uncontrolled"}
    path = root / "data" / "benchmarks" / f"{suite}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    for iteration in range(args.repetitions + 1):
        order = names[iteration % 3:] + names[:iteration % 3]
        for name in order:
            run_id = f"{suite}-{name}-{'warmup' if iteration==0 else iteration}"
            run(["/workspace/jobs/benchmark.py", "--config", name, "--run-id", run_id, "--month", args.month])
            result["warmup_runs" if iteration == 0 else "measured_runs"].append(run_id)
            path.write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")
    run(["/workspace/jobs/compare.py", "--suite", suite])
    print("Verified benchmark:", root / "data" / "benchmarks" / f"{suite}-verified.json")


if __name__ == "__main__":
    main()
