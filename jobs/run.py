"""spark-submit entrypoint."""
import argparse
import json

from taxi_spark.pipeline import ingest, report
from taxi_spark.runtime import session


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi PySpark pipeline")
    parser.add_argument("command", choices=["ingest", "report"])
    parser.add_argument("--root", default="/workspace/data")
    parser.add_argument("--input", default="/workspace/data/input")
    parser.add_argument("--months", nargs="+", default=["2024-01", "2024-02", "2024-03"])
    parser.add_argument("--reprocess", action="store_true")
    parser.add_argument("--max-reject-fraction", type=float, default=0.01)
    parser.add_argument("--fail-before-publish", action="store_true")
    args = parser.parse_args()
    spark = session("taxi-" + args.command)
    try:
        result = ingest(spark, args.root, args.input, args.months, args.reprocess,
                        args.max_reject_fraction, args.fail_before_publish) if args.command == "ingest" else report(
                            spark, args.root, args.input
                        )
        print(json.dumps(result, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
