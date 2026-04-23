#!/usr/bin/env python3
"""Generate auto-derived reliability labels from structured backtests."""

from reliability import RELIABILITY_PATH, build_reliability_labels, save_reliability_labels


def main():
    data = build_reliability_labels()
    save_reliability_labels(data)
    print(f"可靠度标签已生成: {RELIABILITY_PATH}")


if __name__ == "__main__":
    main()
