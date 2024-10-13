#!/usr/bin/env python3
import sys

for line in sys.stdin:
    parts = line.strip().split()

    if len(parts) >= 6:
        timestamp = parts[3]
        print(f"{timestamp} {line.strip()}")

