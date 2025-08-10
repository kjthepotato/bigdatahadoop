#!/usr/bin/env python3
import sys

for line in sys.stdin:
    if line.startswith("Date"):
        continue
    parts = line.strip().split(",")
    if len(parts) >= 5:
        date = parts[0]
        try:
            close_price = float(parts[4])
            print(f"{date}\t{close_price}")
        except ValueError:
            continue