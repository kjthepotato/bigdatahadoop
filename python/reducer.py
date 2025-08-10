#!/usr/bin/env python3
import sys
from datetime import datetime

data = []
for line in sys.stdin:
    try:
        date_str, close = line.strip().split("\t")
        close = float(close)
        date = datetime.strptime(date_str, "%Y-%m-%d")
        data.append((date, close))
    except:
        continue

data.sort()

changes = []
for i in range(1, len(data)):
    prev_date, prev_close = data[i-1]
    curr_date, curr_close = data[i]
    delta = curr_close - prev_close
    changes.append((curr_date.strftime("%Y-%m-%d"), delta))

max_gain = max(changes, key=lambda x: x[1])
max_drop = min(changes, key=lambda x: x[1])
avg_change = sum(x[1] for x in changes) / len(changes)

print(f"Average Daily Change: {avg_change:.4f}")
print(f"Largest Gain: {max_gain[0]} ({max_gain[1]:.4f})")
print(f"Largest Drop: {max_drop[0]} ({max_drop[1]:.4f})")