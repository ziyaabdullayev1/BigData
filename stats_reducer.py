#!/usr/bin/env python3
import sys
import math

if len(sys.argv) != 2:
    print("Usage: stats_reducer.py <function>", file=sys.stderr)
    sys.exit(1)

func = sys.argv[1].lower()
values = []

for line in sys.stdin:
    _, val = line.strip().split('\t')
    values.append(float(val))

n = len(values)

if func == "median":
    values.sort()
    median = values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2
    print(f"Median\t{median}")

elif func == "stddev":
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    stddev = math.sqrt(variance)
    print(f"StandardDeviation\t{stddev}")

elif func == "minmax":
    min_val = min(values)
    max_val = max(values)
    normalized = [(x - min_val) / (max_val - min_val) if max_val > min_val else 0 for x in values]
    for i, val in enumerate(normalized):
        print(f"NormalizedValue_{i}\t{val}")

elif func == "percentile":
    values.sort()
    p = 90
    index = int(math.ceil((p / 100) * n)) - 1
    print(f"90thPercentile\t{values[index]}")

elif func == "skewness":
    mean = sum(values) / n
    stddev = math.sqrt(sum((x - mean) ** 2 for x in values) / n)
    if stddev == 0:
        skewness = 0
    else:
        skewness = sum(((x - mean) / stddev) ** 3 for x in values) / n
    print(f"Skewness\t{skewness}")

else:
    print(f"Unknown function: {func}", file=sys.stderr)
    sys.exit(1)
