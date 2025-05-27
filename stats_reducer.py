#!/usr/bin/env python3
import sys
import math
import os

# Try to import performance monitoring, but don't fail if it's not available
try:
    from performance_monitor import PerformanceMonitor
    performance_monitoring_enabled = True
except ImportError:
    performance_monitoring_enabled = False

if len(sys.argv) != 2:
    print("Usage: stats_reducer.py <function>", file=sys.stderr)
    sys.exit(1)

func = sys.argv[1].lower()
values = []

# Initialize performance monitor if available
if performance_monitoring_enabled:
    try:
        monitor = PerformanceMonitor()
        monitor.start_monitoring()
    except Exception as e:
        print(f"Warning: Performance monitoring failed to start: {e}", file=sys.stderr)
        performance_monitoring_enabled = False

# Read and process input
for line in sys.stdin:
    _, val = line.strip().split('\t')
    values.append(float(val))

n = len(values)

# Process based on function type
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
    # Print in the same format as other functions, with a simple summary
    print(f"Min-Max\t{min_val:.4f}-{max_val:.4f}")

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

# Stop monitoring and save results if it was enabled
if performance_monitoring_enabled:
    try:
        summary = monitor.stop_monitoring(records_processed=n)

        # Ensure performance_logs directory exists
        if not os.path.exists('performance_logs'):
            os.makedirs('performance_logs')

        # Generate performance plot
        plot_file = monitor.create_performance_plot(func)
        print(f"\nPerformance plot saved to: {plot_file}", file=sys.stderr)

        # Print performance metrics
        print("\nPERFORMANCE_METRICS_START", file=sys.stderr)
        for key, value in summary.items():
            print(f"{key}: {value}", file=sys.stderr)
        print("PERFORMANCE_METRICS_END", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Performance monitoring failed: {e}", file=sys.stderr)
