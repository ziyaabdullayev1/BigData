#!/usr/bin/env python3
import sys

for line in sys.stdin:
    value = line.strip()
    if value:
        print(f"val\t{value}")
