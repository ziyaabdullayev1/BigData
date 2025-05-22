import random

with open("big_numbers.txt", "w") as f:
    for _ in range(1_000_000):
        f.write(f"{random.randint(1, 1000)}\n")
