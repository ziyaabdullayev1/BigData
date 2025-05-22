import pandas as pd

df = pd.read_csv("E-commerce Customer Behavior - Sheet1.csv")

# Change this to "Age", "Items Purchased", etc. as needed
column = "Total Spend"

# Export to 1 value per line for Hadoop Streaming
df[column].dropna().to_csv("kaggle_numbers.txt", index=False, header=False)
