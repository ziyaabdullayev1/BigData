import pandas as pd
import numpy as np
import os

def preprocess_retail_data():
    print("\nProcessing Online Retail II dataset...")
    
    # Read the Excel file
    file_path = os.path.join('datasets', 'Online_Retail_II.xlsx')
    print(f"Reading file from: {file_path}")
    
    df = pd.read_excel(file_path)
    print(f"Initial shape: {df.shape}")
    
    # Basic preprocessing and data cleaning
    df = df.dropna(subset=['Invoice', 'Quantity', 'Price', 'Customer ID'])
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['Customer ID'] = df['Customer ID'].astype(int)
    
    # Remove cancelled orders, negative/zero quantities and prices
    df = df[~df['Invoice'].astype(str).str.contains('^C', regex=True)]
    df = df[df['Quantity'] > 0]
    df = df[df['Price'] > 0]
    
    print(f"Shape after cleaning: {df.shape}")
    
    # Calculate columns for analysis
    df['TotalAmount'] = df['Quantity'] * df['Price']
    
    # Save relevant columns
    df['TotalAmount'].to_csv('kaggleinput/retail_transaction_amounts.txt', index=False, header=False)
    df['Quantity'].to_csv('kaggleinput/retail_quantities.txt', index=False, header=False)
    df['Price'].to_csv('kaggleinput/retail_prices.txt', index=False, header=False)
    
    print("\nRetail dataset columns saved:")
    print("- retail_transaction_amounts.txt: Total amount per transaction (Quantity * Price)")
    print("- retail_quantities.txt: Number of items per transaction")
    print("- retail_prices.txt: Unit price of items")

def preprocess_ecommerce_data():
    print("\nProcessing E-commerce Customer Behavior dataset...")
    
    # Read the CSV file
    file_path = os.path.join('datasets', 'E-commerce Customer Behavior - Sheet1.csv')
    print(f"Reading file from: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        print(f"Initial shape: {df.shape}")
        print("Available columns:", df.columns.tolist())
        
        # Basic preprocessing
        numeric_columns = {
            'Total Spend': 'ecom_purchase_amounts.txt',
            'Days Since Last Purchase': 'ecom_days_since_purchase.txt',
            'Items Purchased': 'ecom_items_purchased.txt',
            'Average Rating': 'ecom_ratings.txt'
        }
        
        # Save each numeric column
        for column, filename in numeric_columns.items():
            print(f"\nProcessing column: {column}")
            if column in df.columns:
                # Remove any invalid values
                valid_data = df[df[column].notna() & (df[column] > 0)][column]
                print(f"Found {len(valid_data)} valid rows for {column}")
                output_path = f'kaggleinput/{filename}'
                valid_data.to_csv(output_path, index=False, header=False)
                print(f"Saved to {output_path}")
            else:
                print(f"Warning: Column '{column}' not found in dataset!")
        
        print("\nE-commerce dataset columns saved:")
        print("- ecom_purchase_amounts.txt: Total spend per customer")
        print("- ecom_days_since_purchase.txt: Days since last purchase")
        print("- ecom_items_purchased.txt: Number of items purchased")
        print("- ecom_ratings.txt: Average customer ratings")
    
    except Exception as e:
        print(f"Error processing e-commerce data: {str(e)}")

def main():
    # Create kaggleinput directory if it doesn't exist
    os.makedirs('kaggleinput', exist_ok=True)
    
    # Process both datasets
    preprocess_retail_data()
    preprocess_ecommerce_data()
    
    print("\nAll preprocessing completed successfully!")

if __name__ == "__main__":
    main() 