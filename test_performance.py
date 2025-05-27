#!/usr/bin/env python3
import subprocess
import os
import json
from datetime import datetime

def get_available_datasets():
    """Get list of available datasets in kaggleinput folder"""
    datasets = {
        'retail': {
            'transactions': 'retail_transaction_amounts.txt',
            'quantities': 'retail_quantities.txt',
            'prices': 'retail_prices.txt'
        },
        'ecommerce': {
            'purchases': 'ecom_purchase_amounts.txt',
            'ratings': 'ecom_ratings.txt',
            'days_since_purchase': 'ecom_days_since_purchase.txt',
            'items': 'ecom_items_purchased.txt'
        }
    }
    
    # Verify files exist
    available_files = {}
    for category, files in datasets.items():
        available_files[category] = {}
        for name, filename in files.items():
            filepath = os.path.join('kaggleinput', filename)
            if os.path.exists(filepath):
                available_files[category][name] = filepath
    
    return available_files

def count_lines(filepath):
    """Count number of lines in a file"""
    with open(filepath, 'r') as f:
        return sum(1 for _ in f)

def test_statistical_function(function_name, input_file):
    """Test a statistical function with performance monitoring"""
    print(f"\nTesting {function_name} function on {os.path.basename(input_file)}...")
    
    # Create performance_logs directory if it doesn't exist
    if not os.path.exists('performance_logs'):
        os.makedirs('performance_logs')
    
    # Create mapper process
    mapper = subprocess.Popen(
        ['python', 'mapper.py'],
        stdin=open(input_file, 'r'),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Create reducer process
    reducer = subprocess.Popen(
        ['python', 'stats_reducer.py', function_name],
        stdin=mapper.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Close mapper's stdout to signal EOF to reducer
    mapper.stdout.close()
    
    # Get output and errors
    output, errors = reducer.communicate()
    
    # Print statistical result
    print("\nStatistical Result:")
    print(output.strip())
    
    # Extract performance metrics
    metrics = {}
    in_metrics = False
    for line in errors.split('\n'):
        if line == "PERFORMANCE_METRICS_START":
            in_metrics = True
            continue
        elif line == "PERFORMANCE_METRICS_END":
            in_metrics = False
            continue
        
        if in_metrics and line.strip():
            print(line)  # Still print metrics to console
            try:
                key, value = line.split(': ')
                # Convert string numbers to float where possible
                try:
                    metrics[key] = float(value)
                except ValueError:
                    metrics[key] = value
            except ValueError:
                continue
    
    # Check if performance plot was generated
    try:
        plot_files = [f for f in os.listdir('performance_logs') if f.startswith(f'performance_plot_{function_name}')]
        if plot_files:
            latest_plot = max(plot_files, key=lambda x: os.path.getctime(os.path.join('performance_logs', x)))
            print(f"\nPerformance plot generated: {latest_plot}")
            metrics['plot_file'] = latest_plot
        else:
            print("\nNo performance plot was generated for this run")
    except Exception as e:
        print(f"\nCould not find performance files: {str(e)}")
    
    return {
        'function': function_name,
        'input_file': os.path.basename(input_file),
        'metrics': metrics,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'result': output.strip()
    }

def save_complete_metrics(all_metrics):
    """Save all metrics to a single JSON file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics_file = os.path.join('performance_logs', f'complete_performance_metrics_{timestamp}.json')
    
    with open(metrics_file, 'w') as f:
        json.dump(all_metrics, f, indent=4)
    
    print(f"\nComplete performance metrics saved to: {metrics_file}")

def main():
    # Get available datasets
    datasets = get_available_datasets()
    
    # Statistical functions to test
    functions = ['median', 'stddev', 'minmax', 'percentile', 'skewness']
    
    # Initialize metrics collection
    all_metrics = {
        'test_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'datasets': {},
        'functions': []
    }
    
    # Process each dataset
    for category, files in datasets.items():
        print(f"\nProcessing {category} dataset:")
        all_metrics['datasets'][category] = {}
        
        for data_type, filepath in files.items():
            print(f"\nAnalyzing {data_type}:")
            record_count = count_lines(filepath)
            print(f"Number of records: {record_count}")
            
            all_metrics['datasets'][category][data_type] = {
                'file': os.path.basename(filepath),
                'record_count': record_count
            }
            
            # Run each statistical function
            for func in functions:
                metrics = test_statistical_function(func, filepath)
                metrics['dataset'] = category
                metrics['data_type'] = data_type
                all_metrics['functions'].append(metrics)
                print("\n" + "="*50)
    
    # Save complete metrics to a single JSON file
    save_complete_metrics(all_metrics)

if __name__ == "__main__":
    main() 