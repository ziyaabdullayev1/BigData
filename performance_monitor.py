#!/usr/bin/env python3
import time
import psutil  # type: ignore
import os
import json
from datetime import datetime
import matplotlib.pyplot as plt
from typing import Dict, List

class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.metrics = {
            'runtime': 0,
            'cpu_usage': [],
            'memory_usage': [],
            'timestamps': [],
            'records_processed': 0
        }
        
        # Create performance_logs directory if it doesn't exist
        if not os.path.exists('performance_logs'):
            os.makedirs('performance_logs')

    def start_monitoring(self):
        """Start monitoring performance metrics"""
        self.start_time = time.time()
        self._update_metrics()  # Initial measurement

    def _update_metrics(self):
        """Update current performance metrics"""
        current_time = time.time()
        
        # CPU usage (percentage across all cores)
        cpu_percent = psutil.cpu_percent(interval=None)
        
        # Memory usage
        memory = psutil.Process(os.getpid()).memory_info()
        memory_usage = memory.rss / 1024 / 1024  # Convert to MB
        
        # Store metrics
        self.metrics['cpu_usage'].append(cpu_percent)
        self.metrics['memory_usage'].append(memory_usage)
        self.metrics['timestamps'].append(current_time - self.start_time)

    def stop_monitoring(self, records_processed: int = 0):
        """Stop monitoring and calculate final metrics"""
        self.end_time = time.time()
        self._update_metrics()  # Final measurement
        
        self.metrics['runtime'] = self.end_time - self.start_time
        self.metrics['records_processed'] = records_processed
        self.metrics['throughput'] = records_processed / self.metrics['runtime'] if self.metrics['runtime'] > 0 else 0
        
        return self.get_summary()

    def get_summary(self) -> Dict:
        """Get a summary of performance metrics"""
        return {
            'runtime_seconds': self.metrics['runtime'],
            'records_processed': self.metrics['records_processed'],
            'throughput': self.metrics['throughput'],
            'avg_cpu_usage': sum(self.metrics['cpu_usage']) / len(self.metrics['cpu_usage']) if self.metrics['cpu_usage'] else 0,
            'max_cpu_usage': max(self.metrics['cpu_usage']) if self.metrics['cpu_usage'] else 0,
            'avg_memory_mb': sum(self.metrics['memory_usage']) / len(self.metrics['memory_usage']) if self.metrics['memory_usage'] else 0,
            'max_memory_mb': max(self.metrics['memory_usage']) if self.metrics['memory_usage'] else 0
        }

    def create_performance_plot(self, function_name: str) -> str:
        """Generate performance visualization plot and return the file path"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 8))
        
        # Plot CPU usage
        ax1.plot(self.metrics['timestamps'], self.metrics['cpu_usage'], 'b-', label='CPU Usage (%)')
        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('CPU Usage (%)')
        ax1.set_title(f'CPU Usage - {function_name}')
        ax1.grid(True)
        ax1.legend()
        
        # Plot Memory usage
        ax2.plot(self.metrics['timestamps'], self.metrics['memory_usage'], 'r-', label='Memory Usage (MB)')
        ax2.set_xlabel('Time (seconds)')
        ax2.set_ylabel('Memory Usage (MB)')
        ax2.set_title(f'Memory Usage - {function_name}')
        ax2.grid(True)
        ax2.legend()
        
        plt.tight_layout()
        
        # Save plot
        plot_file = os.path.join('performance_logs', f'performance_plot_{function_name}_{timestamp}.png')
        plt.savefig(plot_file)
        plt.close()
        
        return plot_file

    def save_metrics(self, function_name: str) -> str:
        """Save metrics to JSON file and return the file path"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metrics_file = os.path.join('performance_logs', f'metrics_{function_name}_{timestamp}.json')
        
        with open(metrics_file, 'w') as f:
            json.dump(self.metrics, f, indent=4)
            
        return metrics_file 