import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
import subprocess
import threading
import os
import time
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd

# === Constants ===
HADOOP_STREAMING_JAR = r"C:/hadoop-2.7.7/share/hadoop/tools/lib/hadoop-streaming-2.7.7.jar"

# === Color Scheme ===
COLORS = {
    "bg": "#1A1A1A",  # Darker background
    "fg": "#FFFFFF",  # White text
    "accent": "#2196F3",  # Modern blue
    "button_bg": "#333333",  # Darker button background
    "button_hover": "#444444",  # Lighter button hover
    "output_bg": "#212121",  # Slightly lighter than background
    "output_fg": "#4CAF50",  # Modern green for output
    "status_bg": "#2196F3",  # Modern blue
    "status_fg": "#FFFFFF",  # White text
    "error": "#F44336",  # Error red
    "success": "#4CAF50",  # Success green
    "chart_colors": ["#2196F3", "#4CAF50", "#FFC107", "#9C27B0", "#F44336"]  # Material colors
}

# === Supported jobs ===
jobs = {
    "Median": ("median", COLORS["chart_colors"][0]),
    "Standard Deviation": ("stddev", COLORS["chart_colors"][1]),
    "Min-Max Normalization": ("minmax", COLORS["chart_colors"][2]),
    "90th Percentile": ("percentile", COLORS["chart_colors"][3]),
    "Skewness": ("skewness", COLORS["chart_colors"][4])
}

# === Performance Metrics ===
class PerformanceMetrics:
    def __init__(self):
        self.metrics = []
        self.df = pd.DataFrame(columns=['Function', 'Value', 'Runtime', 'Dataset', 'Timestamp'])
    
    def add_metric(self, function, value, runtime, dataset):
        timestamp = datetime.now()
        # Convert runtime to float using iloc[0] if it's a Series
        if isinstance(runtime, pd.Series):
            runtime = float(runtime.iloc[0])
        else:
            runtime = float(runtime)
            
        self.metrics.append({
            'Function': function,
            'Value': value,
            'Runtime': runtime,
            'Dataset': dataset,
            'Timestamp': timestamp
        })
        self.df = pd.DataFrame(self.metrics)
    
    def get_latest_metrics(self):
        return self.df.sort_values('Timestamp', ascending=False)
    
    def get_comparison_data(self):
        if self.df.empty:
            return pd.DataFrame(columns=['Function', 'mean', 'min', 'max'])
        # Ensure Runtime column is float type
        self.df['Runtime'] = self.df['Runtime'].astype(float)
        return self.df.groupby('Function')['Runtime'].agg(['mean', 'min', 'max']).reset_index()

performance_metrics = PerformanceMetrics()

# === Helper Functions ===
def get_input_files():
    input_path = os.path.join(os.getcwd(), "kaggleinput")
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    return [f for f in os.listdir(input_path) if f.endswith(".txt")]

def refresh_files():
    """Refresh the list of input files in the dropdown."""
    file_dropdown["values"] = get_input_files()
    file_dropdown.set("")

def on_enter(e):
    """Handle mouse enter event for buttons - hover effect."""
    if e.widget.cget('state') != 'disabled':
        e.widget.config(bg=COLORS["button_hover"])

def on_leave(e):
    """Handle mouse leave event for buttons - restore normal color."""
    if e.widget.cget('state') != 'disabled':
        e.widget.config(bg=COLORS["button_bg"])

def restore_ui_state():
    output_text.insert(tk.END, "[UI] Buttons re-enabled\n")
    for btn in function_buttons:
        btn.config(state="normal")
    exit_btn.config(state="normal")

def extract_result_from_hdfs(output_dir):
    try:
        cmd = f"hadoop fs -cat {output_dir}/part-00000"
        result = subprocess.check_output(cmd, shell=True, text=True)
        output_text.insert(tk.END, f"\n[HDFS Output]\n{result}\n")
        lines = result.strip().splitlines()
        for line in lines:
            parts = line.strip().split()
            if len(parts) == 2:
                key, val = parts[0], parts[1]
                if "NormalizedValue" in key:
                    return "Min-Max Normalization", "Multiple Values"
                return key, val
        return "Unknown", "N/A"
    except Exception as e:
        output_text.insert(tk.END, f"[ERROR] Failed to read result: {e}\n")
        return "Error", "N/A"

def safe_exit():
    if status_var.get().startswith("⏳"):
        messagebox.showwarning("Wait", "Job is running. Please wait.")
    else:
        root.quit()

def update_performance_chart():
    """Update performance chart with runtime data from executed jobs."""
    if not performance_metrics.metrics:
        return
    
    # Clear previous chart
    for widget in chart_frame.winfo_children()[1:]:  # Keep the label
        widget.destroy()
    
    # Create a single combined chart with clear metrics
    fig = plt.figure(figsize=(12, 5))
    fig.patch.set_facecolor(COLORS["bg"])
    ax = fig.add_subplot(111)
    
    # Get data for comparison - focus on function runtimes which is most useful
    comparison_data = performance_metrics.get_comparison_data()
    
    if not comparison_data.empty:
        # Create a more informative chart that combines both runtime stats and latest executions
        x = range(len(comparison_data['Function']))
        functions = comparison_data['Function'].values
        
        # Main bars - average runtime
        mean_values = comparison_data['mean'].values.astype(float)
        min_values = comparison_data['min'].values.astype(float)
        max_values = comparison_data['max'].values.astype(float)
        
        # Create the main bars with custom appearance
        bars = ax.bar(x, mean_values, 
               width=0.6,
               yerr=[mean_values - min_values, max_values - mean_values],
               capsize=5,
               color=[jobs.get(func, (None, COLORS["chart_colors"][i % len(COLORS["chart_colors"])]))[1] 
                     for i, func in enumerate(functions)],
               alpha=0.8,
               label='Average Runtime')
        
        # Add min/max indicators
        for i, (func, mean, min_val, max_val) in enumerate(
            zip(functions, mean_values, min_values, max_values)):
            # Add min marker
            ax.scatter(i, min_val, color='white', s=30, zorder=10, marker='_', 
                      label='Min' if i == 0 else "")
            # Add max marker
            ax.scatter(i, max_val, color='white', s=30, zorder=10, marker='_',
                      label='Max' if i == 0 else "")
        
        # Style the chart
        ax.set_title('Function Runtime Performance', 
                   color=COLORS["fg"],
                   fontsize=13,
                   fontweight='bold',
                   pad=20)
        
        ax.set_xticks(x)
        ax.set_xticklabels(functions, rotation=30, ha='right', fontsize=10)
        ax.set_ylabel('Runtime (seconds)', color=COLORS["fg"], fontsize=11)
        ax.set_facecolor(COLORS["bg"])
        ax.tick_params(colors=COLORS["fg"], labelsize=10)
        
        # Add grid for better readability
        ax.grid(True, linestyle='--', alpha=0.2, color=COLORS["fg"])
        
        # Style the chart borders
        for spine in ax.spines.values():
            spine.set_color(COLORS["fg"])
            spine.set_linewidth(0.5)
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                  f'{height:.2f}s',
                  ha='center',
                  va='bottom',
                  color=COLORS["fg"],
                  fontsize=9)
        
        # Add legend
        legend = ax.legend(loc='upper right', framealpha=0.7, 
                         facecolor=COLORS["button_bg"], edgecolor=COLORS["fg"],
                         fontsize=9)
        for text in legend.get_texts():
            text.set_color(COLORS["fg"])
    
    plt.tight_layout()
    
    # Create a frame for the chart with padding
    chart_container = tk.Frame(chart_frame, bg=COLORS["bg"])
    chart_container.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
    
    # Embed chart in tkinter
    canvas = FigureCanvasTkAgg(fig, master=chart_container)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

def run_job(job_label):
    selected_file = file_var.get()
    if not selected_file:
        messagebox.showwarning("Missing File", "Please select an input file.")
        return

    job_key, color = jobs[job_label]
    output_dir = f"/kaggle_output_{job_key}"

    status_var.set(f"⏳ Running {job_label}...")
    output_text.delete(1.0, tk.END)

    for btn in function_buttons:
        btn.config(state="disabled")
    exit_btn.config(state="disabled")
    output_text.insert(tk.END, f"[UI] Running {job_label}\n")

    def thread_job():
        try:
            if not skip_delete.get():
                subprocess.run(["hadoop", "fs", "-rm", "-r", output_dir], shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            start = time.time()

            cmd = [
                "C:/hadoop-2.7.7/bin/hadoop.cmd", "jar", HADOOP_STREAMING_JAR,
                "-input", f"/kaggleinput/{selected_file}",
                "-output", output_dir,
                "-mapper", "C:/Users/abdul/AppData/Local/Programs/Python/Python313/python.exe mapper.py",
                "-reducer", f"C:/Users/abdul/AppData/Local/Programs/Python/Python313/python.exe stats_reducer.py {job_key}"
            ]

            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
            for line in process.stdout:
                output_text.insert(tk.END, line)
                output_text.see(tk.END)
            process.wait()

            duration = round(time.time() - start, 2)

            if process.returncode == 0:
                func, val = extract_result_from_hdfs(output_dir)
                # Add to performance metrics
                performance_metrics.add_metric(func, val, duration, selected_file)
                # Update result table
                root.after(0, lambda: result_table.insert("", "end", values=(func, val, duration, selected_file)))
                root.after(0, lambda: status_var.set(f"✅ {job_label} completed in {duration}s"))
                # Update performance chart
                root.after(0, update_performance_chart)
            else:
                root.after(0, lambda: status_var.set(f"❌ {job_label} failed after {duration}s"))
        except Exception as e:
            output_text.insert(tk.END, f"[FATAL ERROR] {e}\n")
            root.after(0, lambda: status_var.set("❌ Unexpected error"))
        finally:
            root.after(0, restore_ui_state)

    threading.Thread(target=thread_job).start()

# === GUI ===
root = tk.Tk()
root.title("Big Data Statistical Function Launcher")
root.geometry("1200x900")
root.configure(bg=COLORS["bg"])
root.minsize(1200, 900)

# Configure ttk styles
style = ttk.Style()
style.theme_use('clam')

# Configure Treeview style
style.configure("Treeview", 
                background=COLORS["output_bg"],
                foreground=COLORS["fg"],
                fieldbackground=COLORS["output_bg"],
                rowheight=35,
                borderwidth=0)
style.configure("Treeview.Heading",
                background=COLORS["button_bg"],
                foreground=COLORS["fg"],
                relief="flat",
                font=("Segoe UI", 11, "bold"),
                borderwidth=0)
style.map("Treeview.Heading",
          background=[("active", COLORS["button_hover"])])

# Configure Combobox style
style.configure("TCombobox",
                background=COLORS["output_bg"],
                foreground=COLORS["fg"],
                arrowcolor=COLORS["fg"],
                borderwidth=0)

# Configure PanedWindow style
style.configure("TPanedwindow",
                background=COLORS["bg"],
                borderwidth=0)

# Main container with padding
main_frame = tk.Frame(root, bg=COLORS["bg"], padx=40, pady=30)
main_frame.pack(fill=tk.BOTH, expand=True)

# Title with modern styling
title_frame = tk.Frame(main_frame, bg=COLORS["bg"])
title_frame.pack(fill=tk.X, pady=(0, 30))
tk.Label(title_frame, 
         text="Big Data Statistical Function Launcher",
         font=("Segoe UI", 24, "bold"),
         fg=COLORS["fg"],
         bg=COLORS["bg"]).pack()

# File selection frame with modern styling
file_frame = tk.Frame(main_frame, bg=COLORS["bg"])
file_frame.pack(fill=tk.X, pady=(0, 20))

tk.Label(file_frame,
         text="Select Input File:",
         font=("Segoe UI", 12),
         bg=COLORS["bg"],
         fg=COLORS["fg"]).pack(side=tk.LEFT, padx=(0, 15))

file_var = tk.StringVar()
file_dropdown = ttk.Combobox(file_frame,
                            textvariable=file_var,
                            width=50,
                            font=("Segoe UI", 11),
                            style="TCombobox")
file_dropdown["values"] = get_input_files()
file_dropdown.pack(side=tk.LEFT, padx=(0, 15))

refresh_btn = tk.Button(file_frame,
                       text="↻",
                       font=("Segoe UI", 11),
                       bg=COLORS["button_bg"],
                       fg=COLORS["fg"],
                       command=refresh_files,
                       width=3,
                       relief=tk.FLAT,
                       borderwidth=0,
                       padx=10,
                       pady=5)
refresh_btn.pack(side=tk.LEFT)
refresh_btn.bind("<Enter>", on_enter)
refresh_btn.bind("<Leave>", on_leave)

# Skip checkbox with modern style
skip_frame = tk.Frame(main_frame, bg=COLORS["bg"])
skip_frame.pack(fill=tk.X, pady=(0, 20))
skip_delete = tk.BooleanVar()
tk.Checkbutton(skip_frame,
               text="Skip deleting old HDFS output",
               variable=skip_delete,
               bg=COLORS["bg"],
               fg=COLORS["fg"],
               selectcolor=COLORS["button_bg"],
               activebackground=COLORS["bg"],
               activeforeground=COLORS["fg"],
               font=("Segoe UI", 11)).pack()

# Function buttons frame with modern styling
tk.Label(main_frame,
         text="Choose Function",
         font=("Segoe UI", 14, "bold"),
         bg=COLORS["bg"],
         fg=COLORS["fg"]).pack(pady=(0, 10))

# Create a frame with a grid layout for buttons to use less vertical space
btn_frame = tk.Frame(main_frame, bg=COLORS["bg"])
btn_frame.pack(fill=tk.X, padx=20, pady=(0, 20))
function_buttons = []

# Arrange buttons in a grid (3 columns)
col, row = 0, 0
max_cols = 3
for label, (func, color) in jobs.items():
    btn = tk.Button(btn_frame,
                   text=label,
                   width=25,
                   font=("Segoe UI", 11),
                   bg=COLORS["button_bg"],
                   fg=COLORS["fg"],
                   command=lambda l=label: run_job(l),
                   relief=tk.FLAT,
                   borderwidth=0,
                   padx=10,
                   pady=6)
    btn.grid(row=row, column=col, padx=5, pady=5, sticky="ew")
    btn.bind("<Enter>", on_enter)
    btn.bind("<Leave>", on_leave)
    function_buttons.append(btn)
    
    # Advance to next position
    col += 1
    if col >= max_cols:
        col = 0
        row += 1

# Output and Results Section
paned_window = ttk.PanedWindow(main_frame, orient=tk.VERTICAL, style="TPanedwindow")
paned_window.pack(fill=tk.BOTH, expand=True, pady=(20, 10))

# Output section in top pane
output_frame = tk.Frame(paned_window, bg=COLORS["bg"])
paned_window.add(output_frame, weight=1)

tk.Label(output_frame,
         text="Job Output",
         font=("Segoe UI", 14, "bold"),
         bg=COLORS["bg"],
         fg=COLORS["fg"]).pack(anchor=tk.W, pady=(0, 10))

output_text = scrolledtext.ScrolledText(output_frame,
                                      width=90,
                                      height=8,
                                      font=("Consolas", 11),
                                      bg=COLORS["output_bg"],
                                      fg=COLORS["output_fg"],
                                      insertbackground=COLORS["output_fg"],
                                      borderwidth=0,
                                      relief=tk.FLAT)
output_text.pack(fill=tk.BOTH, expand=True, pady=(0, 15))

# Results section in bottom pane
results_paned = ttk.PanedWindow(paned_window, orient=tk.VERTICAL, style="TPanedwindow")
paned_window.add(results_paned, weight=2)

# Result table section with modern styling
result_frame = tk.Frame(results_paned, bg=COLORS["bg"])
results_paned.add(result_frame, weight=1)

tk.Label(result_frame,
         text="Result Summary",
         font=("Segoe UI", 14, "bold"),
         bg=COLORS["bg"],
         fg=COLORS["fg"]).pack(anchor=tk.W, pady=(0, 15))

# Create a frame for the table and scrollbar
table_frame = tk.Frame(result_frame, bg=COLORS["bg"])
table_frame.pack(fill=tk.BOTH, expand=True)

# Create scrollbar with modern styling
scrollbar = ttk.Scrollbar(table_frame)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

result_table = ttk.Treeview(table_frame,
                           columns=("Function", "Value", "Runtime", "Dataset"),
                           show="headings",
                           height=10,
                           yscrollcommand=scrollbar.set)
result_table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
scrollbar.config(command=result_table.yview)

# Configure columns with modern proportions
result_table.heading("Function", text="Function")
result_table.heading("Value", text="Output Value")
result_table.heading("Runtime", text="Runtime (s)")
result_table.heading("Dataset", text="Dataset")
result_table.column("Function", width=300)
result_table.column("Value", width=300)
result_table.column("Runtime", width=150)
result_table.column("Dataset", width=250)

# Performance metrics chart section
chart_frame = tk.Frame(results_paned, bg=COLORS["bg"])
results_paned.add(chart_frame, weight=1)

tk.Label(chart_frame,
         text="Performance Metrics",
         font=("Segoe UI", 14, "bold"),
         bg=COLORS["bg"],
         fg=COLORS["fg"]).pack(anchor=tk.W, pady=(15, 15))

# Exit button with modern styling
exit_btn = tk.Button(main_frame,
                     text="Exit",
                     width=30,
                     font=("Segoe UI", 12),
                     bg=COLORS["button_bg"],
                     fg=COLORS["fg"],
                     command=safe_exit,
                     relief=tk.FLAT,
                     borderwidth=0,
                     padx=15,
                     pady=8)
exit_btn.pack(pady=20)
exit_btn.bind("<Enter>", on_enter)
exit_btn.bind("<Leave>", on_leave)

# Status bar with modern styling
status_frame = tk.Frame(root, bg=COLORS["status_bg"], height=35)
status_frame.pack(fill=tk.X, side=tk.BOTTOM)
status_var = tk.StringVar(value="Ready.")
tk.Label(status_frame,
         textvariable=status_var,
         anchor='w',
         bg=COLORS["status_bg"],
         fg=COLORS["status_fg"],
         font=("Segoe UI", 11),
         padx=20,
         pady=6).pack(fill=tk.X)

root.mainloop()
