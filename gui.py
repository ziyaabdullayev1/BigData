import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
import subprocess
import threading
import os
import time
from datetime import datetime

try:
    import matplotlib
    matplotlib.use('TkAgg')  # Set the backend before importing pyplot
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
except ImportError as e:
    print(f"Error importing matplotlib: {e}")
    print("Please ensure matplotlib is installed: pip install matplotlib")
    raise

try:
    import pandas as pd
except ImportError as e:
    print(f"Error importing pandas: {e}")
    print("Please ensure pandas is installed: pip install pandas")
    raise

# === Constants ===
HADOOP_STREAMING_JAR = r"C:/hadoop-2.7.7/share/hadoop/tools/lib/hadoop-streaming-2.7.7.jar"

# === Color Scheme - Modern Dark Theme ===
COLORS = {
    "bg_dark": "#121212",          # Primary background
    "bg_medium": "#1E1E1E",        # Secondary background
    "bg_light": "#2D2D2D",         # Tertiary background
    "fg": "#FFFFFF",               # Primary text
    "fg_dim": "#BBBBBB",           # Secondary text
    "accent_primary": "#3F51B5",   # Primary accent (Indigo)
    "accent_secondary": "#5C6BC0", # Secondary accent (lighter Indigo)
    "accent_success": "#43A047",   # Success color
    "accent_error": "#E53935",     # Error color
    "accent_warning": "#FFB300",   # Warning color
    "border": "#383838",           # Border color
    "chart_colors": ["#3F51B5", "#43A047", "#FFB300", "#8E24AA", "#E53935"]  # Chart colors
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
    # Update status
    set_status("Files refreshed", "info")

def on_enter(e):
    """Handle mouse enter event for buttons - hover effect."""
    if e.widget.cget('state') != 'disabled':
        e.widget.config(bg=COLORS["bg_light"])

def on_leave(e):
    """Handle mouse leave event for buttons - restore normal color."""
    if e.widget.cget('state') != 'disabled':
        e.widget.config(bg=COLORS["bg_medium"])

def set_status(message, status_type="info"):
    """Set status bar message with appropriate styling."""
    status_var.set(message)
    if status_type == "error":
        status_frame.config(bg=COLORS["accent_error"])
    elif status_type == "success":
        status_frame.config(bg=COLORS["accent_success"])
    elif status_type == "warning":
        status_frame.config(bg=COLORS["accent_warning"])
    else:  # info
        status_frame.config(bg=COLORS["accent_primary"])

def restore_ui_state():
    output_text.insert(tk.END, "[UI] Ready for new job\n")
    for btn in function_buttons:
        btn.config(state="normal")
    # Enable all tabs
    notebook.tab(0, state="normal")
    notebook.tab(1, state="normal")
    notebook.tab(2, state="normal")

def extract_result_from_hdfs(output_dir):
    try:
        cmd = f"hadoop fs -cat {output_dir}/part-00000"
        result = subprocess.check_output(cmd, shell=True, text=True)
        output_text.insert(tk.END, f"\n[HDFS Output]\n{result}\n")
        lines = result.strip().splitlines()
        for line in lines:
            parts = line.strip().split('\t')
            if len(parts) == 2:
                key, val = parts[0], parts[1]
                if key == "Min-Max":
                    return "Min-Max Normalization", val
                return key, val
        return "Unknown", "N/A"
    except Exception as e:
        output_text.insert(tk.END, f"[ERROR] Failed to read result: {e}\n")
        return "Error", "N/A"

def safe_exit():
    if status_var.get().startswith("Running"):
        messagebox.showwarning("Wait", "Job is running. Please wait.")
    else:
        root.quit()

def update_performance_chart():
    """Update performance chart with runtime data from executed jobs."""
    if not performance_metrics.metrics:
        return
    
    # Clear previous chart
    for widget in chart_frame.winfo_children():
        widget.destroy()
    
    # Create single chart
    fig = plt.figure(figsize=(10, 5), dpi=100)
    fig.patch.set_facecolor(COLORS["bg_dark"])
    ax = fig.add_subplot(111)
    
    # Get data for comparison
    comparison_data = performance_metrics.get_comparison_data()
    
    if not comparison_data.empty:
        x = range(len(comparison_data['Function']))
        functions = comparison_data['Function'].values
        
        # Bar chart data
        mean_values = comparison_data['mean'].values.astype(float)
        min_values = comparison_data['min'].values.astype(float)
        max_values = comparison_data['max'].values.astype(float)
        
        # Create bars with custom colors based on function
        bars = ax.bar(
            x, mean_values, 
            width=0.6,
            yerr=[mean_values - min_values, max_values - mean_values],
            capsize=4,
            color=[jobs.get(func, (None, COLORS["chart_colors"][i % len(COLORS["chart_colors"])]))[1] 
                  for i, func in enumerate(functions)],
            alpha=0.8,
            label='Average Runtime'
        )
        
        # Style the chart
        ax.set_title('Runtime Performance by Function', 
                   color=COLORS["fg"],
                   fontsize=12,
                   fontweight='bold')
        
        # Set x-axis labels
        ax.set_xticks(x)
        ax.set_xticklabels(functions, rotation=25, ha='right', fontsize=10)
        
        # Set y-axis labels
        ax.set_ylabel('Runtime (seconds)', color=COLORS["fg"], fontsize=11)
        
        # Style chart appearance
        ax.set_facecolor(COLORS["bg_dark"])
        ax.tick_params(colors=COLORS["fg"], labelsize=10)
        ax.spines['bottom'].set_color(COLORS["border"])
        ax.spines['top'].set_color(COLORS["border"])
        ax.spines['left'].set_color(COLORS["border"])
        ax.spines['right'].set_color(COLORS["border"])
        
        # Add grid for readability
        ax.grid(True, linestyle='--', alpha=0.2, color=COLORS["fg"])
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width()/2., 
                height + 0.1,
                f'{height:.2f}s',
                ha='center',
                va='bottom',
                color=COLORS["fg"],
                fontsize=9
            )
        
        # Add legend with custom styling
        legend = ax.legend(
            loc='upper right', 
            framealpha=0.8,
            facecolor=COLORS["bg_medium"], 
            edgecolor=COLORS["border"]
        )
        for text in legend.get_texts():
            text.set_color(COLORS["fg"])
    
    plt.tight_layout()
    
    # Embed chart in tkinter
    canvas = FigureCanvasTkAgg(fig, master=chart_frame)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

def run_job(job_label):
    selected_file = file_var.get()
    if not selected_file:
        messagebox.showwarning("Missing File", "Please select an input file.")
        return

    job_key, color = jobs[job_label]
    output_dir = f"/kaggle_output_{job_key}"

    # Switch to the Job Output tab
    notebook.select(1)
    
    # Update status
    set_status(f"Running {job_label}...", "warning")
    
    # Clear output
    output_text.delete(1.0, tk.END)
    output_text.insert(tk.END, f"Starting {job_label} job with file {selected_file}...\n\n")

    # Disable function buttons during job run
    for btn in function_buttons:
        btn.config(state="disabled")
        
    # Disable other tabs during processing
    notebook.tab(0, state="disabled")
    notebook.tab(2, state="disabled")

    def thread_job():
        try:
            if not skip_delete.get():
                subprocess.run(["hadoop", "fs", "-rm", "-r", output_dir], shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                output_text.insert(tk.END, f"[INFO] Removed previous output directory: {output_dir}\n")
            else:
                output_text.insert(tk.END, f"[INFO] Keeping previous output directory: {output_dir}\n")

            start = time.time()

            cmd = [
                "C:/hadoop-2.7.7/bin/hadoop.cmd", "jar", HADOOP_STREAMING_JAR,
                "-input", f"/kaggleinput/{selected_file}",
                "-output", output_dir,
                "-mapper", "C:/Users/abdul/AppData/Local/Programs/Python/Python313/python.exe mapper.py",
                "-reducer", f"C:/Users/abdul/AppData/Local/Programs/Python/Python313/python.exe stats_reducer.py {job_key}"
            ]

            output_text.insert(tk.END, f"[COMMAND] {' '.join(cmd)}\n\n")
            
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
                
                # Update status
                root.after(0, lambda: set_status(f"{job_label} completed in {duration}s", "success"))
                
                # Update performance chart and switch to results tab
                root.after(0, update_performance_chart)
                root.after(100, lambda: notebook.select(2))
            else:
                root.after(0, lambda: set_status(f"{job_label} failed after {duration}s", "error"))
        except Exception as e:
            output_text.insert(tk.END, f"[FATAL ERROR] {e}\n")
            root.after(0, lambda: set_status(f"Error: {str(e)[:50]}...", "error"))
        finally:
            root.after(0, restore_ui_state)

    threading.Thread(target=thread_job).start()

# === Main GUI Class ===
class BigDataGUI:
    def __init__(self, root):
        self.root = root
        self.setup_window()
        self.setup_styles()
        self.create_notebook()
        self.create_status_bar()
        
    def setup_window(self):
        """Configure the main window."""
        self.root.title("Big Data Analytics Dashboard")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)
        self.root.configure(bg=COLORS["bg_dark"])
        
    def setup_styles(self):
        """Configure ttk styles for the application."""
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Configure notebook style
        self.style.configure(
            "TNotebook",
            background=COLORS["bg_dark"],
            borderwidth=0
        )
        self.style.configure(
            "TNotebook.Tab",
            background=COLORS["bg_medium"],
            foreground=COLORS["fg_dim"],
            padding=[15, 5],
            borderwidth=0
        )
        self.style.map(
            "TNotebook.Tab",
            background=[("selected", COLORS["accent_primary"])],
            foreground=[("selected", COLORS["fg"])]
        )
        
        # Configure Treeview style
        self.style.configure(
            "Treeview",
            background=COLORS["bg_medium"],
            foreground=COLORS["fg"],
            fieldbackground=COLORS["bg_medium"],
            rowheight=30,
            borderwidth=0
        )
        self.style.configure(
            "Treeview.Heading",
            background=COLORS["bg_light"],
            foreground=COLORS["fg"],
            relief="flat",
            font=("Segoe UI", 10, "bold"),
            borderwidth=1
        )
        self.style.map(
            "Treeview.Heading",
            background=[("active", COLORS["accent_secondary"])]
        )
        
        # Configure Combobox style
        self.style.configure(
            "TCombobox",
            background=COLORS["bg_medium"],
            fieldbackground=COLORS["bg_medium"],
            foreground=COLORS["fg"],
            arrowcolor=COLORS["fg"],
            borderwidth=1
        )
        
        # Configure Frame style
        self.style.configure(
            "TFrame",
            background=COLORS["bg_dark"]
        )
        
        # Configure Scrollbar style
        self.style.configure(
            "TScrollbar",
            background=COLORS["bg_medium"],
            troughcolor=COLORS["bg_dark"],
            borderwidth=0,
            arrowcolor=COLORS["fg"]
        )
        
    def create_notebook(self):
        """Create the tabbed interface."""
        global notebook
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=(10, 0))
        
        self.create_input_tab()
        self.create_output_tab()
        self.create_results_tab()
        
    def create_input_tab(self):
        """Create the input/control tab."""
        global file_var, file_dropdown, skip_delete, function_buttons
        
        input_frame = ttk.Frame(notebook, style="TFrame")
        notebook.add(input_frame, text="  Job Configuration  ")
        
        # Main container with padding
        main_container = tk.Frame(input_frame, bg=COLORS["bg_dark"], padx=30, pady=20)
        main_container.pack(fill=tk.BOTH, expand=True)
        
        # Header
        header_frame = tk.Frame(main_container, bg=COLORS["bg_dark"], pady=10)
        header_frame.pack(fill=tk.X)
        
        tk.Label(
            header_frame,
            text="Big Data Statistical Processing",
            font=("Segoe UI", 22, "bold"),
            fg=COLORS["fg"],
            bg=COLORS["bg_dark"]
        ).pack()
        
        tk.Label(
            header_frame,
            text="Configure and run statistical analysis on big datasets",
            font=("Segoe UI", 11),
            fg=COLORS["fg_dim"],
            bg=COLORS["bg_dark"]
        ).pack(pady=(5, 15))
        
        # Input file section - styled as a card
        file_card = tk.Frame(
            main_container,
            bg=COLORS["bg_medium"],
            highlightbackground=COLORS["border"],
            highlightthickness=1,
            bd=0
        )
        file_card.pack(fill=tk.X, pady=15, ipady=15)
        
        # Card header
        tk.Label(
            file_card,
            text="Dataset Selection",
            font=("Segoe UI", 14, "bold"),
            fg=COLORS["fg"],
            bg=COLORS["bg_medium"]
        ).pack(anchor=tk.W, padx=20, pady=(15, 5))
        
        # Card content
        file_selection_frame = tk.Frame(file_card, bg=COLORS["bg_medium"], padx=20)
        file_selection_frame.pack(fill=tk.X, pady=10)
        
        file_var = tk.StringVar()
        
        # File label
        tk.Label(
            file_selection_frame,
            text="Select Input File:",
            font=("Segoe UI", 11),
            bg=COLORS["bg_medium"],
            fg=COLORS["fg"]
        ).pack(anchor=tk.W, pady=(0, 5))
        
        # File dropdown and refresh button in a frame
        dropdown_frame = tk.Frame(file_selection_frame, bg=COLORS["bg_medium"])
        dropdown_frame.pack(fill=tk.X)
        
        file_dropdown = ttk.Combobox(
            dropdown_frame,
            textvariable=file_var,
            font=("Segoe UI", 10),
            style="TCombobox",
            state="readonly",
            width=50
        )
        file_dropdown["values"] = get_input_files()
        file_dropdown.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        refresh_btn = tk.Button(
            dropdown_frame,
            text="Refresh Files",
            font=("Segoe UI", 10),
            bg=COLORS["bg_light"],
            fg=COLORS["fg"],
            activebackground=COLORS["accent_primary"],
            activeforeground=COLORS["fg"],
            command=refresh_files,
            relief=tk.FLAT,
            borderwidth=0,
            padx=15,
            pady=6,
            cursor="hand2"
        )
        refresh_btn.pack(side=tk.LEFT, padx=(10, 0))
        refresh_btn.bind("<Enter>", on_enter)
        refresh_btn.bind("<Leave>", on_leave)
        
        # Skip checkbox
        skip_frame = tk.Frame(file_selection_frame, bg=COLORS["bg_medium"], pady=10)
        skip_frame.pack(fill=tk.X, anchor=tk.W)
        
        skip_delete = tk.BooleanVar()
        
        skip_checkbox = tk.Checkbutton(
            skip_frame,
            text="Skip deleting old HDFS output",
            variable=skip_delete,
            bg=COLORS["bg_medium"],
            fg=COLORS["fg"],
            selectcolor=COLORS["bg_dark"],
            activebackground=COLORS["bg_medium"],
            activeforeground=COLORS["fg"],
            font=("Segoe UI", 10)
        )
        skip_checkbox.pack(anchor=tk.W)
        
        # Functions section - styled as a card
        functions_card = tk.Frame(
            main_container,
            bg=COLORS["bg_medium"],
            highlightbackground=COLORS["border"],
            highlightthickness=1,
            bd=0
        )
        functions_card.pack(fill=tk.X, pady=15, ipady=15)
        
        # Card header
        tk.Label(
            functions_card,
            text="Statistical Functions",
            font=("Segoe UI", 14, "bold"),
            fg=COLORS["fg"],
            bg=COLORS["bg_medium"]
        ).pack(anchor=tk.W, padx=20, pady=(15, 15))
        
        # Functions grid
        functions_grid = tk.Frame(functions_card, bg=COLORS["bg_medium"], padx=20)
        functions_grid.pack(fill=tk.X)
        
        function_buttons = []
        
        # Functions arranged in a grid (2 columns)
        col, row = 0, 0
        max_cols = 2
        
        for label, (func, color) in jobs.items():
            # Create a card-like frame for each function button
            btn_container = tk.Frame(
                functions_grid,
                bg=COLORS["bg_medium"],
                padx=10,
                pady=5
            )
            btn_container.grid(row=row, column=col, sticky="ew", padx=5, pady=5)
            
            # Configure grid column weights
            functions_grid.grid_columnconfigure(0, weight=1)
            functions_grid.grid_columnconfigure(1, weight=1)
            
            # Function button with icon indicator
            btn_frame = tk.Frame(btn_container, bg=COLORS["bg_medium"])
            btn_frame.pack(fill=tk.X)
            
            # Color indicator
            indicator = tk.Frame(btn_frame, bg=color, width=4)
            indicator.pack(side=tk.LEFT, fill=tk.Y)
            
            # Button
            btn = tk.Button(
                btn_frame,
                text=label,
                font=("Segoe UI", 11),
                bg=COLORS["bg_medium"],
                fg=COLORS["fg"],
                activebackground=COLORS["accent_secondary"],
                activeforeground=COLORS["fg"],
                command=lambda l=label: run_job(l),
                relief=tk.FLAT,
                borderwidth=0,
                padx=20,
                pady=12,
                anchor="w",
                justify=tk.LEFT,
                cursor="hand2"
            )
            btn.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            btn.bind("<Enter>", on_enter)
            btn.bind("<Leave>", on_leave)
            
            function_buttons.append(btn)
            
            # Advance to next position
            col += 1
            if col >= max_cols:
                col = 0
                row += 1
        
        # Help text
        help_frame = tk.Frame(main_container, bg=COLORS["bg_dark"], pady=15)
        help_frame.pack(fill=tk.X)
        
        tk.Label(
            help_frame,
            text="Select a file and function to start processing.",
            font=("Segoe UI", 10, "italic"),
            fg=COLORS["fg_dim"],
            bg=COLORS["bg_dark"]
        ).pack(side=tk.LEFT)
        
        # Exit button on the right
        exit_btn = tk.Button(
            help_frame,
            text="Exit Application",
            font=("Segoe UI", 10),
            bg=COLORS["bg_medium"],
            fg=COLORS["fg"],
            activebackground=COLORS["accent_error"],
            activeforeground=COLORS["fg"],
            command=safe_exit,
            relief=tk.FLAT,
            borderwidth=0,
            padx=20,
            pady=8,
            cursor="hand2"
        )
        exit_btn.pack(side=tk.RIGHT)
        exit_btn.bind("<Enter>", on_enter)
        exit_btn.bind("<Leave>", on_leave)
        
    def create_output_tab(self):
        """Create the job output tab."""
        global output_text
        
        output_frame = ttk.Frame(notebook, style="TFrame")
        notebook.add(output_frame, text="  Job Output  ")
        
        # Container with padding
        container = tk.Frame(output_frame, bg=COLORS["bg_dark"], padx=20, pady=20)
        container.pack(fill=tk.BOTH, expand=True)
        
        # Header
        tk.Label(
            container,
            text="Hadoop Job Output",
            font=("Segoe UI", 16, "bold"),
            fg=COLORS["fg"],
            bg=COLORS["bg_dark"]
        ).pack(anchor=tk.W, pady=(0, 10))
        
        # Output console with custom styling
        console_frame = tk.Frame(
            container,
            bg=COLORS["bg_medium"],
            highlightbackground=COLORS["border"],
            highlightthickness=1
        )
        console_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create the output text widget with a custom scrollbar
        output_text_frame = tk.Frame(console_frame, bg=COLORS["bg_medium"])
        output_text_frame.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        
        output_text = scrolledtext.ScrolledText(
            output_text_frame,
            font=("Consolas", 10),
            bg=COLORS["bg_dark"],
            fg=COLORS["fg"],
            insertbackground=COLORS["fg"],
            selectbackground=COLORS["accent_primary"],
            selectforeground=COLORS["fg"],
            borderwidth=0,
            relief=tk.FLAT
        )
        output_text.pack(fill=tk.BOTH, expand=True)
        
    def create_results_tab(self):
        """Create the results tab with table and chart."""
        global result_table, chart_frame
        
        results_frame = ttk.Frame(notebook, style="TFrame")
        notebook.add(results_frame, text="  Results & Analytics  ")
        
        # Container with padding
        container = tk.Frame(results_frame, bg=COLORS["bg_dark"], padx=20, pady=20)
        container.pack(fill=tk.BOTH, expand=True)
        
        # Use PanedWindow to allow resizing between table and chart
        paned_window = ttk.PanedWindow(container, orient=tk.VERTICAL)
        paned_window.pack(fill=tk.BOTH, expand=True)
        
        # Results table section
        table_container = tk.Frame(paned_window, bg=COLORS["bg_dark"])
        paned_window.add(table_container, weight=1)
        
        # Table header
        tk.Label(
            table_container,
            text="Job Results Summary",
            font=("Segoe UI", 16, "bold"),
            fg=COLORS["fg"],
            bg=COLORS["bg_dark"]
        ).pack(anchor=tk.W, pady=(0, 10))
        
        # Table with scrollbar
        table_frame = tk.Frame(
            table_container,
            bg=COLORS["bg_medium"],
            highlightbackground=COLORS["border"],
            highlightthickness=1
        )
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))
        
        # Create scrollbar
        scrollbar = ttk.Scrollbar(table_frame, style="TScrollbar")
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Create results table
        result_table = ttk.Treeview(
            table_frame,
            columns=("Function", "Value", "Runtime", "Dataset"),
            show="headings",
            style="Treeview",
            yscrollcommand=scrollbar.set
        )
        result_table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=result_table.yview)
        
        # Configure columns
        result_table.heading("Function", text="Function")
        result_table.heading("Value", text="Output Value")
        result_table.heading("Runtime", text="Runtime (s)")
        result_table.heading("Dataset", text="Dataset")
        
        # Set column widths as proportions
        result_table.column("Function", width=200)
        result_table.column("Value", width=200)
        result_table.column("Runtime", width=100)
        result_table.column("Dataset", width=200)
        
        # Performance analytics section
        chart_container = tk.Frame(paned_window, bg=COLORS["bg_dark"])
        paned_window.add(chart_container, weight=1)
        
        # Chart header
        tk.Label(
            chart_container,
            text="Performance Analytics",
            font=("Segoe UI", 16, "bold"),
            fg=COLORS["fg"],
            bg=COLORS["bg_dark"]
        ).pack(anchor=tk.W, pady=(0, 10))
        
        # Chart frame with border
        chart_frame = tk.Frame(
            chart_container,
            bg=COLORS["bg_medium"],
            highlightbackground=COLORS["border"],
            highlightthickness=1
        )
        chart_frame.pack(fill=tk.BOTH, expand=True)
        
    def create_status_bar(self):
        """Create a status bar at the bottom of the window."""
        global status_frame, status_var
        
        status_frame = tk.Frame(
            self.root,
            bg=COLORS["accent_primary"],
            height=28
        )
        status_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        status_var = tk.StringVar(value="Ready.")
        
        status_label = tk.Label(
            status_frame,
            textvariable=status_var,
            anchor="w",
            bg=COLORS["accent_primary"],
            fg=COLORS["fg"],
            font=("Segoe UI", 10),
            padx=20,
            pady=4
        )
        status_label.pack(fill=tk.X)

# === Main Application ===
if __name__ == "__main__":
    root = tk.Tk()
    app = BigDataGUI(root)
    root.mainloop()
