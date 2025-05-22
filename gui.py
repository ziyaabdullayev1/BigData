import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
import subprocess
import threading
import os
import time

# === Hadoop Streaming JAR path ===
HADOOP_STREAMING_JAR = r"C:/hadoop-2.7.7/share/hadoop/tools/lib/hadoop-streaming-2.7.7.jar"

# === Supported jobs ===
jobs = {
    "Median": ("median", "#4caf50"),
    "Standard Deviation": ("stddev", "#2196f3"),
    "Min-Max Normalization": ("minmax", "#ff9800"),
    "90th Percentile": ("percentile", "#9c27b0"),
    "Skewness": ("skewness", "#f44336")
}

def restore_ui_state():
    output_text.insert(tk.END, "[UI] Buttons re-enabled\n")
    for btn in function_buttons:
        btn.config(state="normal")
    exit_btn.config(state="normal")

def get_input_files():
    input_path = os.path.join(os.getcwd(), "kaggleinput")
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    return [f for f in os.listdir(input_path) if f.endswith(".txt")]

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
                root.after(0, lambda: result_table.insert("", "end", values=(func, val, duration)))
                root.after(0, lambda: status_var.set(f"✅ {job_label} completed in {duration}s"))
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
root.title("Big Data Hadoop Job Launcher")
root.geometry("770x760")
root.configure(bg="#202020")

tk.Label(root, text="Big Data Statistical Function Launcher", font=("Arial", 16, "bold"), fg="white", bg="#202020").pack(pady=10)

# File dropdown
tk.Label(root, text="Select Input File:", bg="#202020", fg="white").pack()
file_var = tk.StringVar()
file_dropdown = ttk.Combobox(root, textvariable=file_var, width=60, font=("Arial", 10))
file_dropdown["values"] = get_input_files()
file_dropdown.pack(pady=5)

# Skip checkbox
skip_delete = tk.BooleanVar()
tk.Checkbutton(root, text="Skip deleting old HDFS output", variable=skip_delete, bg="#202020", fg="white", selectcolor="#202020").pack()

# Function buttons
tk.Label(root, text="Choose Function", bg="#202020", fg="white").pack(pady=5)
btn_frame = tk.Frame(root, bg="#202020")
btn_frame.pack()
function_buttons = []

for label, (func, color) in jobs.items():
    btn = tk.Button(btn_frame, text=label, width=30, font=("Arial", 11), bg=color, fg="white", command=lambda l=label: run_job(l))
    btn.pack(pady=3)
    function_buttons.append(btn)

# Exit
exit_btn = tk.Button(root, text="Exit", width=30, font=("Arial", 11), bg="#444", fg="white", command=safe_exit)
exit_btn.pack(pady=10)

# Output window
tk.Label(root, text="Job Output", bg="#202020", fg="white").pack()
output_text = scrolledtext.ScrolledText(root, width=90, height=10, font=("Courier", 9), bg="#111", fg="#00FF00")
output_text.pack(pady=5)

# Result table
tk.Label(root, text="Result Summary", bg="#202020", fg="white").pack()
result_table = ttk.Treeview(root, columns=("Function", "Value", "Runtime"), show="headings", height=5)
result_table.pack(pady=5)
result_table.heading("Function", text="Function")
result_table.heading("Value", text="Output Value")
result_table.heading("Runtime", text="Runtime (s)")
result_table.column("Function", width=200)
result_table.column("Value", width=150)
result_table.column("Runtime", width=120)

# Status bar
status_var = tk.StringVar(value="Ready.")
tk.Label(root, textvariable=status_var, anchor='w', bd=1, relief=tk.SUNKEN, bg="#1e1e1e", fg="lime", font=("Arial", 10)).pack(fill='x', side='bottom')

root.mainloop()
