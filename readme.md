# Big Data Statistical Functions with Hadoop & Python GUI

A desktop application for running exploratory statistical functions on large datasets using Hadoop Streaming and Python, with an interactive Tkinter GUI.

---

## 🚀 Project Overview

This project demonstrates how to:

- Deploy a single-node Hadoop 2.7.7 cluster on Windows  
- Write MapReduce jobs via **Hadoop Streaming** (`mapper.py` + `stats_reducer.py`)  
- Compute five statistical functions:
  1. Median  
  2. Standard Deviation  
  3. Min-Max Normalization  
  4. 90th Percentile  
  5. Skewness  
- Provide a **Tkinter GUI** to:
  - Select an input file  
  - Choose which function to run  
  - View live job logs  
  - See a **result summary table** (function, value, runtime)  
  - Skip HDFS cleanup if desired  

---

## 📂 Repository Structure

