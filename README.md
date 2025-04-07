#  IoT Sensor Data Analysis using Spark SQL

This project explores and analyzes IoT sensor data using **Apache Spark SQL** in Python (PySpark). The dataset contains temperature, humidity, timestamp, location, and sensor type for various sensor readings collected from different building floors.

---

## Dataset

**File:** `sensor_data.csv`  
**Fields:**
- `sensor_id` (Integer)
- `timestamp` (String → converted to Timestamp)
- `temperature` (Double)
- `humidity` (Double)
- `location` (String)
- `sensor_type` (String)

---

##  Tasks Overview

| Task | Description |
|------|-------------|
| **Task 1** | Load data, explore schema, view first rows, count records, get distinct locations |
| **Task 2** | Filter temperatures, perform aggregations by location |
| **Task 3** | Time-based analysis: extract hour, compute average temperature |
| **Task 4** | Rank sensors using average temperature with window functions |
| **Task 5** | Pivot table of avg temperature by location and hour |

Each task saves its output in a structured directory under `/output`.

---

##  How to Run

Make sure you are inside the project folder that contains all task scripts and `sensor_data.csv`.

### Step 1: Set Up Spark Environment

If not already installed:

```bash
pip install pyspark


# Task 1: Load & Basic Exploration
python3 Task1.py

# Task 2: Filtering & Aggregations
python3 Task2.py

# Task 3: Time-Based Analysis
python3 Task3.py

# Task 4: Sensor Ranking with Window Function
python3 Task4.py

# Task 5: Pivot Table by Hour and Location
python3 Task5.py


output/
├── task1/
│   ├── task1a_output/       # Distinct locations
│   ├── task1b_output/       # Total records
│   └── task1c_output/       # First 5 rows
├── task2/
│   └── task2_output/        # Aggregated results by location
├── task3/
│   └── task3_output/        # Avg temp by hour
├── task4/
│   └── task4_output/        # Top 5 ranked sensors
└── task5/
    └── task5_output/        # Pivot table
