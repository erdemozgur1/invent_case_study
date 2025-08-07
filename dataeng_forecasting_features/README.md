#  Invent.ai Data Engineering Case Study

##  Objective

The goal of this assessment is to build a mini ETL pipeline that processes retail sales data using **Python and Pandas**. The pipeline reads and filters data, engineers time-series features, evaluates product performance using **WMAPE**, and outputs the results in a structured way.

---

##  Getting Started

You can run this project in two ways:

###  Option 1: Run with Docker (Recommended)

> Requires: [Docker installed](https://www.docker.com/products/docker-desktop/)

#### 1. Build the Docker image

- Firstly, navigate to the project directory:

```bash
cd dataeng_forecasting_features
```
- Then build your docker image

```bash
docker build -t invent_ai_data_eng .
```

#### 2. Run the pipeline

```bash
docker run --rm `
  --env-file .env `
  -v ${PWD}/input_data:/app/input_data `
  -v ${PWD}/output_data:/app/output_data `
  invent_ai_data_eng `
  --min-date 2021-01-08 `
  --max-date 2021-01-16 `
  --top 10
```
This command mounts your local input and output folders to the container and executes the pipeline.

### Option 2: Run Locally (Without Docker)

1. Firstly, go into this project.

```bash
cd dataeng_forecasting_features
```

2. Create a virtual environment

`python -m venv venv`

3. Activate the virtual environment

#### Windows
`.\venv\Scripts\activate`

#### macOS/Linux
`source venv/bin/activate`

4. Install dependencies

`pip install -r requirements.txt`

5. Run the pipeline

`python solution.py --min-date 2021-01-08 --max-date 2021-01-20 --top 10`
(arg values can be changed according to requirements)

### Command Line Arguments
| Argument     | Description                         | Example         |
| ------------ | ----------------------------------- | --------------- |
| `--min-date` | Start date for filtering sales data | `2021-01-08`    |
| `--max-date` | End date for filtering sales data   | `2021-05-30`    |
| `--top`      | Number of top products by WMAPE     | `5`, `10`, etc. |



## Logic

### Step 1: Load Input Data
Reads CSVs from /input_data/data/:


### Step 2: Data Transformation & Calculation
Filters sales between --min-date and --max-date.

Merges sales with product, brand, and store metadata.

Computes time-series features:

7-day moving average (MA7)

7-day lag (LAG7)

Feature levels:

Product-level: MA7_P, LAG7_P

Brand-level: MA7_B, LAG7_B

Store-level: MA7_S, LAG7_S

### Step 3: Calculate WMAPE

Ranks top N products with the highest WMAPE based on --top argument.

### Step 4: Output Results
Writes processed data to:

output_data/features.csv

output_data/mapes.csv

# Project Structure
```bash
dataeng_forecasting_features/
├── input_data/ # Input CSV files
│ └── data/
│ ├── sales.csv
│ ├── product.csv
│ ├── brand.csv
│ └── store.csv
├── output_data/ # Output CSV files (features.csv, mapes.csv)
├── solution.py # Main pipeline script
├── utils.py # Utility functions
├── Dockerfile # Docker build configuration
├── requirements.txt # Python dependencies
├── .env # Environment variables
├── README.md # This documentation
└── description.md # Case study description
```

| Tool     | Purpose                   |
| -------- | ------------------------- |
| Python   | Core programming language |
| Pandas   | Data transformation       |
| Docker   | Containerization          |
| argparse | Command-line parsing      |

