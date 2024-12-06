# College Scorecard Database

## Description
This repository contains a comprehensive data pipeline and interactive dashboard for analyzing College Scorecard and IPEDS (Integrated Postsecondary Education Data System) data. The system includes database loading scripts, data processing utilities, and a Streamlit-based visualization dashboard.

---

## Components

### 1. Database Pipeline

#### Schema (`schema.sql`)
Defines the SQL schema for the database, creating four main tables:
- **Institutions**: Core institutional data (`UNITID` as the primary key).
- **College_Scorecard_Annual**: Annual performance metrics with `(UNITID, YEAR)` as the primary key.
- **Financial_Data**: Financial metrics and outcomes with `(OPEID, YEAR)` as the primary key.
- **Crosswalks**: Institutional identifier mappings with `(UNITID, OPEID)` as the primary key.

#### Data Loading Scripts
- **Shared Utilities** (`load.py`): Shared utilities for core data processing functions.
- **IPEDS Loader** (`load-ipeds.py`): Processes institutional data and populates the database.
- **Scorecard Loader** (`load-scorecard.py`): Handles annual metrics, including financial and performance data.

### 2. Interactive Dashboard (dashboard.py)
A Streamlit-based visualization tool that provides:
- Regional and state-level analysis.
- Year-over-year comparisons (2019-2022).
- Interactive filters and visualizations.

#### Key Metrics
1. Admission Rates
2. Tuition Trends (In-state vs Out-of-state)
3. Pell Grant Recipients
4. 3-Year Loan Default Rates
5. Median Earnings (8 years after entry)

---

## Setup Instructions

### 1. Database Setup

```bash
pip install pandas psycopg tqdm
```

Create `credentials.py`:

```python
DB_NAME = "YOUR_DATABASE_NAME"
DB_USER = "YOUR_USERNAME"
DB_PASSWORD = "YOUR_PASSWORD"
DB_HOST = "pinniped.postgres.database.azure.com"
```
Initialize schema:

```bash
psql -h pinniped.postgres.database.azure.com -U YOUR_USERNAME -d YOUR_DATABASE_NAME -f schema.sql
```

### 2. Dashboard Setup

Install additional requirements:

```bash
pip install streamlit plotly
```

Launch Dashboard:

```bash
streamlit run dashboard.py
```
## Data Pipeline Features

- **Batch Processing**: Includes progress tracking for efficient data loads.
- **Data Validation**: Ensures all data meets schema constraints before insertion.
- **Transaction Management**: Rolls back on errors for consistent database states.
- **Error Handling**: Provides detailed error reports for troubleshooting.
- **Data Versioning**: Supports loading and tracking updates over time.

---

## Dashboard Features

### Visualization Options
- Toggle between US Regions and States views.
- Year selection (2019-2022).
- Multiple state selection for detailed analysis.

### Metrics and Visualizations

1. **Admission Rates**
   - Average admission rates by region/state.
   - Data coverage statistics.
   - Color-coded bar charts.

2. **Tuition Analysis**
   - In-state vs out-of-state comparison.
   - Regional/state averages.
   - Grouped bar charts with hover details.

3. **Pell Grant Statistics**
   - Percentage of recipients by region/state.
   - Coverage metrics.
   - Heat-mapped visualizations.

4. **Default Rate Analysis**
   - 3-year loan default rates.
   - Regional/state comparisons.
   - Color-scaled visualizations.

5. **Earnings Data**
   - Median earnings 8 years after entry.
   - Regional/state comparisons.
   - Interactive bar charts.

---

### Data Coverage
- Each visualization includes data coverage statistics.
- Transparent reporting of available data points.
- Clear indication of missing or incomplete data.

---

### US Region Definitions
The dashboard uses the following regional classifications:

- **Northeast**: ME, NH, VT, MA, RI, CT, NY, NJ, PA.
- **Midwest**: OH, MI, IN, IL, WI, MN, IA, MO, ND, SD, NE, KS.
- **South**: DE, MD, DC, VA, WV, NC, SC, GA, FL, KY, TN, AL, MS, AR, LA, OK, TX.
- **West**: MT, ID, WY, CO, NM, AZ, UT, NV, WA, OR, CA, AK, HI.

---

### Best Practices

1. **Data Loading**
   - Run `schema.sql` first when setting up.
   - Load IPEDS before Scorecard data.
   - Verify file naming conventions.

2. **Dashboard Usage**
   - Start with regional view for broad trends.
   - Drill down to state level for details.
   - Consider data coverage when interpreting results.

---

### Notes
- Database hosted on `pinniped.postgres.database.azure.com`.
- PEP 8 compliant codebase.
- Comprehensive documentation.
- Created by Team Rhodes, 2024.



