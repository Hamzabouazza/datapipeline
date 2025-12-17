# Data Generation Pipeline

Clean and structured data pipeline for generating synthetic device data.

## Structure

```
generateData/
├── core/                    # Infrastructure layer
│   ├── __init__.py
│   └── DataCore.py          # Spark session manager
│
├── job/                     # Business logic layer
│   ├── __init__.py
│   └── dbg.py               # Device data generator
│
├── main.py                  # Entry point
├── docker-compose.yml       # PostgreSQL setup
└── README.md
```

## Architecture

**Clean separation of concerns:**
- `core/` - Reusable infrastructure (Spark session management)
- `job/` - Business logic (data generation jobs)
- `main.py` - Orchestration and entry point

## Usage

### Run with main.py (Recommended)

```bash
# Activate environment
source ../env/bin/activate

# Run device data generator
python main.py --job device
```

### Run job directly

```bash
python -m job.dbg
```

## DataCore

Simple Spark session manager that provides:
- Clean Spark session creation
- Method chaining for configuration
- JDBC JAR configuration
- Proper resource cleanup

**Example from job/dbg.py:**
```python
from core.DataCore import DataCore

# Initialize and configure
data_core = DataCore(app_name="DeviceDataGenLocal", master="local[*]")

if jar_path:
    data_core.with_jdbc_jar(jar_path)

data_core.with_shuffle_partitions(4)

# Get Spark session
spark = data_core.spark

# Your logic here...

# Cleanup
data_core.stop()
```

## Environment Variables

Create a `.env` file in the project root:

```env
JAR_PATH=/path/to/postgresql-42.7.4.jar
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=device_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

## Adding New Jobs

1. Create a new file in `job/` directory (e.g., `job/new_job.py`)
2. Use `DataCore` for Spark session management
3. Add job runner function in `main.py`
4. Update argparse choices

Example:

```python
# job/new_job.py
from core.DataCore import DataCore

data_core = DataCore(app_name="NewJob")
spark = data_core.spark

# Your logic...

data_core.stop()
```

```python
# main.py
def run_new_job():
    from job import new_job
    
# Add to choices
choices=["device", "new_job"]
```

## Benefits

✅ **Clean Architecture** - Separation of infrastructure and business logic  
✅ **Reusable** - DataCore can be used across all jobs  
✅ **Maintainable** - Easy to add new jobs and modify existing ones  
✅ **Testable** - Each component can be tested independently  
✅ **Simple** - Clear entry point and structure
