# KTH ID2221

## Assignment 1

## How to Run

### Dependencies
Ensure Java 17 is installed and `JAVA_HOME` is set.
Run `pip install -r requirements.txt` in the root to install dependencies.

### Run data pipeline
Start your Jupyter environment by running `jupyter notebook` in the root directory.

Open and execute all cells in `notebooks/ingestion.ipynb` to:
- Download and extract the raw datasets.
- Validate schemas and load the raw data into Bronze Delta tables.
- Normalize timestamps, apply data quality checks, and promote the data to Silver Delta tables.

Open and execute all cells in `notebooks/integration.ipynb` to:
- Aggregate contextual data and execute broadcast joins to enrich the taxi trips.
- Write the final analytical dataset to the Gold layer.
- Execute the storage layout benchmark and analytical queries.

## Assignment 2

To run the analytical notebooks for Assignment 2, the steps from Assignment 1 must first be completed in order to set up the data. This means:

### Expected order of execution
1. `notebooks/ingestion.ipynb`
2. `notebooks/integration.ipynb`

### Analytical Queries
The analytical queries from Task 2 are found in `integration.ipynb` and are executed against the Gold-layer integrated dataset.

### Generate analytical data products
Open and execute all cells in `notebooks/data_products.ipynb` to create the data products used for benchmarking. This notebook builds tables such as `taxi_zone_monthly_demand`, `weather_impact_summary`, `air_quality_demand_summary`, and `zone_weather_sensitivity`. These are found under the `data_products` folder in the Gold layer.

### Run the benchmark experiments
Open and execute all cells in `notebooks/benchmark.ipynb`.



