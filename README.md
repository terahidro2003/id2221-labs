# KTH ID2221

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
