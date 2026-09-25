# KTH ID2221

## Assignment 1

## How to Run

### Dependencies
Ensure Java 17 is installed and `JAVA_HOME` is set.
Run `pip install -r requirements.txt` in the root to install dependencies.

### Run data pipeline (CLI)

From the repository root:

```bash
python -m src.jobs.run_download
python -m src.jobs.run_bronze
python -m src.jobs.run_silver
python -m src.jobs.run_gold                 # --stage integrate|products|all
python -m src.jobs.run_monitoring_report   # ops metrics Spark SQL
```

Optional:

```bash
python -m src.jobs.run_bronze --dataset taxi_zones
python -m src.jobs.run_gold --stage integrate
python -m src.jobs.run_gold --stage products --force-products
python -m src.jobs.run_generator            # synthetic incremental raw updates
```

Each bronze/silver/gold dataset execution appends a row to
`data/lake/ops/pipeline_runs` (duration, processed/inserted/rejected counts,
schema version, validation failures). Inspect with
`python -m src.jobs.run_monitoring_report` or `notebooks/monitoring.ipynb`.

Dataset schemas, partitions, and DQ rules live under `config/datasets/`.
Type compatibility for schema checks is in `config/compatible_types.yaml`.

### Notebooks (exploration / thin runners)

Start Jupyter with `jupyter notebook` from the root. Notebooks call into `src/` and
are optional if you use the CLI jobs above.

| Notebook | Role |
|---|---|
| `notebooks/run_pipeline.ipynb` | **Full demo**: download → bronze/silver/gold → Q1–Q6 → optimizations → products → benchmarks → incremental re-ingest → monitoring |
| `notebooks/ingestion.ipynb` | Download + bronze + silver |
| `notebooks/integration.ipynb` | Gold integrate + exploratory Q1–Q6 |
| `notebooks/data_products.ipynb` | Gold data products |
| `notebooks/data_generator.ipynb` | Incremental raw updates |
| `notebooks/benchmark.ipynb` | Product vs on-demand / AQE benchmarks |
| `notebooks/query_optimization.ipynb` | Query optimization experiments |
| `notebooks/monitoring.ipynb` | Ops pipeline_runs metrics report |

## Assignment 2

Complete Assignment 1 (bronze → silver → gold) first so lake tables exist.

### Expected order

1. `python -m src.jobs.run_download` → `run_bronze` → `run_silver`
2. `python -m src.jobs.run_gold --stage integrate`
3. `python -m src.jobs.run_gold --stage products` (or `--stage all`)
4. Explore queries / run `notebooks/benchmark.ipynb`

### Analytical queries

Shared SQL (Q1–Q6) is in `src/queries/analytical.py`. Integration and benchmark
notebooks import these strings rather than duplicating them.

### Data products

Gold products are written under `data/lake/gold/data_products/` by
`src.gold.products` (daily borough mobility, monthly zone demand, weather impact,
air quality demand, zone weather sensitivity).
