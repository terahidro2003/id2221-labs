# Evaluation Report

## 1. Incremental Update Performance

### Ingest Comparison (Baseline vs. Incremental)

| Dataset | Baseline Time | Incremental Time | Speedup | Time Saved (%) |
| :--- | :---: | :---: | :---: | :---: |
| taxi_trips | 15.60s | 7.39s | 2.11x | 52.6% |
| weather | 7.90s | 0.66s | 11.91x | 91.6% |
| air_quality | 13.35s | 0.85s | 15.74x | 93.6% |
| Total Ingest | 36.84s | 8.90s | 4.14x | 75.8% |

Incremental ingestion reduces total dataset ingest time from 36.84s to 8.90s, achieving an overall 4.14x speedup and saving 75.8% of processing time. Dataset Variations: Smaller datasets saw massive performance gains, >11x speedup, as incremental processing eliminates full raw file re-reading.

### Incremental Pipeline Phase Breakdown

The complete end-to-end incremental pipeline run took 88.14 seconds:

| Pipeline Phase | Processing Time | Share of Total |
| :--- | :---: | :---: |
| Bronze Ingest | 8.88s | 10.1% | 
| Silver Promote | 54.76s | 62.1% | 
| Gold Integrate | 24.50s | 27.8% |
| Total Pipeline | 88.14s | 100.0% |


## 2. Analytical Refresh Performance

* Gold Data Products Refresh: 20.16 seconds

### Key Insights
Refreshing all 5 downstream Gold data products (daily_borough_mobility, taxi_zone_monthly_demand, weather_impact_summary, air_quality_demand_summary, and zone_weather_sensitivity) completes in 20.16 seconds. This fast refresh cycle ensures that analytical models maintain a less compute. 



## 3. Validation Statistics (Data Quality Overhead)

Validation rules were applied across all three datasets during Silver promotion, taking a total execution time of 8.51 seconds (down from prior runs of 15.42s).

| Dataset | Validation Time | Records | Rejected Records | Rejection Rate (%) |
| :--- | :---: | :---: | :---: | :---: |
| taxi_trips | 6.55s | 9,837,995 | 1,342,491 | 12.01% |
| weather | 0.69s | 8,872 | 88 | 0.98% |
| air_quality | 1.27s | 112,853 | 7,017 | 5.85% |
| Total / Summary | 8.51s | 9,959,720 | 1,349,596 | 11.93% |


## 4. Monitoring Results

Pipeline state and lineage were tracked via Delta appends to the operational log table (ops/pipeline_runs).

* Single Append Overhead: 0.242s per step
* Sample Latency (3 Appends): 0.725s
* Estimated Full Pipeline (12 Steps): ~2.90s
* Ops Storage Footprint: 0.36 MB

### Operational Impact
At ~2.90 seconds total, operational logging accounts for only 3.29% of the full 88.14s incremental pipeline run time, proving that full step-level auditability introduces negligible performance cost.

---

## 5. Storage Overhead & Footprint Analysis

| Layer / Storage Zone | Size (MB) | Share of Data Lake |
| :--- | :---: | :---: |
| Raw Landing Zone | 2,429.96 MB | — |
| Bronze | 541.78 MB | 19.8% |
| Silver | 763.18 MB | 27.9% |
| Gold (Integrated & Dual Layout) | 1,431.12 MB | 52.3% |
| Gold (Data Products) | 2.03 MB | 0.1% |
| Rejects (Quarantine) | 47.71 MB | 1.7% |
| Ops (Monitoring Log) | 0.36 MB | <0.1% |
| Total Lakehouse Storage | 2,736.44 MB | 100.0% |

### Platform Ratios
* Platform / Raw Ratio: 1.13x (2,736.44 MB lake vs. 2,429.96 MB raw)
* Storage Growth This Evaluation: 804.15 MB

A multiplier of 1.13x confirms high storage efficiency: the lakehouse maintains full Bronze-Silver-Gold lineage, dual layout partitioning, and error quarantine with only a 13% storage expansion over raw inputs.

---

## 6. Maintainability Analysis

The Medallion layer boundaries (Bronze -> Silver -> Gold) restrict schema changes or ingestion failures to single isolated steps, preventing downstream cascading corruptions. Furthermore, we have explicit error handling. Malformed rows are non-destructively moved to data/lake/rejects, enabling asynchronous triage without blocking active pipeline workflows. Finally there is low maintenance auditing implemented. Automatic operational logging (ops/pipeline_runs) provides continuous execution monitoring without requiring external monitoring tool integration.

---

## 7. Discussion of Scalability

There are some compute bottlenecks, silver promotion represents 62.1% (54.76s) of incremental processing time due to row-level validation. As transaction volumes expand, finding ways to optimize the validation process will be essential to sustain linear scaling The layout storage has some trade offs where the Gold layer accounts for 52.3% of overall storage, driven largely by dual borough partitioning layouts. While this layout guarantees sub-second query execution, adding further dimensional partitions will scale storage non-linearly, requiring automated compaction and retention policies. Here it could be beneficial to deside on a single partitioning strategy for the Gold layer to reduce storage overhead while still meeting query performance requirements.
Finally, incremental scaling path where ingestion time dropped to 8.90s (75.8% time savings vs baseline), proving that the append only Delta log design successfully scales performance relative to batch delta size rather than total historical dataset size.