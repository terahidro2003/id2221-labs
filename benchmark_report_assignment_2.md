## Benchmark Report

### 1. Benchmark methodology

The benchmark compared each original query against its optimized version, where the expensive aggregation was precomputed and stored as a Gold-layer Delta data product. For the base queries, they were run on the integrated taxi trip dataset, while the optimized queries read from the precomputed Delta table containing the precomputed results. In the AQE experiments, the same query was executed with adaptive query execution disabled and enabled to assess the effect of Spark’s runtime optimization. The runs were then checked to make sure that the results were the same, for example with number of rows, and comparing the sorted results. These were then timed and the speedup was calculated in order to better understand the performance improvements of the optimized queries.



### 3. Benchmark Results - Execution times

| Query | Baseline Time | Optimized Time | Speedup |
| --- | ---: | ---: | ---: |
| 1 | 1.37s | 0.23s | 83.4% |
| 2 | 1.25s | 0.24s | 81.1% |
| 3 | 1.72s | 0.19s | 89.0% |
| 4 | 5.41s | 0.34s | 93.7% |
| 5 | 0.49s | 0.42s | 15.2% |
| 6 | 0.52s | 0.17s | 66.6% |


### 4. Execution Plan Analysis

The EXPLAIN FORMATTED recommended in the assigment shows us that the baseline queries perform the whole collection on the integrated dataset, which resulted in a bigger and more expensive operator tree. For the optimized queries, we see that the precomputed results are read from the data products which drastically reduces the operations needed. This visually shows us why we have runtime improvements for queries 1-4. For queries 5 and 6, the operations logic is the same and same number of operations, but spark optimizes the execution plan for the optimized queries, which explains the shorter runtimes.

### 5. Performance Discussion

From the optimizations made, we can see that precomputing expensive queries in the Gold layer data drastically reduces the execution time. This therefore becomes a good pre computation strategy as it lowers the time needed to run queries that are often repeately run. Furtermore, we can see that AQE is a good practice to improve performance, however, pre compution is a clear better aproach when available as they can have larger speedups as seen in the results.

