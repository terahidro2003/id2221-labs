## Benchmark Report

### 1. Objective

Evaluate the scalability of two storage strategies for the Taxi Trips dataset by comparing ingestion time, storage size, query latency, and the number of generated files.

### 2. Storage Strategies

#### Strategy A: `By Timestamp`

- **Partitioning:** `pickup_date`
- **Reasoning:** `Could be better for time based queries such as average duration per day.`

#### Strategy B: `By location`

- **Partitioning:** `pickup_borough`
- **Reasoning:** `Could be better for location based queries such as trip data per borough location.`

### 3. Benchmark Results

#### 3.1 Ingestion and Storage

| Metric | Timestamp | Location |
|---|---:|---:|
| Ingestion time (s) | `26` | `35` |
| Storage size (MB) | `228` | `227` |
| Generated files | `222` | `200` |
| Partitions | `92` | `8` |

#### 3.2 Query Latency

| Query | Timestamp (s) | Location (s) |
|---|---:|---:|
| Number of taxi trips per borough | `2.4` | `2.9` |
| Average trip duration per day | `9.3` | `2.1` |
| Average fare per borough | `1.5` | `0.8` |


### 4. Discussion

- **Ingestion:** `From the results gathered, we can see that it took roughly 10s longer when partitioning by borough. This could potentially be due to the fact that here the data is more clearly skewed, which lead to an uneven division between the partitions and may contribute to a longer write time. This is seen with the fact that Manhattan has over 8 million trips, whereas Staten Island only has 220.`
- **Storage:** `The storage size is roughly the same here, and thus not a lot could be concluded from it.`
- **Query latency:** `For the trips per borough, the difference in time was small and thus unclear, but timestamp partitioning was slightly faster. This was unexpected as location was expected to benefit when counting trips per borough. This may be due to the skew discussed earlier. For average trip duration, the query groups by pickup_date. However, as the dataset is relatively small as seen above, this would indicate that the 92 partitions could be too many for spark and thus even though the data is skewed, the overhead might be bigger. This is similar for the last benchmark where we also group by pickup_borough, thus better suitable for location.`
- **Scalability:** `We could see similar phenomena when increasing the size of the data or continuing past 2024. Thus, the skew between the partitions for borough could exponentially grow, leading to worse performance. Furthermore, an increase in write time could be seen for time-based, as we would have an increased amount of partitions and thus increase the problems seen above. Here it could then be better to change how we do the partitioning for time, by moving from date to month, for example.`

