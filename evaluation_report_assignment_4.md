# Evaluation Report

## 1. Feature Engineering Analysis
This feature pipeline is reused by both approaches. Approach A builds the integrated dataset from raw files, while Approach B starts from the existing Delta table. Both approaches use the same feature pipeline, including trip, weather, air-quality, location, and time-based features. This makes the comparison fair because differences in preprocessing and training performance are mainly caused by the data source and preparation method.


## 2. Model Evaluation Results

| Approach | RMSE | MAE | Training time | Preprocessing time |
|---|---:|---:|---:|---:|
| A: Raw files | $5.64 | $2.63 | 48 s | 24 s |
| B: Integrated Delta | $5.53 | $2.65 | 49 s | 0.18 s |

## 3. Comparison of Approaches

The engineering decisions from earlier assignments such as schema standardization, data cleaning, and integration of multiple datasets simplified the machine learning workflow. The taxi trip dataset provided the most useful features in this case , while weather and air quality datasets were more contributing to the overall data context. The ingestion, validation, integration, and the feature engineering componeents from the workflow became reusable for machine learning tasks. We could see from the results that the integrated platform significantly reduced the time needed for preprocessing the data before feature engineering. We did also see that the training time were similar for both approaches, which indicates that the model training was not affected by the data source. If we added additional datasets, they could be validated, integrated, and included as additional features. The improvements to the platform could be adding automated data quality checks, stronger schema validation and better handling of missing values.

## 4. Reproducibility Analysis

Both of the approaches use fixed date based splits and the same random seed for model training. They also use the same feature engineering pipeline as mentioned earlier, which makes the comparison fair. Approach B is more reproducible due to the fact that it uses an existing integrated Delta table, while approach A depends on the raw files and their integration process. Reproducibilty could be improved by recording software versions, input data versions, delta table versions, model parameters and potential feature pipeline changes.

## 5. Role of Data Engineering for supporting ML 
From this assignment and these results, we can conclude that the role of the data engineer for supporting machine learning is to provide dependable and reusable data pipelines that can be used for multiple machine learning tasks. This becomes especially evident when looking at the time difference for preprocessing the data before feature engineering, thus making sure that an ML engineer may spend more time running the ML application rather than on data preparation. The data engineer is also responsible for ensuring that the data is clean, validated, and integrated from multiple sources, which simplifies the machine learning workflow and allows data engineer to focus on model development and evaluation.