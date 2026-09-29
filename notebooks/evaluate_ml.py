import time
from pathlib import Path
from typing import Union

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame, functions as F


def evaluate_model(
    model: Union[PipelineModel, str, Path],
    test_df: DataFrame,
) -> DataFrame:
    if isinstance(model, (str, Path)):
        print(f"Loading model from: {model}")
        pipeline_model = PipelineModel.load(str(model))
    else:
        pipeline_model = model

    # Filter out zero-distance anomalies before evaluation
    clean_test_df = test_df.filter(
        (F.col("fare_amount") > 0)
        & (F.col("trip_distance") > 0.05)
        & (~F.col("pickup_borough").isin("Unknown", "EWR"))
    )

    t0 = time.time()
    predictions = pipeline_model.transform(clean_test_df).cache()
    print(f"Inference completed in {time.time() - t0:.2f} seconds across {predictions.count():,} rows.\n")

    # Global regression metrics
    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print("==================================================")
    print("           MODEL EVALUATION SUMMARY               ")
    print("==================================================")
    print(f"Root Mean Squared Error (RMSE): ${rmse:.4f}")
    print(f"Mean Absolute Error (MAE):     ${mae:.4f}")
    print(f"Coefficient of Determination (R²): {r2:.4f}")
    print("==================================================\n")

    # Print spatial error breakdown
    residuals = predictions.withColumn("residual", F.col("label") - F.col("prediction")).withColumn(
        "abs_error", F.abs(F.col("residual"))
    )

    print("=== Error Breakdown by Pickup Borough ===")
    (
        residuals.groupBy("pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("abs_error"), 2).alias("mae"),
            F.round(F.sqrt(F.avg(F.pow("residual", 2))), 2).alias("rmse"),
        )
        .orderBy(F.desc("trip_count"))
        .show(truncate=False)
    )

    return residuals