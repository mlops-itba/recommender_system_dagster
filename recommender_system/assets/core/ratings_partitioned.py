from dagster import asset, TimeWindowPartitionsDefinition, logger, Output, MetadataValue
from dateutil.parser import parse
import pandas as pd
from mlflow.client import MlflowClient
import tempfile

START_SCHEDULE_TIME = '2022-01-01T10:00:00'
CRON_SCHEDULE = "0 0 1 * *"



time_partition = TimeWindowPartitionsDefinition(
    cron_schedule=CRON_SCHEDULE,
    start=parse(START_SCHEDULE_TIME),
    timezone='America/Argentina/Buenos_Aires',
    fmt='%Y-%m-%dT%H:%M:%S%z'
)

@asset(
    partitions_def=time_partition,
)
def partitined_ratings(context):
    context.log.info(f"{context.partition_key}")
    client = MlflowClient()
    run_id = '5a9e96afcd53439894d00fb8c097ebb9'
    partition_key = context.partition_key
    date_str = partition_key[:7]
    if date_str[:4] not in ['2022', '2023']:
        return pd.DataFrame([])
    context.log.info(f"{date_str}")
    with tempfile.TemporaryDirectory() as local_dir:
        local_path = client.download_artifacts(run_id, f"ratings/ratings_{date_str}.csv", local_dir)
        df = pd.read_csv(local_path)
    metrics = {
        "Total rows": len(df),
        "scores_mean": float(df['rating'].mean()),
        "scores_std": float(df['rating'].std()),
        "unique_movies": len(df['movieId'].unique()),
        "unique_users": len(df['userId'].unique()),
        "preview": MetadataValue.md(df.head().to_markdown())
    }
    return Output(
        df,
        metadata=metrics
    )