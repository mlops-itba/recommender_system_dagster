# %%
%env MLFLOW_TRACKING_URI=http://localhost:5001
# %%
# https://grouplens.org/datasets/movielens/
# %%

# %%
import mlflow.artifacts
import pandas as pd
import datetime
# %%
ratings = pd.read_csv('../ml-32m/ratings.csv')
# %%
ratings['date'] = ratings['timestamp'].apply(lambda x: str(datetime.datetime.fromtimestamp(x).date()))
# %%
ratings['year_month'] = ratings['date'].apply(lambda x: x[:7])
# %%
df_year_month = ratings['year_month'].value_counts().reset_index().sort_values('year_month', ascending=False)
# %%
df_year_month.plot.bar('year_month','count')
# %%
df_year_month
# %%
ratings_last_years = ratings[ratings['date'] > '2022-01']
# %%
# %%
import mlflow
mlflow.set_experiment('partitioned_data')
mlflow.start_run()
# %%
for year_month, df in ratings_last_years.groupby('year_month'):
    filename = f'../data/ratings_{year_month}.csv'
    df[['userId','movieId','rating','timestamp']].to_csv(filename, index=False)
    mlflow.log_artifact(filename, 'ratings')
# %%
mlflow.end_run()
# %%
from mlflow.client import MlflowClient
import tempfile

client = MlflowClient()
run_id = '5a9e96afcd53439894d00fb8c097ebb9'
partition_key = '2022-04-01T00:00:00-0300'
date_str = partition_key[:7]
with tempfile.TemporaryDirectory() as local_dir:
    local_path = client.download_artifacts(run_id, f"ratings/ratings_{date_str}.csv", local_dir)
    df = pd.read_csv(local_path)
df