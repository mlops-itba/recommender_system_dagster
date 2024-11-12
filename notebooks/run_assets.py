# %%
%load_ext autoreload
%autoreload 2
# %%
%env DAGSTER_HOME=/Users/jganzabal/repos/recommender_system/dagster_home
%env MLFLOW_TRACKING_URI=http://localhost:5001
# %%
from recommender_system.definitions import defs
from dagster import AssetKey, build_asset_context, resource, materialize_to_memory
import mlflow
import pandas as pd
from dagster_mlflow import mlflow_tracking
from tensorflow.keras import Model
# %% 
# Bajo assets de entrada al asset que quiero evaluar
with defs.get_asset_value_loader() as loader:
    movie2Idx: Model = loader.load_asset_value(
        AssetKey("movie2Idx"),
    )
    user2Idx: Model = loader.load_asset_value(
        AssetKey("user2Idx"),
    )
    X_train: Model = loader.load_asset_value(
        AssetKey("X_train"),
    )
    y_train: Model = loader.load_asset_value(
        AssetKey("y_train"),
    )
# %%
from recommender_system.configs import job_training_config
import mock
# %%
# materialize_to_memory()
# %%
# %%
job_training_config['resources']
# %%
job_training_config['ops']['model_trained']
# %%
@resource
def mlflow_tracking(context):
    config = context.resource_config
    experiment_name = config.get("experiment_name")

    if not experiment_name:
        raise ValueError("Experiment name is required.")

    
    import mlflow
    mlflow.set_experiment(experiment_name)

    return mlflow
# %%
job_training_config['ops']['model_trained']['config']
# %%
from recommender_system.assets.recommender.train_model import model_trained
context = build_asset_context(
    resources={'mlflow': mock.MagicMock()},
    resources_config=job_training_config['resources'],
    asset_config=job_training_config['ops']['model_trained']['config'],
)
# %%
model_trained(context, X_train, y_train, user2Idx, movie2Idx)
# %%
model_trained.