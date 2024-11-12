# %%
%load_ext autoreload
%autoreload 2
# %%
%env DAGSTER_HOME=/Users/jganzabal/repos/recommender_system/dagster_home
# %%
from recommender_system.definitions import defs
from dagster import AssetKey
import pandas as pd
from tensorflow.keras import Model
# %%
with defs.get_asset_value_loader() as loader:
    model: Model = loader.load_asset_value(
        AssetKey("model_trained"),
    )
    model_metrics = loader.load_asset_value(
        AssetKey("model_metrics")
    )
# %%
model.summary()
# %%
model_metrics
# %%