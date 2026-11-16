from __future__ import annotations

import pandas as pd

if "transformer" not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        def transformer(func):
            return func
if "test" not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func):
            return func


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    result = df.copy()
    feature_columns = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    if not set(feature_columns).issubset(result.columns):
        feature_columns = [
            column
            for column in result.columns
            if column not in {"target", "target_name", "split"}
        ]
    result.attrs["checked_feature_columns"] = feature_columns
    return result


@test
def test_output(output, *args) -> None:
    assert output.isna().sum().sum() == 0
    assert output["target"].nunique() == 3
