from __future__ import annotations

import pandas as pd
from sklearn.model_selection import train_test_split

try:
    from mage_ai.data_preparation.decorators import transformer
    from mage_ai.data_preparation.decorators import test
except ImportError:
    def transformer(func):
        return func

    def test(func):
        return func


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    train_df, test_df = train_test_split(
        df,
        test_size=0.2,
        random_state=42,
    )
    train_df = train_df.copy()
    test_df = test_df.copy()
    train_df["split"] = "train"
    test_df["split"] = "test"
    return pd.concat([train_df, test_df])


@test
def test_output(output, *args) -> None:
    assert len(output) == 150, "Iris must have 150 rows"
    assert set(output["split"]) == {"train", "test"}
    assert (output["split"] == "train").sum() == 120
    assert (output["split"] == "test").sum() == 30
