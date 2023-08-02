"""init file"""

from typing import Iterable

import pandas as pd

from . import data_engine


def load_table(
    table_names: str | list[str],
    years: int | Iterable[int] | str | None,
    apply_schema: bool = True,
) -> pd.DataFrame:
    loader = data_engine.TableLoader(
        table_names=table_names,
        years=years,
        apply_schema=apply_schema,
    )
    return loader.load()


def add_classification(
    table: pd.DataFrame, classification_name: str, **kwargs
) -> pd.DataFrame:
    table = data_engine.add_classification(
        table=table, classification_name=classification_name, **kwargs
    )
    return table

__version__ = "0.1.0"
