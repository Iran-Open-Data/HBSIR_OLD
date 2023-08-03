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


def create_table_with_schema(schema) -> pd.DataFrame:
    return data_engine.TableLoader(schema=schema).load()


def add_classification(
    table: pd.DataFrame, classification_name: str, **kwargs
) -> pd.DataFrame:
    table = data_engine.add_classification(
        table=table, classification_name=classification_name, **kwargs
    )
    return table


def add_attribute(
    table: pd.DataFrame, attribute_name: data_engine._Attribute, **kwargs
) -> pd.DataFrame:
    table = data_engine.add_attribute(
        table=table, attribute_name=attribute_name, **kwargs
    )
    return table


def add_weight(table: pd.DataFrame) -> pd.DataFrame:
    table = data_engine.add_weights(table)
    return table


__version__ = "0.1.0"
