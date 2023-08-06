"""init file"""

from typing import Iterable, Literal

import pandas as pd

from . import data_engine, metadata, data_cleaner, utils


# pylint: disable=unused-argument
def load_table(
    table_name: str,
    years: int | Iterable[int] | str | None,
    data_type: Literal["processed", "cleaned", "original"] | None = None,
    on_missing: Literal["error", "download", "create"] | None = None,
    save_downloaded: bool | None = None,
    save_created: bool | None = None,
) -> pd.DataFrame:
    metadata.metadatas.reload_schema()
    optional_vars = {key: value for key, value in locals().items() if value is not None}
    settings = metadata.LoadTable(**optional_vars)
    if settings.data_type == "original":
        years = utils.parse_years(years)
        table_parts = []
        for year in years:
            table_parts.append(data_cleaner.read_table_csv(table_name, year))
        table = pd.concat(table_parts)
    elif settings.data_type == "cleaned":
        years = utils.parse_years(years)
        table_parts = []
        for year in years:
            table_parts.append(
                data_engine.TableHandler(table_name, year, settings).read()
            )
        table = pd.concat(table_parts)
    else:
        loader = data_engine.TableLoader(
            table_name=table_name,
            years=years,
            settings=settings,
        )
        table = loader.load()
    return table


# pylint: disable=unused-argument
def create_table_with_schema(
    schema: dict,
    data_type: Literal["processed", "cleaned", "original"] | None = None,
    on_missing: Literal["error", "download", "create"] | None = None,
    save_downloaded: bool | None = None,
    save_created: bool | None = None,
) -> pd.DataFrame:
    metadata.metadatas.reload_schema()
    optional_vars = {key: value for key, value in locals().items() if value is not None}
    settings = metadata.LoadTable(**optional_vars)
    if "table_list" in schema:
        years = schema["years"]
        metadata.metadatas.schema["_Input_Table"] = schema
    else:
        raise NameError
    return data_engine.TableLoader("_Input_Table", years, settings).load()


def add_classification(
    table: pd.DataFrame, classification_name: str = "original", **kwargs
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
