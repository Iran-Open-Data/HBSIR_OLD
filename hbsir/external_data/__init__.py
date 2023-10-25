from typing import Literal, Any

import pandas as pd

from .external_data_cleaner import ExternalDataCleaner

__all__ = ["load_table"]


_DataSource = Literal["SCI", "CBI"]
_Frequency = Literal["Annual", "Monthly"]
_SeparateBy = Literal["Urban_Rural", "Province"]

_Default = Any


def load_table(
    table_name: str,
    data_source: _DataSource = _Default,
    frequency: _Frequency = _Default,
    separate_by: _SeparateBy = _Default,
    reset_index: bool = True,
    download_cleaned: bool = True,
    saved_cleaned: bool = False,
) -> pd.DataFrame:
    name_parts = [data_source, table_name, frequency, separate_by]
    name_parts = [part for part in name_parts if part != _Default]
    name = ".".join(name_parts).lower()
    table = ExternalDataCleaner(name, download_cleaned, saved_cleaned).load_data()
    if reset_index:
        table = table.reset_index()
    return table
