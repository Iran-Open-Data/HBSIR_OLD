from typing import Literal

import pandas as pd

from .external_data_cleaner import ExternalDataCleaner

__all__ = ["load_table"]


_DataSource = Literal["SCI", "CBI"]
_Frequency = Literal["Annual", "Monthly"]
_SeparateBy = Literal["Urban_Rural", "Province"]


def load_table(
    table_name: str,
    data_source: _DataSource = "SCI",
    frequency: _Frequency = "Annual",
    separate_by: _SeparateBy | None = None,
    download_cleaned: bool = False,
    saved_cleaned: bool = True,
) -> pd.DataFrame:
    name = ".".join([data_source, table_name, frequency])
    name = name if separate_by is None else f"{name}.{separate_by}"
    name = name.lower()
    table = ExternalDataCleaner(name, download_cleaned).load_data(saved_cleaned)
    table = table.reset_index()
    return table
