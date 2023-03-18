"""
docstring
"""

import pandas as pd

from . import metadata

defaults = metadata.Defaults()

def _get_parquet(table_name: str, year: int) -> pd.DataFrame:
    table = pd.read_parquet(
        defaults.processed_data.joinpath(f"{year}_{table_name}.parquet")
    )
    return table
