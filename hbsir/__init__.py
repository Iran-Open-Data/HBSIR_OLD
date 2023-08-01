"""init file"""

from typing import Iterable

import pandas as pd

from .data_engine import TableLoader

def load_table(
    table_names: str | list[str],
    years: int | Iterable[int] | str | None,
    apply_schema: bool = True,
) -> pd.DataFrame:
    loader = TableLoader(
        table_names=table_names,
        years=years,
        apply_schema=apply_schema,
    )
    return loader.load()

__version__ = "0.1.0"
