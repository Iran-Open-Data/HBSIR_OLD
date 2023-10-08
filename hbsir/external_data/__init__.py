import pandas as pd

from .external_data_cleaner import ExternalDataCleaner

__all__ = [
    "load_table"
]

def load_table(
    table_name: str, data_source: str = "", frequency: str = ""
) -> pd.DataFrame:
    name = ".".join([data_source, table_name, frequency])
    return ExternalDataCleaner(name).load_data()
