"""Main API for Household Budget Survey of Iran (HBSIR) data.

This module exposes the main functions for loading, processing, and  
analyzing the household survey data.

The key functions are:

- load_table: Load data for a given table name and year range
- add_classification: Add COICOP commodity classification codes  
- add_attribute: Add household attributes like urban/rural
- add_weight: Add sampling weights

The load_table function handles fetching data from different sources
like original CSVs or preprocessed Parquet files. It provides options 
for configuring behavior when data is missing.

Sample usage:

    import hbsir
    
    df = hbsir.load_table('food', [1399, 1400])
    df = hbsir.add_classification(df, 'original')
    df = hbsir.add_attribute(df, 'Urban_Rural')
    
See API documentation for more details.
"""


from . import (
    calculator,
    external_data,
    hbsframe,
    utils,
)
from .api import (
    load_table,
    create_table_with_schema,
    add_classification,
    add_attribute,
    add_weight,
    setup,
)

__version__ = "0.2.5"

__all__ = [
    "add_attribute",
    "add_classification",
    "add_weight",
    "calculator",
    "create_table_with_schema",
    "external_data",
    "hbsframe",
    "load_table",
    "setup",
    "utils",
]
