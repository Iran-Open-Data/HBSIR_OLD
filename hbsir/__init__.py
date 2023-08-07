"""init file"""

from typing import Iterable

import pandas as pd

from . import data_engine


def load_table(
    table_names: str | list[str],
    years: int | Iterable[int] | str | None,
    apply_schema: bool = True,
) -> pd.DataFrame:
    """Load DataFrame for given table name and year range.

    Loads data for specified table, handles different data types,
    and provides options for configuring behavior when data is missing.

    Args:
        table_name: Name of table to load.
        years: Year or years to load data for.
        data_type: What data type to load - 'original', 'cleaned' or 'processed'.
        on_missing: Action if data is missing - 'error', 'download', or 'create'.
        save_downloaded: Whether to save downloaded data.
        save_created: Whether to save newly created data.

    Returns:
        pd.DataFrame: Loaded data for the specified table and year range.

    Raises:
        FileNotFoundError: If data is missing and on_missing='error'.

    Examples:
        >>> load_table('food', 1400)
        >>> load_table('Expenditures', [1399, 1400])

    """
    loader = data_engine.TableLoader(
        table_names=table_names,
        years=years,
        apply_schema=apply_schema,
    )
    return loader.load()


def create_table_with_schema(schema) -> pd.DataFrame:
    """Create and load DataFrame based on input schema.

    Generates a DataFrame by loading or creating data that conforms to
    the provided schema. Provides options to configure behavior.

    Args:
        schema: Dictionary defining schema for output DataFrame.
        data_type: What data type to load or create - 'original',
            'cleaned' or 'processed'.
        on_missing: Action if data is missing - 'error', 'download', or 'create'.
        save_downloaded: Whether to save downloaded data.
        save_created: Whether to save newly created data.

    Returns:
        pd.DataFrame: Loaded or created DataFrame matching schema.

    Examples:
        >>> schema = {'table_list': ['food', 'tobacco'], years='1398-1400'}
        >>> df = create_table_with_schema(schema)

    """
    return data_engine.TableLoader(schema=schema).load()


def add_classification(
    table: pd.DataFrame, classification_name: str = "original", **kwargs
) -> pd.DataFrame:
    """Add commodity classification codes to DataFrame.

    Takes a DataFrame with a 'Code' column and classifies the codes
    based on the specified classification system.

    Args:
        table: DataFrame containing 'Code' column to classify.
        classification_name: Name of classification to apply.
        **kwargs: Additional arguments passed to internal classifier.

    Returns:
        pd.DataFrame: Input DataFrame with added classification columns.

    """
    table = data_engine.add_classification(
        table=table, classification_name=classification_name, **kwargs
    )
    return table


def add_attribute(
    table: pd.DataFrame, attribute_name: data_engine._Attribute, **kwargs
) -> pd.DataFrame:
    """Add household attributes to DataFrame based on ID.

    Takes a DataFrame containing a 'ID' column, and adds columns for the
    specified household attribute such as urban/rural, province, or region.

    The attribute is joined based on matching the 'ID' column.

    Supported attribute names are:

    - 'Urban_Rural': Urban or rural classification
    - 'Province': Province name
    - 'Region': Region name

    Args:
        table: DataFrame containing 'ID' column.
        attribute_name: Name of attribute to add.
        **kwargs: Additional arguments passed to internal adder.

    Returns:
        pd.DataFrame: Input DataFrame with added attribute columns.

    """
    table = data_engine.add_attribute(
        table=table, attribute_name=attribute_name, **kwargs
    )
    return table


def add_weight(table: pd.DataFrame) -> pd.DataFrame:
    """Add sampling weights to DataFrame based on household ID and year.

    Takes a DataFrame containing 'ID' and 'Year' columns, joins the
    appropriate sampling weight for each household based on year, and
    adds a 'Weight' column.

    Weights for years prior to 1395 are loaded from external parquet data,
    while weights for 1395 onward come from the household_information table.

    Args:
        table: DataFrame containing 'ID' and 'Year' columns.

    Returns:
        pd.DataFrame: Input DataFrame with added 'Weight' column.

    """
    table = data_engine.add_weights(table)
    return table


__version__ = "0.1.0"
