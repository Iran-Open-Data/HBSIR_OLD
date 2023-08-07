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

from typing import Iterable, Literal

import pandas as pd

from . import metadata_reader, archive_handler, data_cleaner, data_engine
from .utils import parse_years

_OriginalTable = metadata_reader.OriginalTable


# pylint: disable=too-many-arguments
# pylint: disable=unused-argument
def load_table(
    table_name: str,
    years: int | Iterable[int] | str | None,
    data_type: Literal["processed", "cleaned", "original"] | None = None,
    on_missing: Literal["error", "download", "create"] | None = None,
    save_downloaded: bool | None = None,
    save_created: bool | None = None,
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
    metadata_reader.metadatas.reload_schema()
    optional_vars = {key: value for key, value in locals().items() if value is not None}
    settings = metadata_reader.LoadTable(**optional_vars)
    if settings.data_type == "original":
        years = parse_years(years)
        table_parts = []
        for year in years:
            table_parts.append(data_cleaner.read_table_csv(table_name, year))
        table = pd.concat(table_parts)
    elif settings.data_type == "cleaned":
        years = parse_years(years)
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
    metadata_reader.metadatas.reload_schema()
    optional_vars = {key: value for key, value in locals().items() if value is not None}
    settings = metadata_reader.LoadTable(**optional_vars)
    if "table_list" in schema:
        years = schema["years"]
        metadata_reader.metadatas.schema["_Input_Table"] = schema
    else:
        raise NameError
    return data_engine.TableLoader("_Input_Table", years, settings).load()


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


def setup(
    years: int | Iterable[int] | str | None = None,
    table_names: _OriginalTable | Iterable[_OriginalTable] | None = None,
    replace: bool = False,
) -> None:
    """Set up data by downloading and extracting archive files.
    
    This function handles downloading the archived data files, 
    extracting the CSV tables from the MS Access databases, and 
    saving them locally.
    
    It calls archive_handler.setup() to download and extract the
    archive files for the specified years.
    
    It also calls data_cleaner.save_cleaned_tables_as_parquet()
    to clean the specified tables and save them to Parquet format.
    
    Args:
        years (int|list|str|None): Year(s) to download and extract.
        table_names (list|None): Tables to clean and convert to Parquet.
        replace (bool): Whether to overwrite existing files.
        
    Examples:
        setup(1399)
        setup([1390,1400], ['food'], True)
    """
    archive_handler.setup(years, replace)
    data_cleaner.save_cleaned_tables_as_parquet(table_names, years)


__version__ = "0.1.0"
