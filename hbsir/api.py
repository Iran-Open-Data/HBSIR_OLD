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

from typing import Iterable, Literal, Any

import pandas as pd

from . import (
    archive_handler,
    data_cleaner,
    data_engine,
    decoder,
    utils,
)
from .metadata_reader import (
    metadata,
    LoadTable,
    _Attribute,
    _OriginalTable,
    _Table,
    _Years,
)

_Default = Any


def _extract_parameters(local_variables: dict) -> dict:
    return {
        key: value for key, value in local_variables.items() if value is not _Default
    }


# pylint: disable=too-many-arguments
# pylint: disable=unused-argument
def load_table(
    table_name: _Table,
    years: _Years = "last",
    dataset: Literal["processed", "cleaned", "original"] = _Default,
    on_missing: Literal["error", "download", "create"] = _Default,
    redownload: bool = _Default,
    save_downloaded: bool = _Default,
    recreate: bool = _Default,
    save_created: bool = _Default,
) -> pd.DataFrame:
    """Load DataFrame for given table name and year range.

    Loads data for specified table, handles different data types,
    and provides options for configuring behavior when data is missing.

    Args:
        table_name: Name of table to load.
        years: Year or years to load data for.
        dataset: What data type to load - 'original', 'cleaned' or 'processed'.
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
    metadata.reload_file("schema")
    parameters = _extract_parameters(locals())
    settings = LoadTable(**parameters)
    if settings.dataset == "original":
        years = utils.parse_years(years)
        table_parts = []
        for year in years:
            table_parts.append(data_cleaner.read_table_csv(table_name, year))
        table = pd.concat(table_parts)
    elif settings.dataset == "cleaned":
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
    years: _Years = "last",
    dataset: Literal["processed", "cleaned", "original"] = _Default,
    on_missing: Literal["error", "download", "create"] = _Default,
    redownload: bool = _Default,
    save_downloaded: bool = _Default,
    recreate: bool = _Default,
    save_created: bool = _Default,
) -> pd.DataFrame:
    """Create and load DataFrame based on input schema.

    Generates a DataFrame by loading or creating data that conforms to
    the provided schema. Provides options to configure behavior.

    Args:
        schema: Dictionary defining schema for output DataFrame.
        dataset: What data type to load or create - 'original',
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
    metadata.reload_file("schema")
    parameters = _extract_parameters(locals())
    settings = LoadTable(**parameters)
    if years is None and "years" in schema:
        years = schema["years"]
    if "table_list" in schema:
        table_name = "_Input_Table"
        metadata.schema[table_name] = schema
    elif all("table_list" in table_schema for table_schema in schema.values()):
        table_name = list(schema.keys())[-1]
        metadata.schema.update(schema)
    else:
        raise NameError
    return data_engine.TableLoader(table_name, years, settings).load()


def add_classification(
    table: pd.DataFrame,
    name: str = "original",
    defaults: dict = _Default,
    labels: tuple[str, ...] = _Default,
    levels: tuple[int, ...] = _Default,
    drop_value: bool = _Default,
    output_column_names: tuple[str, ...] = _Default,
    required_columns: tuple[str, ...] = _Default,
    missing_value_replacements: dict[str, str] = _Default,
    code_column_name: str = _Default,
    year_column_name: str = _Default,
    versioned_info: dict = _Default,
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
    parameters = _extract_parameters(locals())
    settings = decoder.CommodityDecoderSettings(**parameters)
    table = decoder.CommodityDecoder(
        table=table, settings=settings
    ).add_classification()
    return table


def add_attribute(
    table: pd.DataFrame,
    name: _Attribute,
    labels: tuple[str, ...] = _Default,
    output_column_names: tuple[str, ...] = _Default,
    id_column_name: str = _Default,
    year_column_name: str = _Default,
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
    parameters = _extract_parameters(locals())
    settings = decoder.IDDecoderSettings(**parameters)
    table = decoder.IDDecoder(table=table, settings=settings).add_attribute()
    return table


def add_weight(table: pd.DataFrame, how="left") -> pd.DataFrame:
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
    table = data_engine.WeightAdder(table).add_weights(how=how)
    return table


def setup(
    years: _Years = "last",
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
