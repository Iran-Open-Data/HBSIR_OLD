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
# pylint: disable=too-many-arguments
# pylint: disable=unused-argument
# pylint: disable=too-many-locals

from typing import Iterable, Literal

import pandas as pd

from .core import archive_handler, data_cleaner, data_engine, decoder, metadata_reader

from . import (
    utils,
)
from .core.metadata_reader import (
    metadata,
    original_tables,
    LoadTableSettings,
    _Attribute,
    _OriginalTable,
    _Table,
    _Years,
)


def _extract_parameters(local_variables: dict) -> dict:
    return {key: value for key, value in local_variables.items() if value is not None}


def load_table(
    table_name: _Table,
    years: _Years = "last",
    dataset: Literal["processed", "cleaned", "original"] | None = None,
    on_missing: Literal["error", "download", "create"] | None = None,
    redownload: bool | None = None,
    save_downloaded: bool | None = None,
    recreate: bool | None = None,
    save_created: bool | None = None,
) -> pd.DataFrame:
    """Load a DataFrame for the given table name and year(s).

    This function loads data for the specified table from the
    specified dataset ('processed', 'cleaned', or 'original').
    It can handle loading data for a single year or multiple years.

    Parameters
    ----------
    table_name : str
        Name of the table to load.
    years : _Years, default "last"
        Year or list of years to load data for.
    dataset : str, default "processed"
        Which dataset to load from - 'processed', 'cleaned',
        or 'original'.
    on_missing : str, default "download"
        Action if data is missing - 'error', 'download', or 'create'
    recreate : bool, default False
        Whether to recreate the data instead of loading it
    redownload : bool, default False
        Whether to re-download the data instead of loading it
    save_downloaded : bool, default False
        Whether to save downloaded data
    save_created : bool, default False
        Whether to save newly created data

    Returns
    -------
    DataFrame
        Loaded data for the specified table and years.

    Examples
    --------
    >>> df = load_table('food')
    >>> df = load_table('Expenditures', '1399-1401')

    Raises
    ------
    FileNotFoundError
        If data is missing and on_missing='error'.

    """
    metadata.reload_file("schema")
    parameters = _extract_parameters(locals())
    settings = LoadTableSettings(**parameters)
    if settings.dataset == "original":
        if table_name not in original_tables:
            raise ValueError
        years = utils.parse_years(years)
        table_parts = []
        for year in years:
            table_parts.append(data_cleaner.load_raw_data(table_name, year))
        table = pd.concat(table_parts)
    elif settings.dataset == "cleaned":
        if table_name not in original_tables:
            raise ValueError
        years = utils.parse_years(years)
        table_parts = []
        for year in years:
            table_parts.append(
                data_engine.TableHandler([table_name], year, settings)[table_name]
            )
        table = pd.concat(table_parts)
    else:
        table = data_engine.create_table(
            table_name=table_name,
            years=years,
            settings=settings,
        )
    return table


def create_table_with_schema(
    schema: str | dict,
    years: _Years = "last",
    on_missing: Literal["error", "download", "create"] | None = None,
    redownload: bool | None = None,
    save_downloaded: bool | None = None,
    recreate: bool | None = None,
    save_created: bool | None = None,
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
    settings = LoadTableSettings(**parameters)
    if isinstance(schema, str):
        return data_engine.create_table(schema, years, settings)

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
    return data_engine.create_table(table_name, years, settings)


def add_classification(
    table: pd.DataFrame,
    name: str = "original",
    classification_type: Literal["commodity", "occupation"] | None = None,
    labels: tuple[str, ...] | None = None,
    levels: tuple[int, ...] | None = None,
    drop_value: bool | None = None,
    output_column_names: tuple[str, ...] | None = None,
    required_columns: tuple[str, ...] | None = None,
    missing_value_replacements: dict[str, str] | None = None,
    code_column_name: str | None = None,
    year_column_name: str | None = None,
    versioned_info: dict | None = None,
    defaults: dict | None = None,
) -> pd.DataFrame:
    """Add commodity classification codes to DataFrame.

    Takes a DataFrame with a 'Code' column and classifies the codes
    based on the specified classification system.

    Args:
        table: DataFrame containing 'Code' column to classify.
        name: Name of classification to apply.
        **kwargs: Additional arguments passed to internal classifier.

    Returns:
        pd.DataFrame: Input DataFrame with added classification columns.

    """
    parameters = _extract_parameters(locals())
    if "classification_type" not in parameters:
        if "code_column_name" in parameters:
            if table[parameters["code_column_name"]].le(10_000).mean() < 0.9:
                class_type = "occupation"
            else:
                class_type = "commodity"
        elif metadata_reader.defaults.columns.commodity_code in table.columns:
            class_type = "commodity"
        elif metadata_reader.defaults.columns.job_code in table.columns:
            class_type = "occupation"
        else:
            raise ValueError("Missing Code Column")
        parameters["classification_type"] = class_type
    settings = decoder.DecoderSettings(**parameters)
    table = decoder.Decoder(table=table, settings=settings).add_classification()
    return table


def add_attribute(
    table: pd.DataFrame,
    name: _Attribute,
    labels: tuple[str, ...] | None = None,
    output_column_names: tuple[str, ...] | None = None,
    id_column_name: str | None = None,
    year_column_name: str | None = None,
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


def add_weight(
    table: pd.DataFrame, adjust_for_household_size: bool = False
) -> pd.DataFrame:
    """Add sample weights to a table of data.

    Loads appropriate sample weights for each year in the table and merges
    them onto the table. Sample weights can optionally be adjusted
    for household size.

    Weights for years prior to 1395 are loaded from external parquet data,
    while weights for 1395 onward come from the household_information table.

    Parameters
    ----------
    table : pd.DataFrame
        Input data table, containing a column of year values
    adjust_for_household_size : bool, default False
        Whether to adjust weights by household size
    year_column_name : str, default "Year"
        Name of column in `table` that contains the year

    Returns
    -------
    table : pd.DataFrame
        Input `table` with 'Weight' column added

    """
    table = data_engine.add_weights(table, adjust_for_household_size)
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
    data_cleaner.save_cleaned_tables(table_names, years)
