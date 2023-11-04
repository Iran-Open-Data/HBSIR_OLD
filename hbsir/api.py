"""HBSIR - Iran Household Budget Survey Data API.

This module provides an API for loading, processing, and analyzing
Iran's Household Budget Survey (HBSIR) data.

The key functions provided are:

- load_table: Load HBSIR data for a given table name and year range.

- add_classification: Add commodity/occupation classification codes. 

- add_attribute: Add household attributes like urban/rural status.

- add_weight: Add sampling weights.  

- add_cpi: Join CPI index data.

- adjust_by_cpi: Adjust monetary values for inflation using CPI.

Sample usage:

    import hbsir
    
    df = hbsir.load_table('Expenditures', years=[1399, 1400])
    df = hbsir.add_classification(df, 'Durability')
    df = hbsir.adjust_by_cpi(df)

See the API docstrings for more details.

"""
# pylint: disable=too-many-arguments
# pylint: disable=unused-argument
# pylint: disable=too-many-locals

from typing import Iterable, Literal

import pandas as pd

from .core import archive_handler, data_cleaner, data_engine, decoder, metadata_reader

from . import external_data, utils

from .core.metadata_reader import (
    metadata,
    original_tables,
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
    *,
    on_missing: Literal["error", "download", "create"] | None = None,
    redownload: bool | None = None,
    save_downloaded: bool | None = None,
    recreate: bool | None = None,
    save_created: bool | None = None,
) -> pd.DataFrame:
    """Load a table for the given table name and year(s).

    This function loads original and standard tables.
    Original tables are survey tables and available in three types:
    original, cleaned and processed.

    - The 'original' dataset contains the raw data, identical to the
    survey data, without any modifications.
    - The 'cleaned' dataset contains the raw data with added column
    labels, data types, and removal of irrelevant values, but no
    changes to actual data values.
    - The 'processed' dataset applies operations like adding columns,
    calculating durations, and standardizing tables across years.

    Standard tables are defined in this package to facilitate
    working with the data and are only available in processed form.

    For more information about available tables check the
    [tables wiki page](https://github.com/Iran-Open-Data/HBSIR/wiki/Tables).

    Parameters
    ----------
    table_name : str
        Name of the table to load.
    years : _Years, default "last"
        Year or list of years to load data for.
    dataset : str, default "processed"
        Which dataset to load from - 'processed', 'cleaned', or 'original'.
    on_missing : str, default "download"
        Action if data is missing - 'error', 'download', or 'create'
    recreate : bool, default False
        Whether to recreate the data instead of loading it
    redownload : bool, default False
        Whether to re-download the data instead of loading it
    save_downloaded : bool, default True
        Whether to save downloaded data
    save_created : bool, default True
        Whether to save newly created data

    Returns
    -------
    DataFrame
        Loaded data for the specified table and years.

    Examples
    --------
    >>> import hbsir
    >>> df = hbsir.load_table('food')
    # Loads processed 'food' table from original survey tables for
        latest available year
    >>> df = hbsir.load_table('Expenditures', '1399-1401')
    # Loads standard 'Expenditures' table for years 1399 - 1401

    Raises
    ------
    FileNotFoundError
        If data is missing and on_missing='error'.

    """
    metadata.reload_file("schema")
    parameters = _extract_parameters(locals())
    settings = data_engine.LoadTableSettings(**parameters)
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
    *,
    on_missing: Literal["error", "download", "create"] | None = None,
    redownload: bool | None = None,
    save_downloaded: bool | None = None,
    recreate: bool | None = None,
    save_created: bool | None = None,
) -> pd.DataFrame:
    """Create and load table based on input schema.

    This function can be used in two ways:

    1) If a schema dictionary is passed, it will generate a DataFrame
    by loading or creating data that matches the schema.

    2) If a string table name is passed, it will load the table with
    that name from the external schema.

    Parameters
    ----------
    schema : dict or str
        Dictionary defining schema or string table name.
    years : str, optional
        Year(s) to load.
    on_missing : {'error', 'download', 'create'}, optional
        Action if data is missing.
    save_downloaded : bool, optional
        Whether to save downloaded data.
    save_created : bool, optional
       Whether to save newly created data.

    Returns
    -------
    DataFrame
        Loaded or created DataFrame.

    Examples
    --------
    >>> schema = {'table_list': ['food', 'tobacco'], 'years':'1398-1400'}
    >>> df = create_table_with_schema(schema)

    >>> table_name = 'User_Defined_Table'
    >>> df = create_table_with_schema(table_name)

    """
    metadata.reload_file("schema")
    parameters = _extract_parameters(locals())
    settings = data_engine.LoadTableSettings(**parameters)
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
    *,
    fields: Iterable[str] | None = None,
    levels: Iterable[int] | int | None = None,
    column_names: Iterable[str] | str | None = None,
    drop_value: bool | None = None,
    missing_value_replacements: dict[str, str] | None = None,
    code_col: str | None = None,
    year_col: str | None = None,
) -> pd.DataFrame:
    """Add classification to table.


    Parameters
    ----------
    table : DataFrame
        DataFrame containing 'Code' column to classify.
    name : str, optional
        Name of classification to apply.
    classification_type : {'commodity', 'occupation'}, optional
        Type of classification system. Inferred if not specified.
    labels : tuple of str, optional
        Names of classification levels.
    levels : tuple of int, optional
        Number of digits for each classification level.
    drop_value : bool, optional
        Whether to drop unclassified values.
    output_column_names : tuple of str, optional
        Names of output classification columns.
    missing_value_replacements : dict, optional
        Replacements for missing values in columns.
    code_column_name : str, optional
        Name of code column.
    year_column_name : str, optional
        Name of year column.
    versioned_info : dict, optional
        Versioning information for classifier.

    Returns
    -------
    DataFrame
        Input DataFrame with added classification columns.

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
    *,
    fields: Iterable[str] | str | None = None,
    column_names: Iterable[str] | str | None = None,
    id_col: str | None = None,
    year_col: str | None = None,
) -> pd.DataFrame:
    """Add household attributes to table based on ID.

    Takes a DataFrame containing a household ID, and adds columns for the
    specified household attribute such as urban/rural, province, or region.

    Supported attribute names are:

        - 'Urban_Rural': Urban or rural classification
        - 'Province': Province name
        - 'Region': Region name

    Parameters
    ----------
    table : DataFrame
        DataFrame containing 'ID' column.
    name : str
        Name of attribute to add.
    labels : tuple of str, optional
        Names of attribute labels.
    output_column_names : tuple of str, optional
        Names of output columns.
    id_column_name : str, optional
        Name of ID column.
    year_column_name : str, optional
        Name of year column.

    Returns
    -------
    DataFrame
        Input DataFrame with added attribute columns.

    """
    parameters = _extract_parameters(locals())
    settings = decoder.IDDecoderSettings(**parameters)
    table = decoder.IDDecoder(table=table, settings=settings).add_attribute()
    return table


def select(
    table: pd.DataFrame,
    *,
    urban_rural: Literal["Urban", "Rural"] | None = None,
    province: metadata_reader._Province | None = None,
    region: str | None = None,
) -> pd.DataFrame:
    """Selects subset of table based on criteria.

    Filters the input table based on provided selection criteria.
    Decodes and adds attributes if needed to perform filtering.
    Removes decoded columns before returning output table.

    Parameters
    ----------
    table : pd.DataFrame
        Input DataFrame to filter.

    urban_rural : Literal["Urban", "Rural"], optional
        Keep only Urban or Rural households.

    province : Province, optional
        Keep only given province.

    region : str, optional
        Keep only given region.

    Returns
    -------
    pd.DataFrame
        Subset of input table meeting criteria.

    """
    if urban_rural is not None:
        table = (
            table.pipe(add_attribute, "Urban_Rural")
            .query(f"Urban_Rural == '{urban_rural}'")
            .drop(columns="Urban_Rural")
        )
    if region is not None:
        table = (
            table.pipe(add_attribute, "Region")
            .query(f"Region == '{region}'")
            .drop(columns="Region")
        )
    elif province is not None:
        table = (
            table.pipe(add_attribute, "Province")
            .query(f"Province == '{province}'")
            .drop(columns="Province")
        )
    return table


def add_weight(
    table: pd.DataFrame, adjust_for_household_size: bool = False
) -> pd.DataFrame:
    """Add sampling weights to the table.

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


def add_cpi(
    table: pd.DataFrame,
    *,
    data_source: Literal["SCI"] = "SCI",
    base_year: Literal[1400] = 1400,
    frequency: Literal["Annual"] = "Annual",
    separate_by: Literal["Urban_Rural"] = "Urban_Rural",
) -> pd.DataFrame:
    """Add CPI values to the table

    Parameters
    ----------
    table : pd.DataFrame
        Input DataFrame to add CPI values to.
    data_source : str, default "SCI"
        Source of CPI data, either "SCI" or "CBS".
    base_year : int, default 1400
        Base year for CPI.
    frequency : str, default "Annual"
        Frequency of CPI data, either "Annual" or "Monthly".
    separate_by : str, default "Urban_Rural"
        Column to separate CPI by, either "Urban_Rural" or None.

    Returns
    -------
    DataFrame
        Input `table` with 'CPI' column added.

    """
    cpi = external_data.load_table(
        f"CPI_{base_year}",
        data_source=data_source,
        frequency=frequency,
        separate_by=separate_by,
        reset_index=False,
    )

    if separate_by == "Urban_Rural":
        urban_rural_available = True
        if "Urban_Rural" not in table.columns:
            table = add_attribute(table, "Urban_Rural")
            urban_rural_available = False

        table = table.join(cpi, on=["Urban_Rural", "Year"])

        if not urban_rural_available:
            table = table.drop(columns="Urban_Rural")
    else:
        table = table.join(cpi, on=["Year"])

    return table


def adjust_by_cpi(
    table: pd.DataFrame, columns: list[str] | None = None, **kwargs
) -> pd.DataFrame:
    """Adjust columns in the table by the CPI

    Divides the specified columns of the DataFrame by the CPI column.
    If no columns specified, divides common monetary value columns
    like 'Expenditure', 'Income' etc.

    Parameters
    ----------
    table : DataFrame
        DataFrame containing a "CPI" column.
    columns : List[str], optional
        List of columns names to divide by CPI.
    **kwargs
        Additional keyword arguments passed to add_cpi.

    Returns
    -------
    DataFrame
        Input `table` with `columns` divided by 'CPI'.

    """
    default_columns = [
        "Expenditure",
        "Net_Expenditure",
        "Gross_Expenditure",
        "Price",
        "Income",
        "Net_Income",
        "Gross_Income",
    ]
    if columns is None:
        columns = [column for column in default_columns if column in table.columns]

    cpi_available = True
    if "CPI" not in table.columns:
        table = add_cpi(table, **kwargs)
        cpi_available = False
    table.loc[:, columns] = (
        table.loc[:, columns]
        .divide(table["CPI"], axis="index")
        .multiply(100, axis="index")
    )
    if not cpi_available:
        table = table.drop(columns="CPI")

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
