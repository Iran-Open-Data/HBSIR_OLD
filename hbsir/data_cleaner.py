"""
Module for cleaning raw data into proper format
"""

from pathlib import Path
from typing import Iterable, Literal

from tqdm import tqdm
import pandas as pd
import numpy as np

from . import utils

from .metadata_reader import (
    defaults,
    metadata,
    original_tables,
    _OriginalTable,
    _Years,
)


def load_raw_data(
    table_name: _OriginalTable, year: int, urban: bool | None = None
) -> pd.DataFrame:
    """Reads CSV file(s) and returns DataFrame for table and year.

    Reads the CSV file(s) containing the specified table data for the
    given year and urban/rural category.

    Concatenates and returns the contents as a pandas DataFrame.

    Parameters
    ----------
    table_name : str
        Name of the table to read.

    year : int
        Year of the data to read.

    urban : bool, optional
        Whether to read only urban, rural or both data.
        If None, reads both urban and rural.

    Returns
    -------
    DataFrame
        Concatenated table data from the CSV file(s).

    Raises
    ------
    FileNotFoundError
        If CSV file(s) not found.

    ValueError
        If invalid table name, year, or corrupt metadata.

    Examples
    --------
    >>> df = load_raw_data('food', 1393)
    >>> df = load_raw_data('population', 1390, urban=True)

    """
    urban_stats = [True, False] if urban is None else [urban]
    tables = []
    for is_urban in urban_stats:
        file_path = _build_file_path(table_name, year, is_urban)
        tables.append(pd.read_csv(file_path, low_memory=False))
    table = pd.concat(tables, ignore_index=True)
    return table


def _build_file_path(table_name: str, year: int, is_urban: bool) -> Path:
    urban_rural = "U" if is_urban else "R"
    year_string = year % 100 if year < 1400 else year
    table_metadata = _get_table_metadata(table_name, year, is_urban)
    file_code = utils.MetadataVersionResolver(
        table_metadata["file_code"], year
    ).get_version()
    if file_code is None:
        raise ValueError(f"Table {table_name} is not available for year {year}")
    file_name = f"{urban_rural}{year_string}{file_code}.csv"
    file_path = defaults.extracted_data.joinpath(str(year), file_name)
    return file_path


# pylint: disable=unsubscriptable-object
# pylint: disable=unsupported-membership-test
def _get_table_metadata(
    table_name: str, year: int, is_urban: bool | None = None
) -> dict:
    table_metadata = metadata.tables[table_name]
    table_metadata = utils.MetadataVersionResolver(table_metadata, year).get_version()
    assert isinstance(table_metadata, dict)

    if is_urban is True:
        if "urban" in table_metadata:
            table_metadata = table_metadata["urban"]
    if is_urban is False:
        if "rural" in table_metadata:
            table_metadata = table_metadata["rural"]

    table_metadata["table_name"] = table_name
    table_metadata["year"] = year
    table_metadata["is_urban"] = is_urban

    return table_metadata


def open_and_clean_table(table_name: _OriginalTable, year: int) -> pd.DataFrame:
    """Cleans table data using metadata transformations.

    Loads raw table data, applies cleaning ops based on metadata,
    and concatenates urban and rural tables.

    Useful as a preprocessing step before further analysis.
    Called by save_processed_tables() to clean each table.

    Parameters
    ----------
    table_name : _OriginalTable
        Name of table to clean.

    year : int
        Year of data to clean.

    Returns
    -------
    DataFrame
        Cleaned concatenated table data.

    """
    cleaned_table_list = []
    for is_urban in [True, False]:
        table = load_raw_data(table_name, year, is_urban)
        table_metadata = _get_table_metadata(table_name, year, is_urban)
        cleaned_table = _apply_metadata_to_table(table, table_metadata)
        cleaned_table_list.append(cleaned_table)
    final_table = pd.concat(cleaned_table_list, ignore_index=True)
    return final_table


def _apply_metadata_to_table(table: pd.DataFrame, table_metadata: dict) -> pd.DataFrame:
    cleaned_table = pd.DataFrame()
    for column_name, column in table.items():
        assert isinstance(column_name, str)
        column_metadata = _get_column_metadata(table_metadata, column_name.upper())
        if column_metadata == "drop":
            continue
        if column_metadata == "error":
            raise ValueError(
                f"Error: The column '{column_name}' was not found in the metadata."
            )
        column = _apply_metadata_to_column(column, column_metadata)
        cleaned_table[column_metadata["new_name"]] = column
    return cleaned_table


def _get_column_metadata(
    table_metadata: dict, column_name: str
) -> dict | Literal["drop", "error"]:
    table_settings = _get_table_settings(table_metadata)
    year = table_metadata["year"]
    columns_metadata = table_metadata["columns"]
    columns_metadata = utils.MetadataVersionResolver(
        columns_metadata, year
    ).get_version()
    if not isinstance(columns_metadata, dict):
        raise ValueError(
            f"Unvalid metadata for column {column_name}: \n {columns_metadata}"
        )
    if column_name in columns_metadata:
        column_metadata = columns_metadata[column_name]
        if not (isinstance(column_metadata, dict) or column_metadata == "drop"):
            print(table_metadata)
            raise ValueError(f"Metadata for column {column_name} is not valid")
    else:
        column_metadata: Literal["drop", "error"] = table_settings["missings"]
        if column_metadata not in ["drop", "error"]:
            raise ValueError("Missing treatment is not valid")
    return column_metadata


def _get_table_settings(table_metadata: dict) -> dict:
    default_table_settings = metadata.tables["default_table_settings"]
    try:
        table_settings = table_metadata["settings"]
    except KeyError:
        return default_table_settings
    for key in default_table_settings:
        if key not in table_settings:
            table_settings[key] = default_table_settings[key]
    return table_settings


def _apply_metadata_to_column(column: pd.Series, column_metadata: dict) -> pd.Series:
    if ("replace" in column_metadata) and (column_metadata["replace"] is not None):
        column = column.replace(column_metadata["replace"])
    column = _apply_type_to_column(column, column_metadata)
    return column


def _apply_type_to_column(column: pd.Series, column_metadata: dict) -> pd.Series:
    column = _general_cleaning(column)
    new_column = pd.Series(np.nan, index=column.index)
    if ("type" not in column_metadata) or (column_metadata["type"] == "string"):
        new_column = column.copy()
    elif column_metadata["type"] == "boolean":
        new_column = column.astype("Int32") == column_metadata["true_condition"]
    elif column_metadata["type"] in ("unsigned", "integer", "float"):
        new_column = pd.to_numeric(column, downcast=column_metadata["type"])
    elif column_metadata["type"] == "category":
        new_column = column.astype("Int32").astype("category")
        new_column = new_column.cat.rename_categories(column_metadata["categories"])
    return new_column


def _general_cleaning(column: pd.Series):
    if pd.api.types.is_numeric_dtype(column):
        return column
    chars_to_remove = r"\n\r\,\@\+\*\[\]\_\?"
    try:
        column = column.str.replace(chr(183), ".").str.rstrip(".")
        column = column.str.replace(f"[{chars_to_remove}]+", "", regex=True)
        column = column.str.replace(r"\b\-", "", regex=True)
        column = column.replace(r"^[\s\.\-]*$", np.nan, regex=True)
    except AttributeError:
        pass
    return column


def save_cleaned_tables(
    table_names: _OriginalTable | Iterable[_OriginalTable] | None = None,
    years: _Years = "all",
) -> None:
    """Saves cleaned table data to Parquet files.

    Cleans, processes and saves the specified tables for the given years
    as Parquet files in the processed_data directory.

    Parameters
    ----------
    table_names : _OriginalTable or Iterable[_OriginalTable], optional
        Names of tables to process.
        Default is to process all tables.

    years : _Years, optional
        Years of data to process.
        Default is "all" years.

    """
    table_names = original_tables if table_names is None else table_names
    table_year = utils.construct_table_year_pairs(table_names, years)
    pbar = tqdm(total=len(table_year), desc="Preparing ...", unit="Table")
    for _table_name, year in table_year:
        pbar.update()
        pbar.desc = f"Table: {_table_name}, Year: {year}"
        table = open_and_clean_table(_table_name, year)
        Path(defaults.processed_data).mkdir(exist_ok=True)
        table.to_parquet(
            defaults.processed_data.joinpath(f"{year}_{_table_name}.parquet"),
            index=False,
        )
    pbar.close()
