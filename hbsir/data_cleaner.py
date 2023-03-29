"""
Module for cleaning raw data into proper format
"""

from pathlib import Path
from typing import Hashable

from tqdm import tqdm
import pandas as pd
import numpy as np

from . import metadata, utils


defaults = metadata.Defaults()
metadata_obj = metadata.Metadata()
_Tables = metadata.Tables


def load_table_data(
    table_name: str, year: int, urban: bool | None = None
) -> pd.DataFrame:
    """
    Reads CSV file(s) containing data for a given table, year, and urban/rural category,
    and returns the data as a pandas DataFrame.

    :param table_name: The name of the table to be read.
    :type table_name: str
    :param year: The year of the data to be read.
    :type year: int
    :param urban: A boolean indicating whether to read data for urban areas only (`True`),
        rural areas only (`False`), or both (`None`, default). If `None`, data for both
        urban and rural areas will be read.
    :type urban: bool|None
    :return: A DataFrame containing the concatenated data from the CSV file(s).
    :rtype: pd.DataFrame
    :raises FileNotFoundError: If the CSV file(s) cannot be found at the expected file path(s).
    :raises ValueError: If the table name or year are invalid, or if the table metadata file is
        corrupt.
    :raises pd.errors.EmptyDataError: If the CSV file(s) are empty.

    :example:

    >>> load_table_data("food", 1393, urban=True)
    """

    if urban is None:
        urban_stats = [True, False]
    else:
        urban_stats = [urban]

    tables = []
    for is_urban in urban_stats:
        file_path = _build_file_path(table_name, year, is_urban)
        tables.append(pd.read_csv(file_path, low_memory=False))
    table = pd.concat(tables, axis="index", ignore_index=True)
    return table


def _build_file_path(table_name: str, year: int, is_urban: bool) -> Path:
    urban_rural = "U" if is_urban else "R"
    year_string = year % 100 if year < 1400 else year
    table_metadata = _get_table_metadata(table_name, year, is_urban)
    file_code = metadata.get_metadata_version(table_metadata["file_code"], year)
    file_name = f"{urban_rural}{year_string}{file_code}.csv"
    file_path = defaults.extracted_data.joinpath(str(year), file_name)
    return file_path


def _get_table_metadata(
    table_name: str, year: int, is_urban: bool | None = None
) -> dict:
    table_metadata = metadata_obj.tables[table_name]
    table_metadata = metadata.get_metadata_version(table_metadata, year)

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


def clean_table_with_metadata(table_name: str, year: int) -> pd.DataFrame:
    """
    Clean the specified table using metadata and return a cleaned pandas DataFrame.

    :param table_name: The name of the table to be cleaned.
    :type table_name: str
    :param year: The year for which the table will be cleaned.
    :type year: int
    :return: A cleaned DataFrame with the specified table's data.
    :rtype: pandas.DataFrame

    """
    cleaned_table_list = []
    for is_urban in [True, False]:
        table = load_table_data(table_name, year, is_urban)
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
        column = _apply_metadata_to_column(column, column_metadata)
        cleaned_table[column_metadata["new_name"]] = column
    return cleaned_table


def _get_column_metadata(table_metadata: dict, column_name: Hashable) -> dict:
    table_settings = _get_table_settings(table_metadata)
    year = table_metadata["year"]
    columns_metadata = table_metadata["columns"]
    columns_metadata = metadata.get_metadata_version(columns_metadata, year)
    try:
        column_metadata = columns_metadata[column_name]
    except KeyError:
        column_metadata = table_settings["missings"]
    else:
        column_metadata = metadata.get_metadata_version(column_metadata, year)
    return column_metadata


def _get_table_settings(table_metadata: dict) -> dict:
    default_table_settings = metadata_obj.tables["default_table_settings"]
    try:
        table_settings = table_metadata["settings"]
    except KeyError:
        return default_table_settings
    for key in default_table_settings:
        if key not in table_settings:
            table_settings[key] = default_table_settings[key]
    return table_settings


def _apply_metadata_to_column(column: pd.Series, column_metadata: dict) -> pd.Series:
    column = _apply_type_to_column(column, column_metadata)
    return column


def _apply_type_to_column(column: pd.Series, column_metadata: dict) -> pd.Series:
    column = _convert_empty_items_to_nan(column)
    non_empty = column.notna()
    new_column = pd.Series(np.nan, index=column.index)
    if column_metadata["type"] == "boolian":
        new_column = column.astype("Int32").astype("category")
        new_column.loc[non_empty] = new_column == column_metadata["true_condition"]
    elif column_metadata["type"] in ("unsigned", "integer", "float"):
        new_column = pd.to_numeric(column, downcast=column_metadata["type"])
    elif column_metadata["type"] == "category":
        new_column = column.astype("Int32").astype("category")
        new_column = new_column.cat.rename_categories(column_metadata["categories"])
    elif column_metadata["type"] == "string":
        new_column = column.copy()
    return new_column


def _convert_empty_items_to_nan(column: pd.Series):
    if pd.api.types.is_numeric_dtype(column):
        return column
    chars_to_remove = r"\n\r\,\@\-\+"
    column = column.str.replace(f"[{chars_to_remove}]", "", regex=True)
    column = column.replace(r"\A\s*\Z", np.nan, regex=True)
    return column


def parquet_clean_data(
    table_name: _Tables | list[_Tables],
    from_year: int | None = None,
    to_year: int | None = None,
) -> None:
    """
    Clean and process data for a specified table and year range, and save it in
    Parquet format.

    :param table_name: Name of the table to be cleaned and processed.
    :type table_name: str

    :param from_year: Starting year of the data to be cleaned and processed
        (inclusive). If not specified, defaults to the earliest year available
        for the table.
    :type from_year: int or None

    :param to_year: Ending year of the data to be cleaned and processed
        (inclusive). If not specified, defaults to the last year.
    :type to_year: int or None

    :return: None

    :raises FileNotFoundError: If the function is unable to find the specified
        table(s) CSV file(s).

    :raises Exception: If an error occurs during the cleaning and processing of
        the data.

    .. note:: The cleaned and processed data will be saved in Parquet format to
        the `processed_data` directory in the project's default settings.

    .. seealso:: `utils.build_year_interval`, `clean_table_with_metadata`

    """
    table_year = utils.create_table_year_product(table_name, from_year, to_year)
    pbar = tqdm(total=len(table_year), desc="Preparing ...", unit="Table")
    for _table_name, year in table_year:
        pbar.update()
        pbar.desc = f"Table: {_table_name}, Year: {year}"
        table = clean_table_with_metadata(_table_name, year)
        Path(defaults.processed_data).mkdir(exist_ok=True)
        table.to_parquet(
            defaults.processed_data.joinpath(f"{year}_{_table_name}.parquet"),
            index=False,
        )
    pbar.close()
