"""
Main file for ordinary use
"""

from typing import Literal, get_args

import pandas as pd

from . import metadata, utils

defaults = metadata.Defaults()
metadata_obj = metadata.Metadata()

_Attributes = Literal["Urban-Rural", "Province", "Region"]


def _check_attribute(attribute: _Attributes | list[_Attributes]):
    available_attribute = get_args(_Attributes)
    if not isinstance(attribute, list):
        attribute = [attribute]
    for atr in attribute:
        if not atr in available_attribute:
            raise KeyError(
                f"{atr} is not in attributes.\n"
                f"Available attributes: {available_attribute}")


def load_table(
    table_name: str,
    from_year=None,
    to_year=None,
    standard=True,
    add_year: bool | None = None,
) -> pd.DataFrame:
    """_summary_

    Parameters
    ----------
    table_name : str
        _description_
    from_year : _type_, optional
        _description_, by default None
    to_year : _type_, optional
        _description_, by default None
    standard : bool, optional
        _description_, by default True

    Returns
    -------
    pd.DataFrame
        _description_
    """
    from_year, to_year = utils.build_year_interval(from_year, to_year)

    if add_year is None:
        add_year = to_year - from_year > 1

    table_list = []
    for year in range(from_year, to_year):
        table = _get_parquet(table_name, year)
        if add_year:
            table["Year"] = year
        table_list.append(table)
    concat_table = pd.concat(table_list)

    if standard:
        concat_table = imply_table_schema(concat_table, table_name, year)

    return concat_table


def _get_parquet(table_name: str, year: int, download: bool = True) -> pd.DataFrame:
    file_name = f"{year}_{table_name}.parquet"
    try:
        table = pd.read_parquet(defaults.processed_data.joinpath(file_name))
    except FileNotFoundError as exc:
        if download:
            _download_parquet(table_name, year)
            table = pd.read_parquet(
                defaults.processed_data.joinpath(file_name))
        else:
            raise exc
    return table


def _download_parquet(table_name: str, year: int) -> None:
    file_name = f"{year}_{table_name}.parquet"
    file_url = f"{defaults.online_dir}/parquet_files/{file_name}"
    local_path = defaults.processed_data.joinpath(file_name)
    utils.download_file(url=file_url, path=local_path, show_progress_bar=True)


def add_attribute(
    table: pd.DataFrame,
    year: int,
    attribute: _Attributes | list[_Attributes],
    id_column_name="ID",
    attribute_text="names",
) -> pd.DataFrame:
    """_summary_

    Parameters
    ----------
    table : pd.DataFrame
        _description_
    year : int
        _description_
    attribute : _Attributes | list[_Attributes]
        _description_
    id_column_name : str, optional
        _description_, by default "ID"

    Returns
    -------
    pd.DataFrame
        _description_
    """
    _check_attribute(attribute)
    if not isinstance(attribute, list):
        attribute_list = [attribute]
    else:
        attribute_list = attribute
    table = table.copy()

    for atr in attribute_list:
        attribute_column = get_household_attribute(
            _input=table,
            year=year,
            attribute=atr,
            id_column_name=id_column_name,
            attribute_text=attribute_text,
        )
        table[atr] = attribute_column
    return table


def get_household_attribute(
    _input: pd.DataFrame | pd.Series,
    year: int,
    attribute: _Attributes,
    id_column_name="ID",
    attribute_text="names",
) -> pd.Series:
    """_summary_

    Parameters
    ----------
    data : pd.DataFrame | pd.Series
        _description_
    year : int
        _description_
    attribute : _Attributes
        _description_

    Returns
    -------
    pd.Series
        _description_
    """
    _check_attribute(attribute)
    if isinstance(_input, pd.DataFrame):
        _input = _input[id_column_name].copy()
    if not isinstance(_input, pd.Series):
        raise ValueError
    return _get_attribute_by_id(_input, year, attribute, attribute_text)


def _get_attribute_by_id(
    household_id_column: pd.Series,
    year: int,
    attribute: _Attributes,
    attribute_text="names",
) -> pd.Series:
    attr_dict = metadata_obj.household[attribute]
    text = metadata.get_metadata_version(attr_dict[attribute_text], year)
    attr_codes = _get_attribute_code(household_id_column, year, attribute)
    return attr_codes.map(text)


def _get_attribute_code(
    household_id_column: pd.Series,
    year: int,
    attribute: _Attributes,
) -> pd.Series:
    id_length = metadata.get_metadata_version(
        metadata_obj.household["ID_Length"], year)
    attr_dict = metadata_obj.household[attribute]
    position = metadata.get_metadata_version(attr_dict["position"], year)
    start, end = position["start"], position["end"]
    attr_codes = household_id_column % pow(10, (id_length - start))
    attr_codes = attr_codes // pow(10, (id_length - end))
    return attr_codes
