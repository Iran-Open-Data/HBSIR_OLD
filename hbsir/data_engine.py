"""
Main file for ordinary use
"""

import re
from collections import defaultdict
from typing import Literal, get_args

import pandas as pd

from . import metadata, utils

defaults = metadata.defaults
metadatas = metadata.metadatas
_Attributes = metadata.Attributes
_OriginalTable = metadata.OriginalTable
_StandardTables = metadata.StandardTable
_Table = metadata.Tables


def load_table(
    table_name: _Table,
    from_year: int | None = None,
    to_year: int | None = None,
    **kwargs,
) -> pd.DataFrame:
    """docs"""
    if table_name in metadata.original_tables:
        table_name_list = [table_name]
    else:
        table_name_list: list[_Table] = metadatas.schema[table_name]["table_list"]

    original_kwargs = kwargs.copy()
    for variable in ["add_year", "add_duration", "add_table_names"]:
        if variable in kwargs:
            continue
        try:
            kwargs[variable] = metadatas.schema[table_name]["settings"][variable]
        except KeyError:
            kwargs[variable] = True

    sub_tables = []
    for _table_name in table_name_list:
        if _table_name in metadata.original_tables:
            table = read_table(_table_name, from_year, to_year, **kwargs)
        else:
            table = load_table(_table_name, from_year, to_year, **kwargs)

        sub_tables.append(table)
    table = pd.concat(sub_tables, ignore_index=True)

    if "classifications" in metadatas.schema[table_name]:
        for classification in metadatas.schema[table_name]["classifications"]:
            table = add_classification(table, **classification)

    if (
        ("add_duration" not in original_kwargs)
        and ("Duration" in table.columns)
        and (len(table["Duration"].unique()) < 2)
    ):
        table = table.drop(columns="Duration")

    table_schema = metadatas.schema[table_name]
    table = _imply_table_schema(table, table_schema, from_year)
    return table


def read_table(
    table_name: _OriginalTable | list[_OriginalTable] | tuple[_OriginalTable],
    from_year: int | None = None,
    to_year: int | None = None,
    original: bool = False,
    add_year: bool = False,
    add_duration: bool = False,
    add_table_names: bool = False,
    **kwargs,
) -> pd.DataFrame:
    """
    Load Tables
    """
    tname_year = utils.create_table_year_product(
        table_name=table_name, from_year=from_year, to_year=to_year
    )
    table_list: list[pd.DataFrame] = []
    for _table_name, year in tname_year:
        table = _get_parquet(_table_name, year, **kwargs)
        if not original:
            try:
                table_schema = metadatas.schema[_table_name]["yearly_schema"]
            except KeyError:
                pass
            else:
                table = _imply_table_schema(table, table_schema, year)
        if add_year:
            table["Year"] = year
        if add_duration:
            table = _add_duration(table, _table_name)
        if add_table_names:
            table["table"] = _table_name
        table_list.append(table)
    concat_table = pd.concat(table_list, ignore_index=True)

    if not add_year:
        concat_table.attrs["year"] = from_year

    return concat_table


def _get_parquet(
    table_name: str, year: int, download: bool = True, save: bool = True
) -> pd.DataFrame:
    file_name = f"{year}_{table_name}.parquet"
    try:
        table = pd.read_parquet(defaults.processed_data.joinpath(file_name))
    except FileNotFoundError as exc:
        print(
            f"Table {table_name} for year {year} not found at expected location: \n"
            f"{defaults.processed_data.joinpath(file_name)}"
        )
        if download and save:
            _download_parquet(table_name, year)
            table = pd.read_parquet(defaults.processed_data.joinpath(file_name))
        elif download:
            table = pd.read_parquet(
                f"{defaults.online_dir}/parquet_files/{year}_{table_name}.parquet"
            )
        else:
            raise exc
    return table


def _download_parquet(table_name: str, year: int) -> None:
    file_name = f"{year}_{table_name}.parquet"
    file_url = f"{defaults.online_dir}/parquet_files/{file_name}"
    local_path = defaults.processed_data.joinpath(file_name)
    utils.download_file(url=file_url, path=local_path, show_progress_bar=True)


def _imply_table_schema(table: pd.DataFrame, table_schema: dict, year: int | None = None):
    """docs"""
    table = table.copy()

    if "columns" in table_schema:
        instructions = table_schema["columns"]

        for name, instruction in instructions.items():
            if isinstance(year, int):
                instruction = metadata.get_metadata_version(instruction, year)
            table = _apply_column_instruction(table, name, instruction)

    if "filter" in table_schema:
        if isinstance(table_schema["filter"], str):
            filt = table.eval(table_schema["filter"])
        elif isinstance(table_schema["filter"], list):
            filts = []
            for filt_str in table_schema["filter"]:
                filts.append(table.eval(filt_str))
            filt = pd.concat(filts, axis="columns").sum(axis="columns") == len(table_schema["filter"])
        else:
            raise KeyError
        table = table.loc[filt]

    if "order" in table_schema:
        column_order = table_schema["order"]
        table = _order_columns_by_schema(table, column_order)
    return table


def _apply_column_instruction(table, name, instruction):
    if instruction is None:
        pass
    elif instruction["type"] == "categorical":
        table[name] = _apply_categorical_instruction(table, name, instruction)
    elif instruction["type"] == "numerical":
        table[name] = _apply_numerical_instruction(table, instruction)

    return table


def _apply_categorical_instruction(table: pd.DataFrame, column_name: str, instruction: dict):
    categories = instruction["categories"]

    if column_name in table.columns:
        categorical_column = table[column_name].copy()

    categorical_column = pd.Series(index=table.index, dtype="category")

    for category, condition in categories.items():
        if condition is None:
            filt = table.index
        elif isinstance(condition, str):
            filt = table[column_name] == condition
        elif isinstance(condition, list):
            filt = table[column_name].isin(condition)
        elif isinstance(condition, dict):
            filts = []
            for other_column, value in condition.items():
                if isinstance(value, (bool, str)):
                    filts.append(table[other_column] == value)
                elif isinstance(value, list):
                    filts.append(table[other_column].isin(value))
                else:
                    raise KeyError
            filt = pd.concat(filts, axis="columns").sum(axis="columns") == len(condition)
        else:
            raise KeyError
        categorical_column = categorical_column.cat.add_categories([category])
        categorical_column.loc[filt] = category

    return categorical_column


def _apply_numerical_instruction(table: pd.DataFrame, instruction: dict) -> pd.Series:
    columns_names = re.split(r"[\+\-\*\/\s\.]+", instruction["expression"])
    columns_names = [name for name in columns_names if not name.isnumeric()]
    columns = table[columns_names].astype(float).copy()
    expr = instruction["expression"]
    result = columns.fillna(0).eval(expr)
    return result


def _order_columns_by_schema(table, column_order):
    new_columns = [column for column in column_order if column in table.columns]
    table = table[new_columns]
    return table


def _add_duration(table, table_name):
    table = table.copy()
    if table_name in metadata.expenditure_tables:
        default_duration = metadatas.commodities["tables"][table_name][
            "default_duration"
        ]
    else:
        default_duration = 360
    table["Duration"] = default_duration
    return table


def add_attribute(
    table: pd.DataFrame,
    attribute: _Attributes | list[_Attributes] | tuple[_Attributes] | None,
    year: int | None = None,
    id_column_name="ID",
    year_column_name: str = "Year",
    attribute_text="names",
) -> pd.DataFrame:
    """docs"""
    if attribute is None:
        attribute_list = [attr for attr in get_args(_Attributes)]
    elif isinstance(attribute, (list, tuple)):
        attribute_list = [attr for attr in attribute]
    else:
        attribute_list: list[_Attributes] = [attribute]

    table = table.copy()

    for _attribute in attribute_list:
        attribute_column = get_attribute(
            _input=table,
            year=year,
            attribute=_attribute,
            id_column_name=id_column_name,
            year_column_name=year_column_name,
            attribute_text=attribute_text,
        )
        table[_attribute] = attribute_column
    return table


def get_attribute(
    _input: pd.DataFrame | pd.Series | pd.Index,
    attribute: _Attributes,
    year: int | None = None,
    id_column_name="ID",
    year_column_name: str = "Year",
    attribute_text="names",
) -> pd.Series:
    """docs"""
    if isinstance(_input, (pd.Series, pd.Index)):
        if year is None:
            raise TypeError(
                "Since the input is a Pandas series, the 'year' variable must "
                "be specified. Please provide a year value in the format YYYY."
            )
        return _get_attribute_by_id(_input, year, attribute, attribute_text)
    if not isinstance(_input, pd.DataFrame):
        raise ValueError

    _input = _input.copy()
    years: list[int] = []
    if year is not None:
        years = [year]
        _input["__Year__"] = year
    elif year_column_name in _input.columns:
        years = [int(_year) for _year in _input[year_column_name].unique()]
        _input["__Year__"] = _input[year_column_name]
    elif "year" in _input.attrs:
        year = _input.attrs["year"]
        assert isinstance(year, int)
        years = [year]
        _input["__Year__"] = year
    else:
        raise TypeError(
            "DataFrame does not have a 'year' column. Please provide the "
            "'year' column or specify a value for the 'year' variable."
        )

    attribute_column = pd.Series(None, dtype="object", index=_input.index)
    for _year in years:
        filt = _input["__Year__"] == _year
        id_series = _input.loc[filt, id_column_name]
        attribute_series = _get_attribute_by_id(
            household_id_column=id_series,
            attribute=attribute,
            attribute_text=attribute_text,
            year=_year,
        )
        attribute_column.loc[filt] = attribute_series

    attribute_column = attribute_column.astype("category")
    return attribute_column


def _get_attribute_by_id(
    household_id_column: pd.Series | pd.Index,
    year: int,
    attribute: _Attributes,
    attribute_text="names",
) -> pd.Series:
    attr_dict = metadatas.household[attribute]
    text = metadata.get_metadata_version(attr_dict[attribute_text], year)
    attr_codes = _get_attribute_code(household_id_column, year, attribute)
    attr_codes = attr_codes.map(text)
    attr_codes = attr_codes.astype("category")
    return attr_codes


def _get_attribute_code(
    household_id_column: pd.Series | pd.Index,
    year: int,
    attribute: _Attributes,
) -> pd.Series:
    id_length = metadata.get_metadata_version(metadatas.household["ID_Length"], year)
    attr_dict = metadatas.household[attribute]
    position = metadata.get_metadata_version(attr_dict["position"], year)
    start, end = position["start"], position["end"]
    attr_codes = household_id_column % pow(10, (id_length - start))
    attr_codes = attr_codes // pow(10, (id_length - end))
    return attr_codes


def add_classification(
    table: pd.DataFrame,
    classification: str = "original",
    level: int | list[int] | None = None,
    year: int | None = None,
    code_column_name: str = "Code",
    year_column_name: str = "Year",
    new_column_name: str | list[str] | None = None,
    attribute: str | None = None,
    dropna: bool = False,
) -> pd.DataFrame:
    """docs"""
    table = table.copy()

    if level is None:
        levels = metadatas.commodities[classification]["default_levels"]
    elif isinstance(level, int):
        levels = [level]
    elif isinstance(level, list):
        levels = level
    else:
        raise TypeError

    if new_column_name is None:
        if "default_names" in metadatas.commodities[classification]:
            column_names = metadatas.commodities[classification]["default_names"]
        else:
            column_names = [f"{classification}-{_level}" for _level in levels]
    elif isinstance(new_column_name, str):
        column_names = [new_column_name]
    else:
        column_names = new_column_name

    assert len(levels) == len(column_names)

    level_and_name = zip(levels, column_names)

    for _level, column_name in level_and_name:
        classification_column = get_classification(
            table,
            classification=classification,
            level=_level,
            year=year,
            code_column_name=code_column_name,
            year_column_name=year_column_name,
            attribute=attribute,
        )
        table[column_name] = classification_column

    if dropna:
        table = table.dropna(subset=column_names)

    return table


def get_classification(
    _input: pd.DataFrame | pd.Series,
    classification: str,
    level: int,
    year: int | None = None,
    code_column_name: str = "Code",
    year_column_name: str = "Year",
    attribute: str | None = None,
) -> pd.Series:
    """docs"""
    if isinstance(_input, pd.Series):
        if year is None:
            raise TypeError(
                "Since the input is a Pandas series, the 'year' variable must "
                "be specified. Please provide a year value in the format YYYY."
            )
        return _get_classification_by_code(
            _input, classification, level, year, attribute
        )
    if not isinstance(_input, pd.DataFrame):
        raise ValueError

    _input = _input.copy()
    if year is not None:
        years = [year]
        _input["__Year__"] = year
    elif year_column_name in _input.columns:
        years = [int(y) for y in _input[year_column_name].unique()]
        _input["__Year__"] = _input[year_column_name]
    elif "year" in _input.attrs:
        year = _input.attrs["year"]
        if year is not None:
            years = [year]
        else:
            raise KeyError
        _input["__Year__"] = year
    else:
        raise TypeError(
            "DataFrame does not have a 'year' column. Please provide the "
            "'year' column or specify a value for the 'year' variable."
        )

    classification_column = pd.Series(None, dtype="object", index=_input.index)
    for _year in years:
        filt = _input["__Year__"] == _year
        code_series = _input.loc[filt, code_column_name]
        classification_series = _get_classification_by_code(
            commodity_code_column=code_series,
            classification=classification,
            level=level,
            year=_year,
            attribute=attribute,
        )
        classification_column.loc[filt] = classification_series

    classification_column = classification_column.astype("category")
    return classification_column


def _get_classification_by_code(
    commodity_code_column: pd.Series,
    classification: str,
    level: int,
    year: int,
    attribute: str | None = None,
) -> pd.Series:
    translator = _build_translator(
        classification=classification, level=level, year=year, attribute=attribute
    )
    classification_column = commodity_code_column.map(translator)
    classification_column = classification_column.astype("category")
    return classification_column


def _build_translator(
    classification: str,
    level: int,
    year: int,
    attribute: str | None = None,
    default_value: str | None = None,
) -> dict:
    def closure(_input):
        def inner_function():
            return _input

        return inner_function

    commodity_codes = metadatas.commodities[classification]["items"]
    commodity_codes = metadata.get_metadata_version(commodity_codes, year)
    selected_items = {
        name: info for name, info in commodity_codes.items() if info["level"] == level
    }
    translator = {}
    if attribute is None:
        for name, info in selected_items.items():
            categories = metadata.get_categories(info)
            for category_info in categories:
                if "default" in category_info:
                    translator = defaultdict(closure(name), translator)
                    break
                code_range = _get_code_range(category_info["code"])
                for code in code_range:
                    translator[code] = name
    else:
        for info in selected_items.values():
            categories = metadata.get_categories(info)
            for category_info in categories:
                try:
                    attribute_value = category_info[attribute]
                except KeyError:
                    attribute_value = default_value
                if "default" in category_info:
                    translator = defaultdict(closure(attribute_value), translator)
                    break
                code_range = _get_code_range(category_info["code"])
                for code in code_range:
                    translator[code] = attribute_value

    return translator


def _get_code_range(code_range_info: int | dict | list) -> list[int]:
    if isinstance(code_range_info, int):
        code_range = [code_range_info]
    elif isinstance(code_range_info, dict):
        if ("start" in code_range_info) and ("end" in code_range_info):
            code_range = list(range(code_range_info["start"], code_range_info["end"]))
        elif "code" in code_range_info:
            code_range = _get_code_range(code_range_info["code"])
        else:
            raise KeyError
    elif isinstance(code_range_info, list):
        code_range = []
        for element in code_range_info:
            code_range.extend(_get_code_range(element))
    else:
        raise KeyError

    return code_range


def add_weights(
    table: pd.DataFrame,
    year: int | None = None,
    **kwargs
) -> pd.DataFrame:
    pass


def get_weights(
    _input: int | pd.DataFrame,
    method: Literal["default", "external", "household_info"] = "default",
) -> pd.Series:
    if isinstance(_input , int):
        if (method == "household_info") or (method == "default" and _input > 1395):
            weights = _get_weights_by_household_info(_input)
        else:
            weights = _get_weights_by_external_data(_input)
    elif isinstance(_input, pd.DataFrame):
        pass
    else:
        raise ValueError
    return weights


def _get_weights_by_household_info(year: int) -> pd.Series:
    hh_info = read_table("household_information", year)
    hh_info = hh_info.set_index("ID")
    weights = hh_info["Weight"]
    return weights

def _get_weights_by_external_data(year: int) -> pd.Series:
    pass
