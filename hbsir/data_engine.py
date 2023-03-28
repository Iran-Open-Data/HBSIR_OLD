"""
Main file for ordinary use
"""

from typing import get_args

import sympy
import pandas as pd

from . import metadata, utils

defaults = metadata.Defaults()
metadata_obj = metadata.Metadata()
_Attributes = metadata.Attributes
_Tables = metadata.Tables


def _check_attribute(attribute: _Attributes | list[_Attributes]) -> None:
    available_attributes = get_args(_Attributes)
    if not isinstance(attribute, (list, tuple)):
        attribute = [attribute]
    for atr in attribute:
        if not atr in available_attributes:
            available_attributes_str = ", ".join(str(x) for x in available_attributes)
            raise KeyError(
                f"Invalid attribute: {atr}. This attribute is not supported."
                f"Available attributes: {available_attributes_str}"
            )


def load_table(
    table_name: _Tables | list[_Tables] | tuple[_Tables],
    from_year: int | None = None,
    to_year: int | None = None,
    standard: bool | None = None,
    add_year: bool | None = None,
) -> pd.DataFrame:
    """docs
    """
    year_name = utils.create_table_year_product(
        table_name=table_name, from_year=from_year, to_year=to_year
    )

    if add_year is None:
        add_year = utils.is_multi_year(table_name, from_year, to_year)

    if standard is None:
        if isinstance(table_name, str):
            standard = table_name in metadata_obj.schema
        else:
            standard = False

    table_list = []
    for name, year in year_name:
        table = _get_parquet(name, year)
        if add_year:
            table["Year"] = year
        if standard:
            table = imply_table_schema(table, name, year)
        table_list.append(table)
    concat_table = pd.concat(table_list)

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


def imply_table_schema(table, table_name, year):
    """docs
    """
    table = table.copy()

    table_schema = metadata_obj.schema[table_name]

    if "columns" in table_schema:
        instructions = table_schema["columns"]

        for name, instruction in instructions.items():
            instruction = metadata.get_metadata_version(instruction, year)
            table = _apply_column_instruction(table, name, instruction)

    if "order" in table_schema:
        column_order = table_schema["order"]
        table = _order_columns_by_schema(table, column_order)
    return table


def _apply_column_instruction(table, name, instruction):
    if instruction is None:
        pass
    elif instruction["type"] == "categorical":
        table = _apply_categorical_instruction(table, name, instruction)
    elif instruction["type"] == "numerical":
        table = _apply_numerical_instruction(table, name, instruction)

    return table


def _apply_categorical_instruction(table, column_name, instruction):
    categories = instruction["categories"]

    if column_name not in table.columns:
        table[column_name] = None

    # chack column type and behave accordingly not just changing the type
    table[column_name] = table[column_name].astype(str)

    for category, condition in categories.items():
        if isinstance(condition, str):
            filt = table[column_name] == condition
        elif isinstance(condition, list):
            filt = table[column_name].isin(condition)
        elif isinstance(condition, dict):
            filt = pd.Series(False, index=table.index)
            for other_column, value in condition.items():
                filt = filt | (table[other_column] == value)
        else:
            raise KeyError
        table.loc[filt, column_name] = category

    table[column_name] = table[column_name].astype("category")
    return table


def _apply_numerical_instruction(table, column_name, instruction):
    expr = instruction["expression"]
    pandas_expr = _parse_expression(expr)
    table[column_name] = pd.eval(pandas_expr)
    return table


def _parse_expression(expression, table_name="table"):
    expr = sympy.simplify(expression)
    terms = []

    if len(expr.args) == 0:
        _, var = expr.as_coeff_Mul()
        return f"{table_name}['{var}']"

    if (len(expr.args) == 2) and (len(expr.args[1].args) == 0):
        coeff, var = expr.as_coeff_Mul()
        return f"{table_name}['{var}'] * {coeff}"

    for term in expr.args:
        coeff, var = term.as_coeff_Mul()
        terms.append(f"{table_name}['{var}'].fillna(0) * {coeff}")
    return " + ".join(terms)


def _order_columns_by_schema(table, column_order):
    new_columns = [column for column in column_order if column in table.columns]
    return table[new_columns]


def add_attribute(
    table: pd.DataFrame,
    attribute: _Attributes | list[_Attributes] | tuple[_Attributes] | None,
    year: int | None = None,
    id_column_name="ID",
    year_column_name: str = "Year",
    attribute_text="names",
) -> pd.DataFrame:
    """docs
    """
    if attribute is None:
        attribute_list = [attr for attr in get_args(_Attributes)]
    elif isinstance(attribute, (list, tuple)):
        attribute_list = [attr for attr in attribute]
    else:
        attribute_list: list[_Attributes] = [attribute]

    table = table.copy()

    for _attribute in attribute_list:
        attribute_column = get_household_attribute(
            _input=table,
            year=year,
            attribute=_attribute,
            id_column_name=id_column_name,
            year_column_name=year_column_name,
            attribute_text=attribute_text,
        )
        table[_attribute] = attribute_column
    return table


def get_household_attribute(
    _input: pd.DataFrame | pd.Series,
    attribute: _Attributes,
    year: int | None = None,
    id_column_name="ID",
    year_column_name: str = "Year",
    attribute_text="names",
) -> pd.Series:
    """docs
    """
    _check_attribute(attribute)
    if isinstance(_input, pd.Series):
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
    household_id_column: pd.Series,
    year: int,
    attribute: _Attributes,
    attribute_text="names",
) -> pd.Series:
    attr_dict = metadata_obj.household[attribute]
    text = metadata.get_metadata_version(attr_dict[attribute_text], year)
    attr_codes = _get_attribute_code(household_id_column, year, attribute)
    attr_codes = attr_codes.map(text)
    attr_codes = attr_codes.astype("category")
    return attr_codes


def _get_attribute_code(
    household_id_column: pd.Series,
    year: int,
    attribute: _Attributes,
) -> pd.Series:
    id_length = metadata.get_metadata_version(metadata_obj.household["ID_Length"], year)
    attr_dict = metadata_obj.household[attribute]
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
) -> pd.DataFrame:
    """docs
    """
    table = table.copy()

    if level is None:
        levels = metadata_obj.commodities[classification]["default_levels"]
    elif isinstance(level, int):
        levels = [level]
    elif isinstance(level, list):
        levels = level
    else:
        raise TypeError

    if new_column_name is None:
        column_names = [f"{classification}-{_level}" for _level in levels]
    elif isinstance(new_column_name, str):
        column_names = [new_column_name]
    else:
        column_names = new_column_name

    assert len(levels) == len(column_names)

    level_and_name = zip(levels, column_names)

    for _level, column_name in level_and_name:
        classification_column = get_code_classification(
            table,
            classification=classification,
            level=_level,
            year=year,
            code_column_name=code_column_name,
            year_column_name=year_column_name,
        )
        table[column_name] = classification_column

    return table


def get_code_classification(
    _input: pd.DataFrame | pd.Series,
    classification: str,
    level: int,
    year: int | None = None,
    code_column_name: str = "Code",
    year_column_name: str = "Year",
) -> pd.Series:
    """docs
    """
    if isinstance(_input, pd.Series):
        if year is None:
            raise TypeError(
                "Since the input is a Pandas series, the 'year' variable must "
                "be specified. Please provide a year value in the format YYYY."
            )
        return _get_classification_by_code(_input, classification, level, year)
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
        )
        classification_column.loc[filt] = classification_series

    classification_column = classification_column.astype("category")
    return classification_column


def _get_classification_by_code(
    commodity_code_column: pd.Series,
    classification: str,
    level: int,
    year: int,
) -> pd.Series:
    translator = _build_translator(
        classification=classification, level=level, year=year
    )
    classification_column = commodity_code_column.map(translator)
    classification_column = classification_column.astype("category")
    return classification_column


def _build_translator(
    classification: str,
    level: int,
    year: int,
    attribute: str = "name",
    default_value: str | None = None,
) -> dict:
    commodity_codes = metadata_obj.commodities[classification]["items"]
    commodity_codes = metadata.get_metadata_version(commodity_codes, year)
    selected_items = {
        name: info for name, info in commodity_codes.items() if info["level"] == level
    }
    translator = {}
    if attribute == "name":
        for name, info in selected_items.items():
            categories = metadata.get_categories(info)
            for category_info in categories:
                code_range = _get_code_range(category_info["code"])
                for code in code_range:
                    translator[code] = name
    else:
        for info in selected_items.values():
            categories = metadata.get_categories(info)
            for category_info in categories:
                code_range = _get_code_range(category_info["code"])
                for code in code_range:
                    try:
                        attribute_value = category_info[attribute]
                    except KeyError:
                        attribute_value = default_value
                    translator[code] = attribute_value

    return translator


def _get_code_range(code_range_info: int | dict | list) -> list[int]:
    if isinstance(code_range_info, int):
        code_range = [code_range_info]
    elif isinstance(code_range_info, dict):
        code_range = list(range(code_range_info["start"], code_range_info["end"]))
    elif isinstance(code_range_info, list):
        code_range = []
        for element in code_range_info:
            code_range.extend(_get_code_range(element))
    else:
        raise KeyError

    return code_range
