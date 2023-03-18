import pandas as pd
import numpy as np

from . import utils
from .metadata import Defaults, Metadata


def categoricla_columns(df, column_name, column_property):
    def create_column(df, column_name, column_property):
        for category_name, condition in column_property["categories"].items():
            filt = df.query(condition).index
            df.loc[filt, column_name] = category_name
        df[column_name] = df[column_name].astype("category")
        return df

    def edit_column(df, column_name, column_property):
        for old_name, new_name in column_property["categories"].items():
            if new_name not in df[column_name].cat.categories:
                df[column_name] = df[column_name].cat.add_categories([new_name])
            filt = df[column_name] == old_name
            df.loc[filt, column_name] = new_name
            df[column_name] = df[column_name].cat.remove_categories([old_name])
        return df

    if column_name not in df.columns:
        df = create_column(df, column_name, column_property)
    elif df[column_name].dtype == "category":
        df = edit_column(df, column_name, column_property)
    else:
        df = create_column(df, column_name, column_property)
    return df


def numerical_columns(df, column_name, column_property):
    # TODO clean this mess
    elements = column_property["calculation"].split(" ")
    if len(elements) == 2:
        df[column_name] = df[elements[1]] * float(elements[0])
    elif len(elements) == 5:
        if elements[2] == "+":
            df[column_name] = df[elements[1]].fillna(0) * float(elements[0]) + df[
                elements[4]
            ].fillna(0) * int(elements[3])
            filt = df[elements[1]].isna() & df[elements[4]].isna()
            df.loc[filt, column_name] = np.nan
    return df


def make_table_standard(df, table_name, year):
    properties = Metadata.standard_tables[table_name]
    for column_name in properties["columns"].keys():
        column_property = utils.get_version(properties["columns"][column_name], year)
        if column_property is None:
            continue

        if "type" in column_property:
            if column_property["type"] == "categorical":
                categoricla_columns(df, column_name, column_property)
            elif column_property["type"] == "numerical":
                numerical_columns(df, column_name, column_property)
    return df


def get_column_order(df, table_name):
    properties = Metadata.standard_tables[table_name]
    old_order = df.columns
    new_order = [column for column in properties["order"] if column in old_order]
    return new_order


def load_table(
    table_name,
    from_year=None,
    to_year=None,
    standardize=False,
    note_year=None,
    parquets_directory=Defaults.processed_data,
):
    start, end = utils.build_year_interval(from_year=from_year, to_year=to_year)
    if note_year is None:
        if (end - start) > 1:
            note_year = True
        else:
            False

    table_collection = []
    for year in range(start, end):
        table = pd.read_parquet(
            parquets_directory.joinpath(f"{year}_{table_name}.parquet")
        )
        if standardize:
            table = make_table_standard(df=table, table_name=table_name, year=year)
        if note_year:
            table["Year"] = year
        table_collection.append(table)
    output_table = pd.concat(table_collection, ignore_index=True)
    if standardize:
        columns = get_column_order(output_table, table_name=table_name)
        output_table = output_table[columns]
    return output_table


def _get_part_position(part_name: str, year: int):
    id_length = utils.get_version(
        dictionary=Metadata.house_hold_id["ID_Length"], year=year
    )
    position = utils.get_version(
        dictionary=Metadata.house_hold_id[part_name]["position"], year=year
    )
    right = id_length - position["end"]
    left = position["end"] - position["start"]
    return right, left


def get_part_code(id_column: pd.Series, part_name: str, year: int):
    right, left = _get_part_position(part_name=part_name, year=year)
    new_column = (id_column // pow(10, right) % pow(10, left)).copy()
    return new_column


def get_part_names(id_column: pd.Series, part_name: str, year: int):
    part_codes = get_part_code(id_column=id_column, part_name=part_name, year=year)
    names_mapping = utils.get_version(
        dictionary=Metadata.house_hold_id[part_name]["names"], year=year
    )
    part_codes = part_codes.astype("category")
    part_names = part_codes.cat.rename_categories(names_mapping)
    return part_names


def _get_commodity_properties(commodity_name: str, year: int):
    def _read_property(property):
        if "table" in property:
            table_name = property["table"]
        else:
            table_name = commodity_property["table"]
        if "duration" in property:
            duration = property["duration"]
        else:
            duration = 30
        if type(property["code"]) is int:
            code_intervals = [[property["code"], property["code"] + 1]]
        elif type(property["code"][0]) is int:
            code_intervals = [property["code"]]
        else:
            code_intervals = property["code"]
        return table_name, code_intervals, duration

    commodity_property = Metadata.commodity_codes[commodity_name]
    code_history = utils.get_version(commodity_property["code_history"], year)
    property_list = []
    if 1 in code_history:
        for code_property in code_history.values():
            property_list.append(_read_property(code_property))
    else:
        property_list.append(_read_property(code_history))
    return property_list


def _build_filter_from_intervals(df, code_intervals):
    filt = pd.Series(False, index=df.index, dtype=bool)
    for start, end in code_intervals:
        filt = filt | ((start < df["Code"]) & (df["Code"] < end))
    return filt


def get_commodity_data(commodity_name: str, year: int, note_duration=False):
    property_list = _get_commodity_properties(commodity_name=commodity_name, year=year)
    table_collection = []
    for table_name, code_intervals, duration in property_list:
        table = load_table(table_name=table_name, from_year=year, standardize=True)
        filt = _build_filter_from_intervals(df=table, code_intervals=code_intervals)
        table = table.loc[filt]
        if note_duration:
            table["Duration"] = duration
        table_collection.append(table)
    output_table = pd.concat(table_collection).sort_index()
    return output_table
