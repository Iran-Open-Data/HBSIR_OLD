"""
Main file for ordinary use
"""

import re
from typing import Literal, Iterable
from types import ModuleType
import importlib
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import yaml

from . import decoder, utils, external_data
from .metadata_reader import (
    defaults,
    metadata,
    original_tables,
    LoadTable,
    _Years,
    _OriginalTable,
)
from .data_cleaner import open_and_clean_table


def extract_dependencies(table_name: str, year: int) -> dict:
    table_list = [table_name]
    dependencies: dict[str, dict] = {}
    while len(table_list) > 0:
        table = table_list.pop(0)
        if "table_list" in metadata.schema[table]:
            dependencies[table] = metadata.schema[table]
            upstream_tables = utils.MetadataVersionResolver(
                metadata.schema[table]["table_list"], year=year
            ).get_version()
            if isinstance(upstream_tables, str):
                upstream_tables = [upstream_tables]
            assert isinstance(upstream_tables, list)
            table_list.extend(upstream_tables)
        else:
            file_name = f"{year}_{table}.parquet"
            local_path = defaults.processed_data.joinpath(file_name)
            size = local_path.stat().st_size if local_path.exists() else None
            dependencies[table] = {"size": size}
    return dependencies


class TableHandler:
    """A class for loading parquet files"""

    def __init__(
        self,
        table_list: Iterable[_OriginalTable],
        year: int,
        settings: LoadTable | None = None,
    ) -> None:
        self.table_list = table_list
        self.year = year
        self.settings = settings if settings is not None else LoadTable()
        self.tables: dict[str, pd.DataFrame] = self.setup()

    def __getitem__(self, __name: _OriginalTable) -> pd.DataFrame:
        return self.tables[__name]

    def get(
        self, names: _OriginalTable | Iterable[_OriginalTable]
    ) -> list[pd.DataFrame]:
        names = [names] if isinstance(names, str) else names
        return [self[name] for name in names]

    def setup(self) -> dict[str, pd.DataFrame]:
        with ThreadPoolExecutor(max_workers=6) as executer:
            tables = zip(
                self.table_list, executer.map(self.read_table, self.table_list)
            )
        return dict(tables)

    def read_table(self, table_name: _OriginalTable) -> pd.DataFrame:
        """Read the parquet file"""
        file_name = f"{self.year}_{table_name}.parquet"
        local_file = defaults.processed_data.joinpath(file_name)

        if self.settings.recreate:
            table = self.create_table(table_name)
        elif self.settings.redownload:
            table = self.download_table(table_name)
        elif local_file.exists():
            table = pd.read_parquet(local_file)
        elif self.settings.on_missing == "create":
            table = self.create_table(table_name)
        elif self.settings.on_missing == "download":
            table = self.download_table(table_name)
        else:
            raise FileNotFoundError

        table.attrs["table_name"] = table_name
        table.attrs["year"] = self.year
        return table

    def create_table(self, table_name: _OriginalTable) -> pd.DataFrame:
        file_name = f"{self.year}_{table_name}.parquet"
        local_path = defaults.processed_data.joinpath(file_name)
        table = open_and_clean_table(table_name, self.year)
        if self.settings.save_created:
            table.to_parquet(local_path)
        return table

    def download_table(self, table_name: _OriginalTable) -> pd.DataFrame:
        file_name = f"{self.year}_{table_name}.parquet"
        local_path = defaults.processed_data.joinpath(file_name)
        file_url = f"{defaults.online_dir}/parquet_files/{file_name}"
        table = pd.read_parquet(file_url)
        if self.settings.save_downloaded:
            table.to_parquet(local_path)
        return table


class Applier:
    def __init__(
        self, table: pd.DataFrame, instructions: list, properties: dict | None = None
    ) -> None:
        self.table = table
        self.properties = properties if properties is not None else {}
        self.modules: dict[str, ModuleType] = {}
        for instruction in instructions:
            if instruction is None:
                continue
            method_name, method_input = self.extract_method_name(instruction)
            if method_input is None:
                getattr(self, f"_{method_name}")()
            else:
                getattr(self, f"_{method_name}")(method_input)

    def extract_method_name(self, instruction):
        if isinstance(instruction, str):
            method_name = instruction
            method_input = None
        elif isinstance(instruction, dict):
            method_name, method_input = list(instruction.items())[0]
        else:
            raise TypeError
        return method_name, method_input

    def _add_year(self) -> None:
        self.table["Year"] = self.properties["year"]

    def _add_table_name(self) -> None:
        self.table["Table_Name"] = self.properties["table_name"]

    def _add_weights(self) -> None:
        self.table = add_weights(self.table)

    def _add_classification(self, method_input: dict | None = None) -> None:
        if method_input is None:
            return
        settings = decoder.DecoderSettings(**method_input)
        self.table = decoder.Decoder(self.table, settings).add_classification()

    def _add_attribute(self, method_input: dict | None = None) -> None:
        if method_input is None:
            return
        settings = decoder.IDDecoderSettings(**method_input)
        self.table = decoder.IDDecoder(self.table, settings).add_attribute()

    def _apply_order(self, method_input: list):
        new_order = [
            column if isinstance(column, str) else list(column.keys())[0]
            for column in method_input
        ]
        types = {
            list(column.keys())[0]: list(column.values())[0]
            for column in method_input
            if isinstance(column, dict)
        }

        self.table = self.table[list(new_order)].astype(types)

    def _create_column(self, method_input: dict | None = None) -> None:
        if method_input is None:
            return
        column_name = method_input["name"]
        if method_input["type"] == "numerical":
            expression = method_input["expression"]
            self.__apply_numerical_instruction(column_name, expression)
        elif method_input["type"] == "categorical":
            categories = method_input["categories"]
            self.__apply_categorical_instruction(column_name, categories)

    def __apply_numerical_instruction(self, column_name, expression: int | str) -> None:
        if isinstance(expression, int):
            self.table.loc[:, column_name] = expression
            return
        columns_names = re.split(r"[\+\-\*\/\s\.\(\)]+", expression)
        columns_names = [
            name for name in columns_names if not (name.isnumeric() or (name is None))
        ]
        self.table[column_name] = (
            self.table[columns_names].fillna(0).eval(expression, engine="python")
        )

    def __apply_categorical_instruction(
        self, column_name: str, categories: dict
    ) -> None:
        if column_name in self.table.columns:
            categorical_column = self.table[column_name].copy()
        else:
            categorical_column = pd.Series(index=self.table.index, dtype="category")

        for category, condition in categories.items():
            filt = self.__construct_filter(column_name, condition)
            categorical_column = categorical_column.cat.add_categories([category])
            categorical_column.loc[filt] = category

        self.table[column_name] = categorical_column

    def __construct_filter(self, column_name, condition) -> pd.Series:
        if condition is None:
            filt = self.table.index.to_series()
        elif isinstance(condition, str):
            filt = self.table[column_name] == condition
        elif isinstance(condition, list):
            filt = self.table[column_name].isin(condition)
        elif isinstance(condition, dict):
            filts = []
            for other_column, value in condition.items():
                if isinstance(value, (bool, str)):
                    filts.append(self.table[other_column] == value)
                elif isinstance(value, list):
                    filts.append(self.table[other_column].isin(value))
                else:
                    raise KeyError
            filt_sum = pd.concat(filts, axis="columns").sum(axis="columns")
            filt = filt_sum == len(condition)
        else:
            raise KeyError
        return filt

    def _apply_filter(self, conditions: str | list[str] | None = None) -> None:
        if conditions is None:
            return
        conditions = [conditions] if isinstance(conditions, str) else conditions
        for condition in conditions:
            self.table = self.table.query(condition)

    def _apply_pandas_function(self, method_input: str | None = None) -> None:
        if method_input is None:
            return
        method_input = "self.table" + method_input.replace("\n", "")
        table = pd.eval(method_input, target=self.table, engine="python")
        assert isinstance(table, pd.DataFrame)
        self.table = table

    def _apply_external_function(self, method_input: str | None = None) -> None:
        if method_input is None:
            return
        module_name, func_name = method_input.rsplit(".", 1)
        self.__load_module(module_name)
        func = getattr(self.modules[module_name], func_name)
        self.table = func(self.table)

    def __load_module(self, module_name: str) -> None:
        if module_name not in self.modules:
            self.modules[module_name] = importlib.import_module(module_name)

    def _join(self, method_input: dict | str | None = None):
        if method_input is None:
            return
        if isinstance(method_input, str):
            table_name = method_input
            columns = ["Year", "ID"]
            years = None
        elif isinstance(method_input, dict):
            table_name = method_input["table_name"]
            columns = method_input["columns"]
            years = method_input.get("year", None)
        else:
            raise TypeError
        years = list(self.table["Year"].unique())
        other_table = load_table(table_name, years)
        self.table = self.table.merge(other_table, on=columns)


class TableLoader:
    def __init__(
        self,
        table_name: str,
        year: int,
        settings: LoadTable | None = None,
    ):
        self.table_name = table_name
        self.year = year
        self.settings = settings if settings is not None else LoadTable()
        schema = utils.MetadataVersionResolver(metadata.schema, year).get_version()

        if isinstance(schema, dict):
            self.schema = dict(schema)
        else:
            raise ValueError("Invalid Schema")

        if table_name in self.schema:
            table_schema = self.schema.get(table_name)
            assert isinstance(table_schema, dict)
            self.table_schema = table_schema
        else:
            self.table_schema = {}

        dependencies = extract_dependencies(table_name, year)
        dependencies = [
            table for table, props in dependencies.items() if "size" in props
        ]
        self.table_handler = TableHandler(dependencies, year)

    def load(self, table_name: str | None = None) -> pd.DataFrame:
        table_name = self.table_name if table_name is None else table_name

        if table_name in original_tables:
            table = self.table_handler[table_name]
            if not table.empty and (table_name in self.schema):
                table = self._apply_schema(table, table_name)
        elif self.schema[table_name].get("cache_result", False):
            try:
                table = self.read_cached_table(table_name)
            except FileNotFoundError:
                table = self._construct_schema_based_table(table_name)
                self.save_cache(table, table_name)
        else:
            table = self._construct_schema_based_table(table_name)
        return table

    def read_cached_table(
        self,
        table_name: str,
    ) -> pd.DataFrame:
        if not self.check_table_dependencies(table_name):
            raise FileNotFoundError
        file_name = f"{table_name}_{self.year}.parquet"
        file_path = defaults.cached_data.joinpath(file_name)
        table = pd.read_parquet(file_path)
        return table

    def check_table_dependencies(self, table_name: str) -> bool:
        file_name = f"{table_name}_{self.year}_metadata.yaml"
        cach_metadata_path = defaults.cached_data.joinpath(file_name)
        with open(cach_metadata_path, encoding="utf-8") as file:
            cach_metadata = yaml.safe_load(file)
        file_dependencies = cach_metadata["dependencies"]
        current_dependencies = extract_dependencies(table_name, self.year)
        return file_dependencies == current_dependencies

    def save_cache(
        self,
        table: pd.DataFrame,
        table_name: str,
    ) -> None:
        defaults.cached_data.mkdir(parents=True, exist_ok=True)
        file_name = f"{table_name}_{self.year}.parquet"
        file_path = defaults.cached_data.joinpath(file_name)
        file_name = f"{table_name}_{self.year}_metadata.yaml"
        cach_metadata_path = defaults.cached_data.joinpath(file_name)
        file_metadata = {"dependencies": extract_dependencies(table_name, self.year)}
        with open(cach_metadata_path, mode="w", encoding="utf-8") as file:
            yaml.safe_dump(file_metadata, file)
        table.to_parquet(file_path, index=False)

    def _apply_schema(
        self,
        table: pd.DataFrame,
        table_name: str,
    ):
        if "instructions" not in self.schema[table_name]:
            return table

        instructions = self.schema[table_name]["instructions"]
        assert isinstance(instructions, list)
        props = {"year": self.year, "table_name": table_name}
        table = Applier(table, instructions, props).table
        return table

    def _construct_schema_based_table(self, table_name: str) -> pd.DataFrame:
        if table_name not in self.schema:
            raise KeyError(f"Table name {table_name} is not available in schema")
        table_names = self.schema[table_name]["table_list"]
        assert isinstance(table_names, (str, list))

        table_list = self._collect_schema_tables(table_names)

        table = pd.concat(table_list)
        table = self._apply_schema(table, table_name)
        return table

    def _collect_schema_tables(
        self, table_names: str | list[str]
    ) -> list[pd.DataFrame]:
        table_names = [table_names] if isinstance(table_names, str) else table_names
        table_list = []
        for name in table_names:
            table = self.load(name)
            if not table.empty:
                table_list.append(table)
        return table_list


def load_table(
    table_name: str,
    years: _Years,
    settings: LoadTable | None = None,
) -> pd.DataFrame:
    table_list = []
    for year in utils.parse_years(years):
        TableLoader(table_name, year, settings)
        table = TableLoader(table_name, year, settings).load()
        table_list.append(table)
    table = pd.concat(table_list)
    # if "views" in self.table_schema:
    #     table.view.views = self.table_schema["views"]
    return table


def load_weights(
    year: int,
    adjust_for_household_size: bool = False,
    method: Literal["default", "external", "household_info"] = "default",
) -> pd.Series:
    """Load sample weights for a given year.

    Loads weights from different sources based on the year and specified
    method. Weights can be multiplied by the number of household members
    if multiply_members=True.

    Parameters
    ----------
    year : int
        Year to load weights for
    multiply_members : bool, default False
        Whether to multiply weights by the number of household members
    method : {"default", "external", "household_info"}, default "default"
        Where to load the weights from:
            "default": Use "external" for years <= 1395, "household_info" for later years
            "external": Load from external parquet file
            "household_info": Load from household_information table

    Returns
    -------
    weights : pd.Series
        Sample weights indexed by household ID

    """
    if method == "default":
        if year <= 1395:
            method = "external"
        else:
            method = "household_info"
    elif method in ["external", "household_info"]:
        pass
    else:
        raise ValueError("Method is not valid")

    if method == "external":
        weights = _load_from_external_data(year)
    else:
        weights = _load_from_household_info(year)

    if adjust_for_household_size:
        members = (
            load_table("Number_of_Members", years=year)
            .set_index("ID")
            .loc[:, "Members"]
        )
        weights, members = weights.align(members, join="left")
        weights = weights.mul(members)

    weights = weights.rename(defaults.columns.weight)
    return weights


def _load_from_household_info(year) -> pd.Series:
    loader = TableLoader("household_information", year)
    hh_info = loader.load()
    weights = hh_info.set_index("ID")["Weight"]
    return weights


def _load_from_external_data(year) -> pd.Series:
    weights = external_data.load_table("weights", reset_index=False)
    weights = weights.loc[(year), "Weight"]
    assert isinstance(weights, pd.Series)
    return weights


def add_weights(
    table: pd.DataFrame,
    adjust_for_household_size: bool = False,
    year_column_name: str = "Year",
) -> pd.DataFrame:
    """Add sample weights to a table of data.

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
    years = decoder.extract_column(table, year_column_name)
    years = years.drop_duplicates()
    weights_list = []
    for year in years:
        weights_list.append(load_weights(year, adjust_for_household_size))
    weights = pd.concat(weights_list, axis="index", keys=years, names=["Year", "ID"])
    table = table.join(weights, on=["Year", "ID"])
    return table
