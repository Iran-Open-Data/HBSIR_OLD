"""
Main file for ordinary use
"""

import re
from typing import Literal
from types import ModuleType
import importlib

import pandas as pd
import yaml

from . import decoder, utils
from .metadata_reader import defaults, metadata, original_tables, LoadTable, _Years
from .data_cleaner import open_and_clean_table


class TableHandler:
    """A class for loading parquet files"""

    def __init__(self, table_name: str, year: int, settings: LoadTable) -> None:
        self.table_name = table_name
        self.year = year
        self.file_name = f"{year}_{table_name}.parquet"
        self.local_path = defaults.processed_data.joinpath(self.file_name)
        self.file_url = f"{defaults.online_dir}/parquet_files/{self.file_name}"
        self.settings = settings

    def read(self) -> pd.DataFrame:
        """Read the parquet file"""
        self.local_path.parent.mkdir(exist_ok=True, parents=True)
        if (self.settings.on_missing == "create") or self.settings.recreate:
            if (not self.local_path.exists()) or self.settings.recreate:
                table = open_and_clean_table(self.table_name, self.year)
                if self.settings.save_created:
                    table.to_parquet(self.local_path)
            else:
                table = self.read_local_file()
        elif self.settings.on_missing == "download" or self.settings.redownload:
            if (not self.local_path.exists()) or self.settings.redownload:
                table = self.download()
                if self.settings.save_downloaded:
                    table.to_parquet(self.local_path)
            else:
                table = self.read_local_file()
        else:
            table = self.read_local_file()

        table.attrs["table_name"] = self.table_name
        table.attrs["year"] = self.year
        return table

    def download(self) -> pd.DataFrame:
        """Download parquet file to memory"""
        return pd.read_parquet(self.file_url)

    def read_local_file(self) -> pd.DataFrame:
        """Load parquet file from local hard drive"""
        return pd.read_parquet(self.local_path)


class Applier:
    def __init__(
        self, table: pd.DataFrame, instructions: list, properties: dict | None = None
    ) -> None:
        self.table = table
        self.properties = properties if properties is not None else {}
        self.modules: dict[str, ModuleType] = {}
        for instruction in instructions:
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
        self.table = WeightAdder(self.table).add_weights()

    def _add_classification(self, method_input: dict) -> None:
        settings = decoder.CommodityDecoderSettings(**method_input)
        self.table = decoder.CommodityDecoder(self.table, settings).add_classification()

    def _add_attribute(self, method_input: dict) -> None:
        settings = decoder.IDDecoderSettings(**method_input)
        self.table = decoder.IDDecoder(self.table, settings).add_attribute()

    def _apply_order(self, method_input: list):
        new_columns = [
            column for column in method_input if column in self.table.columns
        ]
        self.table = self.table[new_columns]

    def _create_column(self, method_input=None) -> None:
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

    def _apply_filter(self, conditions: str | list[str]):
        conditions = [conditions] if isinstance(conditions, str) else conditions
        for condition in conditions:
            self.table = self.table.query(condition)

    def _apply_pandas_function(self, method_input: str | None = None) -> None:
        if method_input is None:
            return
        method_input = "self.table" + method_input
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
        settings = LoadTable()
        other_table = TableLoader(table_name, years, settings).load()
        self.table = self.table.merge(other_table, on=columns)


class TableLoader:
    def __init__(
        self,
        table_name: str,
        years: _Years,
        settings: LoadTable,
    ):
        self.table_name = table_name
        self.years = utils.parse_years(years)
        self.settings = settings
        self.schema: dict = metadata.schema.copy()
        if table_name in self.schema:
            self.table_schema: dict = self.schema[table_name]
        else:
            self.table_schema = {}
        self.original_tables_cache: dict[str, pd.DataFrame] = {}

    def load(self) -> pd.DataFrame:
        table_list = []
        for year in self.years:
            table = self._load_table(self.table_name, year)
            table_list.append(table)
        table = pd.concat(table_list)
        if "views" in self.table_schema:
            table.view.views = self.table_schema["views"]
        return table

    def _load_table(self, table_name: str, year: int) -> pd.DataFrame:
        if table_name in original_tables:
            table = self._load_original_table(table_name, year)
        elif self.schema[table_name].get("cache_result", False):
            try:
                table = self.read_cached_table(table_name, year)
            except FileNotFoundError:
                table = self._construct_schema_based_table(table_name, year)
                self.save_cache(table, table_name, year)
        else:
            table = self._construct_schema_based_table(table_name, year)
        return table

    def read_cached_table(
        self,
        table_name: str | None = None,
        year: int | None = None,
    ) -> pd.DataFrame:
        if not self.check_table_dependencies(table_name, year):
            raise FileNotFoundError
        file_name = f"{table_name}_{year}.parquet"
        file_path = defaults.cached_data.joinpath(file_name)
        table = pd.read_parquet(file_path)
        return table

    def extract_dependencies(self, table_name: str, year: int) -> dict:
        table_list = [table_name]
        dependencies: dict[str, dict] = {}
        while len(table_list) > 0:
            table = table_list.pop(0)
            if "table_list" in metadata.schema[table]:
                dependencies[table] = metadata.schema[table]
                upstream_tables = utils.MetadataVersionResolver(
                    metadata.schema[table]["table_list"], year=year
                ).get_version()
                assert isinstance(upstream_tables, list)
                table_list.extend(upstream_tables)
            else:
                file_name = f"{year}_{table}.parquet"
                local_path = defaults.processed_data.joinpath(file_name)
                dependencies[table] = {"size": local_path.stat().st_size}
        return dependencies

    def check_table_dependencies(self, table_name, year) -> bool:
        file_name = f"{table_name}_{year}_metadata.yaml"
        cach_metadata_path = defaults.cached_data.joinpath(file_name)
        with open(cach_metadata_path, encoding="utf-8") as file:
            cach_metadata = yaml.safe_load(file)
        file_dependencies = cach_metadata["dependencies"]
        current_dependencies = self.extract_dependencies(table_name, year)
        return file_dependencies == current_dependencies

    def save_cache(
        self,
        table: pd.DataFrame,
        table_name: str,
        year: int,
    ) -> None:
        defaults.cached_data.mkdir(parents=True, exist_ok=True)
        file_name = f"{table_name}_{year}.parquet"
        file_path = defaults.cached_data.joinpath(file_name)
        file_name = f"{table_name}_{year}_metadata.yaml"
        cach_metadata_path = defaults.cached_data.joinpath(file_name)
        file_metadata = {"dependencies": self.extract_dependencies(table_name, year)}
        with open(cach_metadata_path, mode="w", encoding="utf-8") as file:
            yaml.safe_dump(file_metadata, file)
        table.to_parquet(file_path, index=False)

    def _apply_schema(
        self,
        table: pd.DataFrame,
        table_name: str | None = None,
        year: int | None = None,
    ):
        if "instructions" not in self.schema[table_name]:
            return table

        if (table_name is None) and ("table_name" in table.attrs):
            table_name = table.attrs["table_name"]

        if (year is None) and ("year" in table.attrs):
            year = table.attrs["year"]
        instructions = utils.MetadataVersionResolver(
            self.schema[table_name]["instructions"], year
        ).get_version()
        assert isinstance(instructions, list)
        props = {"year": year, "table_name": table_name}
        table = Applier(table, instructions, props).table
        return table

    def _load_original_table(self, table_name: str, year: int) -> pd.DataFrame:
        if f"{table_name}_{year}" in self.original_tables_cache:
            return self.original_tables_cache[f"{table_name}_{year}"]
        table = TableHandler(table_name, year, self.settings).read()
        if not table.empty and (table_name in self.schema):
            table = self._apply_schema(table, table_name, year)
        self.original_tables_cache[f"{table_name}_{year}"] = table
        return table

    def _construct_schema_based_table(self, table_name: str, year: int) -> pd.DataFrame:
        if table_name not in self.schema:
            raise KeyError(f"Table name {table_name} is not available in schema")
        table_names = self.schema[table_name]["table_list"]
        table_names = utils.MetadataVersionResolver(table_names, year).get_version()
        assert isinstance(table_names, (str, list))

        table_list = self._collect_schema_tables(table_names, year)

        table = pd.concat(table_list)
        table = self._apply_schema(table, table_name, year)
        return table

    def _collect_schema_tables(
        self, table_names: str | list[str], year: int
    ) -> list[pd.DataFrame]:
        table_names = [table_names] if isinstance(table_names, str) else table_names
        table_list = []
        for name in table_names:
            table = self._load_table(name, year)
            if not table.empty:
                table_list.append(table)
        return table_list


class WeightAdder:
    def __init__(
        self,
        table: pd.DataFrame,
        method: Literal["default", "external", "household_info"] = "default",
        year_column_name: str = "Year",
    ) -> None:
        self.table = table
        self.method = method
        self.year_column_name = year_column_name

    def load_weights(self, year) -> pd.Series:
        if self.method == "default":
            if year <= 1395:
                method = "external"
            else:
                method = "household_info"
        elif self.method in ["external", "household_info"]:
            method = self.method
        else:
            raise ValueError("Method is not valid")

        if method == "external":
            weights = self._load_from_external_data(year)
        else:
            weights = self._load_from_household_info(year)
        return weights

    @staticmethod
    def _load_from_household_info(year) -> pd.Series:
        settings = LoadTable()
        loader = TableLoader("household_information", year, settings)
        hh_info = loader.load()
        weights = hh_info.set_index("ID")["Weight"]
        return weights

    @staticmethod
    def _load_from_external_data(year) -> pd.Series:
        weights_path = defaults.external_data.joinpath("weights.parquet")
        if not weights_path.exists():
            defaults.external_data.mkdir(parents=True, exist_ok=True)
            utils.download(
                f"{defaults.online_dir}/external_data/weights.parquet", weights_path
            )
        weights = pd.read_parquet(weights_path)
        weights = weights.loc[(year), "Weight"]
        assert isinstance(weights, pd.Series)
        return weights

    def add_weights(self, **kwargs) -> pd.DataFrame:
        years = decoder.extract_column(self.table, self.year_column_name)
        years = years.drop_duplicates()
        weights_list = []
        for year in years:
            weights_list.append(self.load_weights(year))
        weights = pd.concat(
            weights_list, axis="index", keys=years, names=["Year", "ID"]
        )
        self.table = self.table.join(weights, on=["Year", "ID"], **kwargs)
        return self.table
