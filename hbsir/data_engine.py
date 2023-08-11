"""
Main file for ordinary use
"""

import re
from typing import Iterable, Literal
from types import ModuleType
import importlib

import pandas as pd

from . import metadata_reader, decoder, utils
from .data_cleaner import open_and_clean_table

defaults = metadata_reader.defaults
metadatas = metadata_reader.metadata
_Attribute = metadata_reader.Attribute
_OriginalTable = metadata_reader.OriginalTable
_StandardTables = metadata_reader.StandardTable
_Table = metadata_reader.Table


class TableHandler:
    """A class for loading parquet files"""

    def __init__(
        self, table_name: str, year: int, settings: metadata_reader.LoadTable
    ) -> None:
        self.table_name = table_name
        self.year = year
        self.file_name = f"{year}_{table_name}.parquet"
        self.local_path = defaults.processed_data.joinpath(self.file_name)
        self.file_url = f"{defaults.online_dir}/parquet_files/{self.file_name}"
        self.settings = settings

    def read(self) -> pd.DataFrame:
        """Read the parquet file"""
        if self.settings.on_missing == "create":
            if not self.local_path.exists() or self.settings.recreate:
                table = open_and_clean_table(self.table_name, self.year)
            else:
                table = self.read_local_file()
            if self.settings.save_created:
                self.local_path.parent.mkdir(exist_ok=True, parents=True)
                table.to_parquet(self.local_path)
        elif self.settings.on_missing == "download":
            if not self.local_path.exists() or self.settings.recreate:
                table = self.download()
            else:
                table = self.read_local_file()
            if self.settings.save_downloaded:
                self.local_path.parent.mkdir(exist_ok=True, parents=True)
                table.to_parquet(self.local_path)
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

    def _apply_order(self, method_input):
        new_columns = [
            column for column in method_input if column in self.table.columns
        ]
        self.table = self.table[new_columns]

    def _create_column(self, method_input=None) -> None:
        if method_input is None:
            return
        column_name = method_input["name"]
        expression = method_input["expression"]
        if method_input["type"] == "numerical":
            self.__apply_numerical_instruction(column_name, expression)
        elif method_input["type"] == "categorical":
            self.__apply_categorical_instruction(column_name, expression)

    def __apply_numerical_instruction(self, column_name, expression: int | str) -> None:
        if isinstance(expression, int):
            self.table.loc[:, column_name] = expression
            return
        columns_names = re.split(r"[\+\-\*\/\s\.]+", expression)
        columns_names = [name for name in columns_names if not name.isnumeric()]
        self.table[column_name] = self.table[columns_names].fillna(0).eval(expression)

    def __apply_categorical_instruction(
        self, column_name: str, instruction: dict
    ) -> None:
        categories: dict = instruction["categories"]

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

    def _apply_external_function(self, method_input: str) -> None:
        module_name, func_name = method_input.rsplit(".", 1)
        self.__load_module(module_name)
        func = getattr(self.modules[module_name], func_name)
        self.table = func(self.table)

    def __load_module(self, module_name: str) -> None:
        if module_name not in self.modules:
            self.modules[module_name] = importlib.import_module(module_name)


class TableLoader:
    def __init__(
        self,
        table_name: str,
        years: int | Iterable[int] | str | None,
        settings: metadata_reader.LoadTable,
    ):
        self.table_name = table_name
        self.years = utils.parse_years(years)
        self.settings = settings
        self.schema: dict = metadatas.schema.copy()
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
        if table_name in metadata_reader.original_tables:
            table = self._load_original_table(table_name, year)
        else:
            table = self._construct_schema_based_table(table_name, year)
        return table

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
        settings = metadata_reader.LoadTable()
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

    def add_weights(self) -> pd.DataFrame:
        years = decoder.extract_column(self.table, self.year_column_name)
        years = years.drop_duplicates()
        weights_list = []
        for year in years:
            weights_list.append(self.load_weights(year))
        weights = pd.concat(
            weights_list, axis="index", keys=years, names=["Year", "ID"]
        )
        self.table = self.table.join(weights, on=["Year", "ID"])
        return self.table
