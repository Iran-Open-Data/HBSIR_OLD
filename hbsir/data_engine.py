"""
Main file for ordinary use
"""

import re
from dataclasses import dataclass
from typing import Iterable, Literal, get_args
from types import ModuleType
import importlib
import itertools

import pandas as pd

from . import metadata_reader, utils
from .data_cleaner import open_and_clean_table

defaults = metadata_reader.defaults
metadatas = metadata_reader.metadatas
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
        if self.local_path.exists():
            table = self.read_local_file()
        elif self.settings.on_missing == "create":
            table = open_and_clean_table(self.table_name, self.year)
            if self.settings.save_created:
                self.local_path.parent.mkdir(exist_ok=True, parents=True)
                table.to_parquet(self.local_path)
        elif self.settings.on_missing == "download":
            table = self.download()
            if self.settings.save_downloaded:
                self.local_path.parent.mkdir(exist_ok=True, parents=True)
                table.to_parquet(self.local_path)
        else:
            raise FileNotFoundError

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
        self.table = add_weights(self.table)

    def _add_classification(self, method_input: dict) -> None:
        self.table = add_classification(self.table, **method_input)

    def _add_attribute(self, method_input: dict) -> None:
        self.table = add_attribute(self.table, **method_input)

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
        table = pd.concat(table_list, ignore_index=True)
        if "views" in self.table_schema:
            table.view.views = self.table_schema["views"]
        return table

    def _load_table(self, table_name: str, year: int) -> pd.DataFrame:
        if table_name in get_args(_OriginalTable):
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

        table = pd.concat(table_list, ignore_index=True)
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


@dataclass
class ClassificationSettings:
    levels: list[int] | tuple[int, ...] = (2, 3)
    column_names: list[str] | tuple[str, ...] = ("classification",)
    labels: list[str] | tuple[str, ...] = ("item_key",)
    required_columns = ("drop",)
    code_column_name: str = "Code"
    year_column_name: str = "Year"
    drop_value: bool = False
    missing_value_replacements: dict[str, str] | None = None


class Classification:
    def __init__(
        self,
        classification_name: str = "original",
        year: int | None = None,
        settings: ClassificationSettings | None = None,
        **kwargs,
    ):
        self.name = classification_name
        self.year = year
        versioned_info = metadatas.commodities[classification_name]
        category_resolver = utils.MetadataCategoryResolver(versioned_info, year)
        self.classification_info = category_resolver.categorize_metadata()
        self.settings = self.create_settings(settings, **kwargs)

    def create_settings(self, settings, **kwargs):
        classification_default_settings = {
            key.split("_", 1)[1]: value
            for key, value in self.classification_info.items()
            if key.split("_", 1)[0] == "default"
        }
        if settings is None:
            settings = ClassificationSettings()
        for key, value in classification_default_settings.items():
            setattr(settings, key, value)

        for key, value in kwargs.items():
            setattr(settings, key, value)

        if isinstance(settings.levels, int):
            settings.levels = [settings.levels]
        if isinstance(settings.column_names, str):
            settings.column_names = [settings.column_names]
        if isinstance(settings.labels, str):
            settings.labels = [settings.labels]
        return settings

    @property
    def classification_table(self) -> pd.DataFrame:
        # pylint: disable=unsubscriptable-object
        table = pd.DataFrame(self.classification_info["items"])
        table["code_range"] = table["code"].apply(
            utils.Argham,  # type: ignore
            default_start=defaults.first_year,
            default_end=defaults.last_year + 1,
            keywords=["code"],
        )
        table = table.drop(columns=["code"])
        for column_name in self.settings.required_columns:
            table = self._set_column(table, column_name)
        return table

    def _set_column(self, table: pd.DataFrame, column_name: str):
        default_value = getattr(self.settings, f"{column_name}_value")
        if column_name not in table.columns:
            table[column_name] = default_value
        elif default_value is not None:
            table[column_name] = table[column_name].fillna(default_value)
        return table

    def construct_general_mapping_table(self, table: pd.DataFrame) -> pd.DataFrame:
        classification_codes = table["Code"].drop_duplicates()
        tables = []
        for _, row in self.classification_table.iterrows():
            code_table = self._build_code_table(classification_codes, row)
            tables.append(code_table)
        mapping_table = pd.concat(tables, ignore_index=True)
        # Validate
        filt = mapping_table.duplicated(["Code", "level"], keep=False)
        if filt.sum():
            invalid_case_sample = (
                mapping_table.loc[filt].sort_values(["Code", "level"]).head(10)
            )
            raise ValueError(f"Classification is not valid \n{invalid_case_sample}")
        mapping_table = (
            mapping_table.drop(columns=["code_range"])
            .set_index(["Code", "level"])
            .unstack()
        )
        assert isinstance(mapping_table, pd.DataFrame)
        return mapping_table

    @staticmethod
    def _build_code_table(
        classification_codes: pd.Series, row: pd.Series
    ) -> pd.DataFrame:
        filt = classification_codes.apply(lambda x: x in row["code_range"])
        available_codes = classification_codes.loc[filt]
        code_table = pd.DataFrame(
            index=available_codes, columns=row.index
        ).reset_index()
        code_table[row.index] = row  # pylint: disable=unsupported-assignment-operation
        return code_table

    def construct_mapping_table(self, table: pd.DataFrame) -> pd.DataFrame:
        mapping_table = self.construct_general_mapping_table(table)
        mapping_table = self._apply_drop(mapping_table)
        mapping_table = self._rename_columns(mapping_table)
        return mapping_table

    def _apply_drop(self, mapping_table: pd.DataFrame) -> pd.DataFrame:
        if "drop" not in mapping_table.columns:
            return mapping_table
        filt = mapping_table.loc[:, ("drop", slice(None))].prod(axis="columns") == 0  # type: ignore
        mapping_table = mapping_table.loc[filt]
        mapping_table = mapping_table.drop(columns=["drop"])
        return mapping_table

    def _rename_columns(self, mapping_table: pd.DataFrame) -> pd.DataFrame:
        levels = self.settings.levels
        available_levels = list(
            mapping_table.columns.get_level_values("level").unique()
        )
        levels = levels if len(levels) > 0 else available_levels
        column_names = list(itertools.product(self.settings.labels, levels))
        for column_name in column_names:
            if column_name not in mapping_table.columns:
                mapping_table[column_name] = None
        mapping_table = mapping_table.loc[:, column_names]
        if len(column_names) == len(self.settings.column_names):
            mapping_table.columns = self.settings.column_names
        elif len(self.settings.labels) == len(self.settings.column_names):
            column_names = itertools.product(self.settings.column_names, levels)
            mapping_table.columns = [
                f"{label}_{level}" for label, level in column_names
            ]
        else:
            mapping_table.columns = [
                f"{label}_{level}" for label, level in column_names
            ]
        return mapping_table

    def add_classification(self, table: pd.DataFrame) -> pd.DataFrame:
        mapping_table = self.construct_mapping_table(table)
        old_columns = table.columns
        table = table.merge(
            mapping_table, left_on="Code", right_index=True, how="left", validate="m:1"
        )
        new_columns = [column for column in table.columns if column not in old_columns]
        table = self._set_default_values(table, new_columns)
        return table

    def _set_default_values(self, table: pd.DataFrame, columns: list):
        if self.settings.missing_value_replacements is None:
            return table
        for column_name in columns:
            if column_name not in self.settings.missing_value_replacements:
                continue
            value = self.settings.missing_value_replacements[column_name]
            table[column_name] = table[column_name].fillna(value)
        return table


class Attribute:
    def __init__(
        self,
        attribute_name: _Attribute,
        year: int,
        translations: str | Iterable[str] = "names",
        columns_labels: Iterable[str] | None = None,
    ) -> None:
        self.household_metadata = utils.MetadataVersionResolver(
            metadatas.household, year
        ).get_version()
        self.attribute_name = attribute_name
        self.translations = (
            [translations] if isinstance(translations, str) else list(translations)
        )
        if columns_labels is None:
            if len(self.translations) == 1:
                self.columns_labels = [attribute_name]
            else:
                self.columns_labels = [
                    f"{attribute_name}_{translation}"
                    for translation in self.translations
                ]

    def construct_mapping_table(self, table: pd.DataFrame):
        household_ids = table["ID"].drop_duplicates().copy()
        household_ids = household_ids.set_axis(household_ids)
        mappings = []
        for translation in self.translations:
            translator = self._create_code_translator(translation)
            mappings.append(translator(household_ids))
        mapping_table = pd.concat(mappings, axis="columns", keys=self.columns_labels)
        return mapping_table

    def _create_code_translator(self, translation):
        assert isinstance(self.household_metadata, dict)
        # pylint: disable=unsubscriptable-object
        mapping = self.household_metadata[self.attribute_name][translation]
        code_builder = self._create_code_builder()

        def translator(household_id_column: pd.Series) -> pd.Series:
            mapped = code_builder(household_id_column).map(mapping).astype("category")
            mapped.name = translation
            return mapped

        return translator

    def _create_code_builder(self):
        assert isinstance(self.household_metadata, dict)
        # pylint: disable=unsubscriptable-object
        ld_len = self.household_metadata["ID_Length"]
        attr_dict = self.household_metadata[self.attribute_name]
        if ("position" not in attr_dict) or attr_dict["position"] is None:
            raise ValueError("Code position is not available")
        start, end = attr_dict["position"]["start"], attr_dict["position"]["end"]

        def builder(household_id_column: pd.Series) -> pd.Series:
            return (
                household_id_column
                % pow(10, (ld_len - start))
                // pow(10, (ld_len - end))
            )

        return builder

    def add_attribute(self, table: pd.DataFrame):
        mapping_table = self.construct_mapping_table(table)
        table = table.merge(
            mapping_table, left_on="ID", right_index=True, how="left", validate="m:1"
        )
        return table


class Weight:
    def __init__(
        self,
        year: int,
        method: Literal["default", "external", "household_info"] = "default",
    ) -> None:
        self.year = year
        if method == "default":
            if year <= 1395:
                self.method = "external"
            else:
                self.method = "household_info"
        elif method in ["external", "household_info"]:
            self.method = method
        else:
            raise ValueError("Method is not valid")

    def load_weights(self) -> pd.Series:
        if self.method == "external":
            weights = self._load_from_external_data()
        else:
            weights = self._load_from_household_info()
        return weights

    def _load_from_household_info(self) -> pd.Series:
        settings = metadata_reader.LoadTable()
        loader = TableLoader("household_information", self.year, settings)
        hh_info = loader.load()
        weights = hh_info.set_index("ID")["Weight"]
        return weights

    def _load_from_external_data(self) -> pd.Series:
        weights_path = defaults.external_data.joinpath("weights.parquet")
        if not weights_path.exists():
            defaults.external_data.mkdir(parents=True, exist_ok=True)
            utils.download(
                f"{defaults.online_dir}/external_data/weights.parquet", weights_path
            )
        weights = pd.read_parquet(weights_path)
        weights = weights.loc[(self.year), "Weight"]
        assert isinstance(weights, pd.Series)
        return weights

    def add_weights(self, table: pd.DataFrame) -> pd.DataFrame:
        weights = self.load_weights()
        table = table.merge(
            weights, left_on="ID", right_index=True, how="left", validate="m:1"
        )
        return table


def add_classification(
    table: pd.DataFrame, classification_name: str = "original", **kwargs
):
    if table.empty:
        return table
    years = table["Year"].drop_duplicates().to_list()
    subtables = []
    for year in years:
        classificationer = Classification(
            classification_name=classification_name, year=year, **kwargs
        )
        filt = table["Year"] == year
        subtable = classificationer.add_classification(table.loc[filt])
        subtables.append(subtable)
    return pd.concat(subtables, ignore_index=True)


def add_attribute(
    table: pd.DataFrame, attribute_name: _Attribute, **kwargs
) -> pd.DataFrame:
    years = list(table["Year"].drop_duplicates())
    subtables = []
    for year in years:
        attribute_reader = Attribute(year=year, attribute_name=attribute_name, **kwargs)
        filt = table["Year"] == year
        subtable = table.loc[filt].copy()
        subtable = attribute_reader.add_attribute(subtable)
        subtables.append(subtable)
    return pd.concat(subtables, ignore_index=True)


def add_weights(table: pd.DataFrame) -> pd.DataFrame:
    years = table["Year"].drop_duplicates().to_list()
    subtables = []
    for year in years:
        filt = table["Year"] == year
        subtable = table.loc[filt].copy()
        subtable = Weight(year).add_weights(subtable)
        subtables.append(subtable)
    return pd.concat(subtables, ignore_index=True)
