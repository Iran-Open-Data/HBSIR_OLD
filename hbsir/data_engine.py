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


# pylint: disable=unsubscriptable-object
# pylint: disable=unsupported-membership-test
class SchemaApplier:
    def __init__(self, table: pd.DataFrame, schema: dict):
        self.table = table
        schema_version = utils.MetadataVersionResolver(schema).get_version()
        # TODO(mohammad_ali): Add schema validation here.
        if isinstance(schema_version, dict):
            self.schema = schema_version
        else:
            raise ValueError("Schema is not valid")
        self.modules: dict[str, ModuleType] = {}

    def apply(self) -> pd.DataFrame:
        if self.table.empty:
            return self.table
        self._apply_settings()
        if "preprocess" in self.schema:
            self._apply_instructions(self.schema["preprocess"])
        if "classifications" in self.schema:
            self._add_classifications(self.schema["classifications"])
        if "attributes" in self.schema:
            self._add_attribute(self.schema["attributes"])
        if "columns" in self.schema:
            instructions: dict = self.schema["columns"]
            for name, instruction in instructions.items():
                self._apply_column_instruction(name, instruction)
        if "postprocess" in self.schema:
            self._apply_instructions(self.schema["postprocess"])
        self._apply_order()
        return self.table

    def _apply_settings(self) -> None:
        settings: dict = metadatas.schema["default_settings"].copy()
        if "settings" in self.schema:
            settings.update(self.schema["settings"])
        if settings["add_table_names"]:
            self.table["Table_Name"] = self.schema["table_name"]
        if settings["add_year"]:
            self.table["Year"] = self.schema["year"]
        if ("add_weights" in settings) and settings["add_weights"]:
            self.table = add_weights(self.table)

    def _apply_instructions(self, instructions: str | list[str]) -> None:
        instructions = [instructions] if isinstance(instructions, str) else instructions
        module_name_list = []
        func_name_list = []
        for instruction in instructions:
            module_name, func_name = instruction.rsplit(".", 1)
            module_name_list.append(module_name)
            func_name_list.append(func_name)
        module_list = self._load_modules(module_name_list)
        for func_name, module in zip(func_name_list, module_list):
            func = getattr(module, func_name)
            self.table = func(self.table)

    def _load_modules(self, module_name_list: list[str]) -> Iterable[ModuleType]:
        for name in module_name_list:
            if name not in self.modules:
                complete_name = "hbsir.schema_functions." + name
                self.modules[name] = importlib.import_module(complete_name)
        module_list = map(lambda x: self.modules[x], module_name_list)
        return module_list

    def _add_classifications(self, classifications) -> None:
        classifications = (
            [classifications] if isinstance(classifications, dict) else classifications
        )
        for classification in classifications:
            self.table = add_classification(self.table, **classification)

    def _add_attribute(self, attributes: dict | list[dict]) -> None:
        attributes = [attributes] if isinstance(attributes, dict) else attributes
        for attribute in attributes:
            self.table = add_attribute(self.table, **attribute)

    def _apply_column_instruction(self, column_name, instruction) -> None:
        if instruction is None:
            pass
        elif instruction["type"] == "numerical":
            self._apply_numerical_instruction(column_name, instruction)
        elif instruction["type"] == "categorical":
            self._apply_categorical_instruction(column_name, instruction)

    def _apply_numerical_instruction(self, column_name, instruction: dict) -> None:
        if isinstance(instruction["expression"], int):
            self.table.loc[:, column_name] = instruction["expression"]
            return
        columns_names = re.split(r"[\+\-\*\/\s\.]+", instruction["expression"])
        columns_names = [name for name in columns_names if not name.isnumeric()]
        self.table[column_name] = (
            self.table[columns_names].fillna(0).eval(instruction["expression"])
        )

    def _apply_categorical_instruction(
        self, column_name: str, instruction: dict
    ) -> None:
        categories: dict = instruction["categories"]

        if column_name in self.table.columns:
            categorical_column = self.table[column_name].copy()
        else:
            categorical_column = pd.Series(index=self.table.index, dtype="category")

        for category, condition in categories.items():
            filt = self._construct_filter(column_name, condition)
            categorical_column = categorical_column.cat.add_categories([category])
            categorical_column.loc[filt] = category

        self.table[column_name] = categorical_column

    def _construct_filter(self, column_name, condition) -> pd.Series:
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

    def _apply_order(self) -> pd.DataFrame:
        if "order" in self.schema:
            new_columns = [
                column
                for column in self.schema["order"]
                if column in self.table.columns
            ]
            self.table = self.table[new_columns]
        return self.table


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
        self.tables_schema = metadatas.schema.copy()
        self.original_tables: dict[str, pd.DataFrame] = {}

    def load(self) -> pd.DataFrame:
        table_list = []
        for year in self.years:
            table_list.append(self._load_table(self.table_name, year))
        table = pd.concat(table_list, ignore_index=True)
        return table

    def _load_table(self, table_name: str, year: int) -> pd.DataFrame:
        if table_name in get_args(_OriginalTable):
            table = self._load_original_table(table_name, year)
        else:
            table = self._construct_schema_based_table(table_name, year)
        return table

    def _get_schema(self, table_name: str, year: int) -> dict:
        schema: dict = self.tables_schema[table_name].copy()
        schema.update({"table_name": table_name, "year": year})
        return schema

    def _apply_schema(
        self,
        table: pd.DataFrame,
        table_name: str | None = None,
        year: int | None = None,
    ):
        if table_name is None:
            if "table_name" in table.attrs:
                table_name = table.attrs["table_name"]
            else:
                raise ValueError("Table name not provided")
        if year is None:
            if "year" in table.attrs:
                year = table.attrs["year"]
            else:
                raise ValueError("Year not provided")
        schema = self._get_schema(table_name, year)  # type: ignore
        table = SchemaApplier(table, schema).apply()
        return table

    def _load_original_table(self, table_name: str, year: int) -> pd.DataFrame:
        if f"{table_name}_{year}" in self.original_tables:
            return self.original_tables[f"{table_name}_{year}"]
        table = TableHandler(table_name, year, self.settings).read()
        if table_name in self.tables_schema:
            table = self._apply_schema(table, table_name, year)
        self.original_tables[f"{table_name}_{year}"] = table
        return table

    def _construct_schema_based_table(self, table_name: str, year: int) -> pd.DataFrame:
        if table_name not in self.tables_schema:
            raise KeyError(f"Table name {table_name} is not available in schema")
        table_names = self.tables_schema[table_name]["table_list"]

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
            table_list.append(self._load_table(name, year))
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
    # pylint: disable=unsupported-assignment-operation
    def classification_table(self) -> pd.DataFrame:
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
        # pylint: disable=unsupported-assignment-operation
        code_table[row.index] = row
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
        mapping = self.household_metadata[self.attribute_name][translation]
        code_builder = self._create_code_builder()

        def translator(household_id_column: pd.Series) -> pd.Series:
            mapped = code_builder(household_id_column).map(mapping).astype("category")
            mapped.name = translation
            return mapped

        return translator

    def _create_code_builder(self):
        assert isinstance(self.household_metadata, dict)
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
