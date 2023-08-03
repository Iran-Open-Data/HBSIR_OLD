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

from . import metadata, utils

defaults = metadata.defaults
metadatas = metadata.metadatas
_Attribute = metadata.Attribute
_OriginalTable = metadata.OriginalTable
_StandardTables = metadata.StandardTable
_Table = metadata.Table


class ParquetHandler:
    """A class for loading parquet files"""

    def __init__(
        self,
        table_name: str,
        year: int,
        download_if_missing: bool = True,
        save_if_download: bool = True,
    ) -> None:
        self.table_name = table_name
        self.year = year
        self.file_name = f"{year}_{table_name}.parquet"
        self.local_path = defaults.processed_data.joinpath(self.file_name)
        self.file_url = f"{defaults.online_dir}/parquet_files/{self.file_name}"
        self.download_if_missing = download_if_missing
        self.save_if_download = save_if_download

    def read(self) -> pd.DataFrame:
        """Read the parquet file"""
        if not self.local_path.exists() and not self.download_if_missing:
            raise FileNotFoundError
        if not self.local_path.exists() and self.download_if_missing:
            table = self.download()
            if self.save_if_download:
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
        self._apply_settings()
        if "preprocess" in self.schema:
            self._apply_instructions(self.schema["preprocess"])
        if "columns" in self.schema:
            instructions: dict = self.schema["columns"]
            for name, instruction in instructions.items():
                self._apply_column_instruction(name, instruction)
        if "postprocess" in self.schema:
            self._apply_instructions(self.schema["postprocess"])
        return self.table

    def _apply_settings(self) -> None:
        settings: dict = metadatas.schema["default_settings"]
        if "settings" in self.schema:
            settings.update(self.schema["settings"])
        if settings["add_table_names"]:
            self.table["Table_Name"] = self.schema["table_name"]
        if settings["add_year"]:
            self.table["Year"] = self.schema["year"]
        if settings["add_duration"]:
            self._add_duration()

    def _add_duration(self):
        self.table = add_classification(self.table, labels=[], add_duration=True)

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

    def _apply_column_instruction(self, column_name, instruction) -> None:
        if instruction is None:
            pass
        elif instruction["type"] == "numerical":
            self._apply_numerical_instruction(column_name, instruction)
        elif instruction["type"] == "categorical":
            self._apply_categorical_instruction(column_name, instruction)

    def _apply_numerical_instruction(self, column_name, instruction: dict) -> None:
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

    def apply_order(self) -> pd.DataFrame:
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
        table_names: str | list[str],
        years: int | Iterable[int] | str | None,
        apply_schema: bool = True,
    ):
        table_names = [table_names] if isinstance(table_names, str) else table_names
        self.table_names = table_names
        self.original_table_names = [
            name for name in table_names if name in get_args(_OriginalTable)
        ]
        self.schema_based_table_names = [
            name for name in table_names if name not in self.original_table_names
        ]

        self.years = utils.parse_years(years)
        self.apply_schema = apply_schema

        self.original_tables: dict[str, pd.DataFrame] = {}

    def load(self) -> pd.DataFrame:
        table_list = []
        table_years = utils.construct_table_year_pairs(self.table_names, self.years)
        table_list = [
            self._load_table(table_name, year) for table_name, year in table_years
        ]
        table = pd.concat(table_list, ignore_index=True)
        if len(self.table_names) == 1:
            schema = self._get_schema(self.table_names[0], self.years[-1])
            table = SchemaApplier(table, schema).apply_order()
        return table

    def _load_table(self, table_name: str, year: int) -> pd.DataFrame:
        if table_name in self.original_table_names:
            table = self._load_original_table(table_name, year)
        else:
            table = self._construct_schema_based_table(table_name, year)
        return table

    def _get_schema(self, table_name: str, year: int) -> dict:
        schema: dict = metadatas.schema[table_name]
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
        table = ParquetHandler(table_name, year).read()
        if self.apply_schema and (table_name in metadatas.schema):
            table = self._apply_schema(table)
        self.original_tables[f"{table_name}_{year}"] = table
        return table

    def _construct_schema_based_table(self, table_name: str, year: int) -> pd.DataFrame:
        if table_name not in metadatas.schema:
            raise KeyError("Table name is not available in schema")
        table_names = metadatas.schema[table_name]["table_list"]

        table_list = self._collect_schema_tables(table_names, year)

        table = pd.concat(table_list, ignore_index=True)
        table = self._apply_schema(table, table_name, year)
        return table

    def _collect_schema_tables(
        self, table_names: list[str], year: int
    ) -> list[pd.DataFrame]:
        original_table_names = [
            name for name in table_names if (name in get_args(_OriginalTable))
        ]
        schema_based_table_names = [
            name for name in table_names if name not in original_table_names
        ]
        table_list = []
        for name in original_table_names:
            table_list.append(self._load_original_table(name, year))
        for name in schema_based_table_names:
            table_list.append(self._construct_schema_based_table(name, year))
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
    add_duration: bool = False
    duration_value: int = 30
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
            raise ValueError("Classification is not valid")
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
        if self.settings.add_duration:
            duration = self._create_duration(mapping_table)
        else:
            duration = None
        mapping_table = self._rename_columns(mapping_table)
        if duration is not None:
            mapping_table["duration"] = duration
        return mapping_table

    def _apply_drop(self, mapping_table: pd.DataFrame) -> pd.DataFrame:
        if "drop" not in mapping_table.columns:
            return mapping_table
        filt = mapping_table.loc[:, ("drop", slice(None))].prod(axis="columns") == 0  # type: ignore
        mapping_table = mapping_table.loc[filt]
        mapping_table = mapping_table.drop(columns=["drop"])
        return mapping_table

    def _create_duration(self, mapping_table: pd.DataFrame) -> pd.Series:
        if not "duration" in mapping_table.columns:
            duration = pd.Series(
                self.settings.duration_value, index=mapping_table.index
            )
        else:
            duratuin_columns = mapping_table.loc[:, ("duration", slice(None))]  # type: ignore
            duration_min = duratuin_columns.min(axis="columns")
            duration_max = duratuin_columns.max(axis="columns")
            if (duration_min != duration_max).sum() > 0:
                raise ValueError(
                    "Expected duration columns to be equal for each row, but found unequal values."
                )
            duration = duration_min
        return duration

    def _rename_columns(self, mapping_table: pd.DataFrame) -> pd.DataFrame:
        levels = self.settings.levels
        levels = levels if len(levels) > 0 else list(mapping_table["level"].unique())
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

    def add_classification(self, table: pd.DataFrame, copy=False) -> pd.DataFrame:
        if copy:
            table = table.copy()
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
        year: int,
        attribute: _Attribute,
        translations: str | Iterable[str] = "names",
    ) -> None:
        self.household_metadata = utils.MetadataVersionResolver(
            metadatas.household, year
        ).get_version()
        self.attribute = attribute
        self.translations = (
            [translations] if isinstance(translations, str) else translations
        )

    def construct_mapping_table(self, table: pd.DataFrame):
        household_ids = table["ID"].drop_duplicates().copy()
        household_ids = household_ids.set_axis(household_ids)
        mappings = []
        for translation in self.translations:
            translator = self._create_code_translator(translation)
            mappings.append(translator(household_ids))
        mapping_table = pd.concat(mappings, axis="columns")
        return mapping_table

    def _create_code_translator(self, translation):
        assert isinstance(self.household_metadata, dict)
        mapping = self.household_metadata[self.attribute][translation]
        code_builder = self._create_code_builder()

        def translator(household_id_column: pd.Series) -> pd.Series:
            mapped = code_builder(household_id_column).map(mapping).astype("category")
            mapped.name = translation
            return mapped

        return translator

    def _create_code_builder(self):
        assert isinstance(self.household_metadata, dict)
        ld_len = self.household_metadata["ID_Length"]
        attr_dict = self.household_metadata[self.attribute]
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
        hh_info = TableLoader("household_information", self.year).load()
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
    table_years = (
        table[["Table_Name", "Year"]].drop_duplicates().to_records(index=False).tolist()
    )
    tables_defaults = metadatas.commodities["tables"]
    subtables = []
    for table_name, year in table_years:
        settings = _extract_default_values(table_name, tables_defaults)
        settings.update(kwargs)
        classificationer = Classification(
            classification_name=classification_name, year=year, **settings
        )
        filt = (table["Year"] == year) & (table["Table_Name"] == table_name)
        subtable = table.loc[filt].copy()
        subtable = classificationer.add_classification(subtable)
        subtables.append(subtable)
    return pd.concat(subtables, ignore_index=True)


def _extract_default_values(table_name, tables_defaults) -> dict:
    if table_name in tables_defaults:
        default_values = {
            key.split("_", 1)[1]: value
            for key, value in tables_defaults[table_name].items()
            if key.split("_", 1)[0] == "default"
        }
    else:
        default_values = {}
    return default_values


def add_attribute(
    table: pd.DataFrame, attribute_name: _Attribute, **kwargs
) -> pd.DataFrame:
    table_years = (
        table[["Table_Name", "Year"]].drop_duplicates().to_records(index=False).tolist()
    )
    subtables = []
    for table_name, year in table_years:
        attribute_reader = Attribute(year=year, attribute=attribute_name, **kwargs)
        filt = (table["Year"] == year) & (table["Table_Name"] == table_name)
        subtable = table.loc[filt].copy()
        subtable = attribute_reader.add_attribute(subtable)
        subtables.append(subtable)
    return pd.concat(subtables, ignore_index=True)


def add_weights(table: pd.DataFrame) -> pd.DataFrame:
    table_years = (
        table[["Table_Name", "Year"]].drop_duplicates().to_records(index=False).tolist()
    )
    subtables = []
    for table_name, year in table_years:
        filt = (table["Year"] == year) & (table["Table_Name"] == table_name)
        subtable = table.loc[filt].copy()
        subtable = Weight(year).add_weights(subtable)
        subtables.append(subtable)
    return pd.concat(subtables, ignore_index=True)


# def load_table(
#     table_name: _Table,
#     years: int | Iterable[int] | str | None = None,
#     **kwargs,
# ) -> pd.DataFrame:
#     """docs"""
#     if table_name in metadata.original_tables:
#         table_name_list = [table_name]
#     else:
#         table_name_list: list[_Table] = metadatas.schema[table_name]["table_list"]

#     original_kwargs = kwargs.copy()
#     for variable in ["add_year", "add_duration", "add_table_names"]:
#         if variable in kwargs:
#             continue
#         try:
#             kwargs[variable] = metadatas.schema[table_name]["settings"][variable]
#         except KeyError:
#             kwargs[variable] = True

#     sub_tables = []
#     for _table_name in table_name_list:
#         if _table_name in metadata.original_tables:
#             table = read_table(_table_name, years, **kwargs)
#         else:
#             table = load_table(_table_name, years)

#         sub_tables.append(table)
#     table = pd.concat(sub_tables, ignore_index=True)

#     if "classifications" in metadatas.schema[table_name]:
#         for classification in metadatas.schema[table_name]["classifications"]:
#             table = add_classification(table, **classification)

#     if (
#         ("add_duration" not in original_kwargs)
#         and ("Duration" in table.columns)
#         and (len(table["Duration"].unique()) < 2)
#     ):
#         table = table.drop(columns="Duration")

#     table_schema = metadatas.schema[table_name]
#     table = _imply_table_schema(table, table_schema, utils.parse_years(years)[0])
#     return table


# def read_table(
#     table_names: _OriginalTable | list[_OriginalTable] | tuple[_OriginalTable],
#     years: int | Iterable[int] | str | None = None,
#     apply_yearly_schema: bool = True,
#     add_year: bool = False,
#     add_duration: bool = False,
#     add_table_names: bool = False,
#     **kwargs,
# ) -> pd.DataFrame:
#     """
#     Load Tables
#     """
#     tname_year = utils.construct_table_year_pairs(table_names=table_names, years=years)
#     table_list: list[pd.DataFrame] = []
#     for _table_name, year in tname_year:
#         table = _get_parquet(_table_name, year, **kwargs)
#         if apply_yearly_schema:
#             try:
#                 table_schema = metadatas.schema[_table_name]["yearly_schema"]
#             except KeyError:
#                 pass
#             else:
#                 table = _imply_table_schema(table, table_schema, year)
#         if add_year:
#             table["Year"] = year
#         if add_duration:
#             table = _add_duration(table, _table_name)
#         if add_table_names:
#             table["table"] = _table_name
#         table_list.append(table)
#     concat_table = pd.concat(table_list, ignore_index=True)

#     if not add_year:
#         concat_table.attrs["year"] = utils.parse_years(years)[0]

#     return concat_table


# def _get_parquet(
#     table_name: str, year: int, download: bool = True, save: bool = True
# ) -> pd.DataFrame:
#     file_name = f"{year}_{table_name}.parquet"
#     try:
#         table = pd.read_parquet(defaults.processed_data.joinpath(file_name))
#     except FileNotFoundError as exc:
#         print(
#             f"Table {table_name} for year {year} not found at expected location: \n"
#             f"{defaults.processed_data.joinpath(file_name)}"
#         )
#         if download and save:
#             _download_parquet(table_name, year)
#             table = pd.read_parquet(defaults.processed_data.joinpath(file_name))
#         elif download:
#             table = pd.read_parquet(
#                 f"{defaults.online_dir}/parquet_files/{year}_{table_name}.parquet"
#             )
#         else:
#             raise exc
#     return table


# def _download_parquet(table_name: str, year: int) -> None:
#     file_name = f"{year}_{table_name}.parquet"
#     file_url = f"{defaults.online_dir}/parquet_files/{file_name}"
#     local_path = defaults.processed_data.joinpath(file_name)
#     utils.download(url=file_url, path=local_path, show_progress_bar=True)


# def _imply_table_schema(
#     table: pd.DataFrame, table_schema: dict, year: int | None = None
# ) -> pd.DataFrame:
#     """docs"""
#     table = table.copy()

#     if "preprocessing" in table_schema:
#         table = _preprocess_table(table, table_schema["preprocessing"])

#     if "columns" in table_schema:
#         instructions = table_schema["columns"]

#         for name, instruction in instructions.items():
#             if isinstance(year, int):
#                 instruction = metadata.get_metadata_version(instruction, year)
#             table = _apply_column_instruction(table, name, instruction)

#     if "filter" in table_schema:
#         if isinstance(table_schema["filter"], str):
#             filt = table.eval(table_schema["filter"])
#         elif isinstance(table_schema["filter"], list):
#             filts = []
#             for filt_str in table_schema["filter"]:
#                 filts.append(table.eval(filt_str))
#             filt_sum = pd.concat(filts, axis="columns").sum(axis="columns")
#             filt = filt_sum == len(table_schema["filter"])
#         else:
#             raise KeyError
#         table = table.loc[filt]

#     if "order" in table_schema:
#         column_order = table_schema["order"]
#         table = _order_columns_by_schema(table, column_order)
#     return table


# def _preprocess_table(table: pd.DataFrame, instructions: list) -> pd.DataFrame:
#     for instruction in instructions:
#         table = pd.eval(instruction, target=table)  # type: ignore
#     return table


# def _apply_column_instruction(table, name, instruction):
#     if instruction is None:
#         pass
#     elif instruction["type"] == "categorical":
#         table[name] = _apply_categorical_instruction(table, name, instruction)
#     elif instruction["type"] == "numerical":
#         table[name] = _apply_numerical_instruction(table, instruction)

#     return table


# def _apply_categorical_instruction(
#     table: pd.DataFrame, column_name: str, instruction: dict
# ) -> pd.Series:
#     categories = instruction["categories"]

#     if column_name in table.columns:
#         categorical_column = table[column_name].copy()

#     categorical_column = pd.Series(index=table.index, dtype="category")

#     for category, condition in categories.items():
#         if condition is None:
#             filt = table.index
#         elif isinstance(condition, str):
#             filt = table[column_name] == condition
#         elif isinstance(condition, list):
#             filt = table[column_name].isin(condition)
#         elif isinstance(condition, dict):
#             filts = []
#             for other_column, value in condition.items():
#                 if isinstance(value, (bool, str)):
#                     filts.append(table[other_column] == value)
#                 elif isinstance(value, list):
#                     filts.append(table[other_column].isin(value))
#                 else:
#                     raise KeyError
#             filt_sum = pd.concat(filts, axis="columns").sum(axis="columns")
#             filt = filt_sum == len(condition)
#         else:
#             raise KeyError
#         categorical_column = categorical_column.cat.add_categories([category])
#         categorical_column.loc[filt] = category

#     return categorical_column


# def _apply_numerical_instruction(table: pd.DataFrame, instruction: dict) -> pd.Series:
#     columns_names = re.split(r"[\+\-\*\/\s\.]+", instruction["expression"])
#     columns_names = [name for name in columns_names if not name.isnumeric()]
#     columns = table[columns_names].astype(float).copy()
#     expr = instruction["expression"]
#     result = columns.fillna(0).eval(expr)
#     return result


# def _order_columns_by_schema(table, column_order):
#     new_columns = [column for column in column_order if column in table.columns]
#     table = table[new_columns]
#     return table


# def _add_duration(table, table_name):
#     table = table.copy()
#     if table_name in metadata.expenditure_tables:
#         default_duration = metadatas.commodities["tables"][table_name][
#             "default_duration"
#         ]
#     else:
#         default_duration = 360
#     table["Duration"] = default_duration
#     return table


# def add_attribute(
#     table: pd.DataFrame,
#     attribute: _Attribute | list[_Attribute] | tuple[_Attribute] | None,
#     **kwargs,
# ) -> pd.DataFrame:
#     """docs"""
#     if attribute is None:
#         attribute_list = [attr for attr in get_args(_Attribute)]
#     elif isinstance(attribute, (list, tuple)):
#         attribute_list = [attr for attr in attribute]
#     else:
#         attribute_list: list[_Attribute] = [attribute]

#     table = table.copy()

#     for _attribute in attribute_list:
#         attribute_column = get_attribute(_input=table, attribute=_attribute, **kwargs)
#         table[_attribute] = attribute_column
#     return table


# def get_attribute(
#     _input: pd.DataFrame | pd.Series | pd.Index,
#     attribute: _Attribute,
#     year: int | None = None,
#     index_id: bool = False,
#     id_column_name: str = "ID",
#     year_column_name: str = "Year",
#     attribute_text="names",
# ) -> pd.Series:
#     """docs"""
#     if isinstance(_input, (pd.Series, pd.Index)):
#         if year is not None:
#             return _get_attribute_by_id(_input, year, attribute, attribute_text)
#         if (isinstance(_input, pd.Series)) and ("year" in _input.attrs):
#             assert isinstance(_input.attrs["year"], int)
#             return _get_attribute_by_id(
#                 _input, _input.attrs["year"], attribute, attribute_text
#             )
#         raise TypeError(
#             "Since the input is a Pandas series, the 'year' variable must "
#             "be specified. Please provide a year value in the format YYYY."
#         )

#     if not isinstance(_input, pd.DataFrame):
#         raise ValueError

#     _input = _input.copy()
#     years: list[int] = []
#     if year is not None:
#         years = [year]
#         _input["__Year__"] = year
#     elif year_column_name in _input.columns:
#         years = [int(_year) for _year in _input[year_column_name].unique()]
#         _input["__Year__"] = _input[year_column_name]
#     elif "year" in _input.attrs:
#         year = _input.attrs["year"]
#         assert isinstance(year, int)
#         years = [year]
#         _input["__Year__"] = year
#     else:
#         raise TypeError(
#             "DataFrame does not have a 'year' column. Please provide the "
#             "'year' column or specify a value for the 'year' variable."
#         )

#     attribute_column = pd.Series(None, dtype="object", index=_input.index)
#     for _year in years:
#         filt = _input["__Year__"] == _year
#         if index_id:
#             id_series = _input.loc[filt].index
#         else:
#             id_series = _input.loc[filt, id_column_name]
#         attribute_series = _get_attribute_by_id(
#             household_id_column=id_series,
#             attribute=attribute,
#             attribute_text=attribute_text,
#             year=_year,
#         )
#         attribute_column.loc[filt] = attribute_series

#     attribute_column = attribute_column.astype("category")
#     return attribute_column


# def _get_attribute_by_id(
#     household_id_column: pd.Series | pd.Index,
#     year: int,
#     attribute: _Attribute,
#     attribute_text="names",
# ) -> pd.Series:
#     attr_dict = metadatas.household[attribute]
#     text = metadata.get_metadata_version(attr_dict[attribute_text], year)
#     attr_codes = _get_attribute_code(household_id_column, year, attribute)
#     attr_codes = attr_codes.map(text)
#     attr_codes = attr_codes.astype("category")
#     return attr_codes


# def _get_attribute_code(
#     household_id_column: pd.Series | pd.Index,
#     year: int,
#     attribute: _Attribute,
# ) -> pd.Series:
#     id_length = metadata.get_metadata_version(metadatas.household["ID_Length"], year)
#     attr_dict = metadatas.household[attribute]
#     position = metadata.get_metadata_version(attr_dict["position"], year)
#     start, end = position["start"], position["end"]
#     attr_codes = household_id_column % pow(10, (id_length - start))
#     attr_codes = attr_codes // pow(10, (id_length - end))
#     return attr_codes


# def add_classification(
#     table: pd.DataFrame,
#     classification: str = "original",
#     level: int | list[int] | None = None,
#     year: int | None = None,
#     code_column_name: str = "Code",
#     year_column_name: str = "Year",
#     new_column_name: str | list[str] | None = None,
#     attribute: str | None = None,
#     dropna: bool = False,
# ) -> pd.DataFrame:
#     """docs"""
#     table = table.copy()

#     if level is None:
#         levels = metadatas.commodities[classification]["default_levels"]
#     elif isinstance(level, int):
#         levels = [level]
#     elif isinstance(level, list):
#         levels = level
#     else:
#         raise TypeError

#     if new_column_name is None:
#         if "default_names" in metadatas.commodities[classification]:
#             column_names = metadatas.commodities[classification]["default_names"]
#         else:
#             column_names = [f"{classification}-{_level}" for _level in levels]
#     elif isinstance(new_column_name, str):
#         column_names = [new_column_name]
#     else:
#         column_names = new_column_name

#     assert len(levels) == len(column_names)

#     level_and_name = zip(levels, column_names)

#     for _level, column_name in level_and_name:
#         classification_column = get_classification(
#             table,
#             classification=classification,
#             level=_level,
#             year=year,
#             code_column_name=code_column_name,
#             year_column_name=year_column_name,
#             attribute=attribute,
#         )
#         table[column_name] = classification_column

#     if dropna:
#         table = table.dropna(subset=column_names)

#     return table


# def get_classification(
#     _input: pd.DataFrame | pd.Series,
#     classification: str,
#     level: int,
#     year: int | None = None,
#     code_column_name: str = "Code",
#     year_column_name: str = "Year",
#     attribute: str | None = None,
# ) -> pd.Series:
#     """docs"""
#     if isinstance(_input, pd.Series):
#         if year is not None:
#             return _get_classification_by_code(
#                 _input, classification, level, year, attribute
#             )
#         if "year" in _input.attrs:
#             return _get_classification_by_code(
#                 _input, classification, level, _input.attrs["year"], attribute
#             )
#         raise TypeError(
#             "Since the input is a Pandas series, the 'year' variable must "
#             "be specified. Please provide a year value in the format YYYY."
#         )
#     if not isinstance(_input, pd.DataFrame):
#         raise ValueError

#     _input = _input.copy()
#     if year is not None:
#         years = [year]
#         _input["__Year__"] = year
#     elif year_column_name in _input.columns:
#         years = [int(y) for y in _input[year_column_name].unique()]
#         _input["__Year__"] = _input[year_column_name]
#     elif "year" in _input.attrs:
#         year = _input.attrs["year"]
#         if year is not None:
#             years = [year]
#         else:
#             raise KeyError
#         _input["__Year__"] = year
#     else:
#         raise TypeError(
#             "DataFrame does not have a 'year' column. Please provide the "
#             "'year' column or specify a value for the 'year' variable."
#         )

#     classification_column = pd.Series(None, dtype="object", index=_input.index)
#     for _year in years:
#         filt = _input["__Year__"] == _year
#         code_series = _input.loc[filt, code_column_name]
#         classification_series = _get_classification_by_code(
#             commodity_code_column=code_series,
#             classification=classification,
#             level=level,
#             year=_year,
#             attribute=attribute,
#         )
#         classification_column.loc[filt] = classification_series

#     classification_column = classification_column.astype("category")
#     return classification_column


# def _get_classification_by_code(
#     commodity_code_column: pd.Series,
#     classification: str,
#     level: int,
#     year: int,
#     attribute: str | None = None,
# ) -> pd.Series:
#     translator = _build_translator(
#         classification=classification, level=level, year=year, attribute=attribute
#     )
#     classification_column = commodity_code_column.map(translator)
#     classification_column = classification_column.astype("category")
#     return classification_column


# def _build_translator(
#     classification: str,
#     level: int,
#     year: int,
#     attribute: str | None = None,
#     default_value: str | None = None,
# ) -> dict:
#     def closure(_input):
#         def inner_function():
#             return _input

#         return inner_function

#     commodity_codes = metadatas.commodities[classification]["items"]
#     commodity_codes = metadata.get_metadata_version(commodity_codes, year)
#     selected_items = {
#         name: info for name, info in commodity_codes.items() if info["level"] == level
#     }
#     translator = {}
#     if attribute is None:
#         for name, info in selected_items.items():
#             categories = metadata.get_categories(info)
#             for category_info in categories:
#                 if "default" in category_info:
#                     translator = defaultdict(closure(name), translator)
#                     break
#                 code_range = _get_code_range(category_info["code"])
#                 for code in code_range:
#                     translator[code] = name
#     else:
#         for info in selected_items.values():
#             categories = metadata.get_categories(info)
#             for category_info in categories:
#                 try:
#                     attribute_value = category_info[attribute]
#                 except KeyError:
#                     attribute_value = default_value
#                 if "default" in category_info:
#                     translator = defaultdict(closure(attribute_value), translator)
#                     break
#                 code_range = _get_code_range(category_info["code"])
#                 for code in code_range:
#                     translator[code] = attribute_value

#     return translator


# def _get_code_range(code_range_info: int | dict | list) -> list[int]:
#     if isinstance(code_range_info, int):
#         code_range = [code_range_info]
#     elif isinstance(code_range_info, dict):
#         if ("start" in code_range_info) and ("end" in code_range_info):
#             code_range = list(range(code_range_info["start"], code_range_info["end"]))
#         elif "code" in code_range_info:
#             code_range = _get_code_range(code_range_info["code"])
#         else:
#             raise KeyError
#     elif isinstance(code_range_info, list):
#         code_range = []
#         for element in code_range_info:
#             code_range.extend(_get_code_range(element))
#     else:
#         raise KeyError

#     return code_range


# def add_weights(table: pd.DataFrame, year: int | None = None, **kwargs) -> pd.DataFrame:
#     """Add weight column to dataframe"""
#     _input = table if year is None else year
#     weights = get_weights(_input, **kwargs)
#     table["Weights"] = weights
#     return table


# def get_weights(
#     _input: int | list[int] | pd.DataFrame,
#     method: Literal["default", "external", "household_info"] = "default",
# ) -> pd.Series:
#     """Return wight column for specified year(s) or dataframe"""
#     years: list[int]
#     if isinstance(_input, int):
#         years = [_input]
#     elif isinstance(_input, pd.DataFrame):
#         years = list(_input["Year"].unique())
#     elif isinstance(_input, list):
#         years = _input

#     if method == "external":
#         weights = _get_weights_from_external_data(years)
#     elif method == "household_info":
#         weights = _get_weights_from_household_info(years)
#     elif method == "default":
#         before1395 = [year for year in years if year <= 1395]
#         after1395 = [year for year in years if year > 1395]

#         weight_list = []
#         if len(before1395) > 0:
#             weight_list.append(_get_weights_from_external_data(before1395))
#         if len(after1395) > 0:
#             weight_list.append(_get_weights_from_household_info(after1395))
#         weights = pd.concat(weight_list, axis="index")
#         assert isinstance(weights, pd.Series)
#     else:
#         raise KeyError
#     weights.index = weights.index.set_levels(  # type: ignore
#         [weights.index.levels[0].astype(int), weights.index.levels[1]]  # type: ignore
#     )
#     return weights


# def _get_weights_from_household_info(years: int | list[int]) -> pd.Series:
#     if isinstance(years, int):
#         years = [years]
#     weight_list = []
#     for year in years:
#         hh_info = TableLoader("household_information", year).load()
#         hh_info = hh_info.set_index("ID")
#         weight_list.append(hh_info["Weight"])
#     weights = pd.concat(weight_list, axis="index", keys=years, names=["Year", "ID"])
#     assert isinstance(weights, pd.Series)
#     return weights


# def _get_weights_from_external_data(years: int | list[int]) -> pd.Series:
#     if isinstance(years, int):
#         years = [years]
#     weights_path = defaults.external_data.joinpath("weights.parquet")
#     if not weights_path.exists():
#         defaults.external_data.mkdir(parents=True, exist_ok=True)
#         utils.download(
#             f"{defaults.online_dir}/external_data/weights.parquet", weights_path
#         )
#     weights = pd.read_parquet(weights_path)
#     weights = weights.loc[(years), "Weight"]
#     return weights
