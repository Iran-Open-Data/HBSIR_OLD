"""Main module for loading and transforming HBSIR data.

This module provides the central interfaces for loading raw Iranian 
household data tables, transforming them, and constructing cleaned
derivative tables for analysis.

Key functions:

- extract_dependencies - Get dependencies for building a table 
- TableHandler - Loads multiple dependency tables
- Pipeline - Applies a sequence of transform steps to a table
- TableFactory - Loads and builds tables from different sources
- create_table - Constructs a table by loading multiple years  
- load_weights - Loads sample weights for a given year
- add_weights - Adds weights to a table

The module focuses on ETL (Extract, Transform, Load) functions to go 
from raw provided data tables to cleaned analytic tables.

Relies on metadata schema and configuration for how to process tables.

"""

import re
from typing import Literal, Iterable
from types import ModuleType
import importlib
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from pydantic import BaseModel
import yaml

from . import decoder, metadata_reader

from .. import utils, external_data
from .metadata_reader import (
    defaults,
    metadata,
    original_tables,
    _Years,
    _OriginalTable,
)
from .data_cleaner import open_and_clean_table


def extract_dependencies(table_name: str, year: int) -> dict:
    """Extract the dependencies of a table based on the metadata schema.

    For the given table name and year, traverses the schema metadata to find
    all upstream dependencies that are required to construct the table.

    Recursively extracts dependencies of dependencies until only base tables
    remain. Base tables have their file size stored instead of further dependencies.

    Parameters
    ----------
    table_name : str
        Name of the target table to extract dependencies for

    year : int
        Year to extract schema dependencies for

    Returns
    -------
    dependencies : dict
        Dictionary with dependencies in the format:
        {table_name: {"dependencies": {dep1: {}, dep2: {}}},
         table_name2: {"size": 1024}}
    """
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
        elif table in original_tables:
            file_name = f"{year}_{table}.parquet"
            local_path = defaults.processed_data.joinpath(file_name)
            size = local_path.stat().st_size if local_path.exists() else None
            dependencies[table] = {"size": size}
        else:
            raise ValueError
    return dependencies


class LoadTableSettings(BaseModel):
    dataset: Literal["processed", "cleaned", "original"] = metadata_reader.settings[
        ("functions_defaults", "load_table", "dataset")
    ]
    on_missing: Literal["error", "download", "create"] = metadata_reader.settings[
        ("functions_defaults", "load_table", "on_missing")
    ]
    save_downloaded: bool = metadata_reader.settings[
        ("functions_defaults", "load_table", "save_downloaded")
    ]
    redownload: bool = metadata_reader.settings[
        ("functions_defaults", "load_table", "redownload")
    ]
    save_created: bool = metadata_reader.settings[
        ("functions_defaults", "load_table", "save_created")
    ]
    recreate: bool = metadata_reader.settings[
        ("functions_defaults", "load_table", "recreate")
    ]


class TableHandler:
    """Handles loading multiple tables from parquet.

    Loads a set of tables by reading from local parquet files or downloading
    or generating if missing. Provides access to the tables via indexing.

    Attributes
    ----------
    table_list : list of str
        List of table names to load
    year : int
        Year for tables
    settings : LoadTable
        Settings for how to load the tables
    tables : dict of DataFrames
        Loaded tables keyed by table name

    """

    def __init__(
        self,
        table_list: Iterable[_OriginalTable],
        year: int,
        settings: LoadTableSettings | None = None,
    ) -> None:
        self.table_list = table_list
        self.year = year
        self.settings = settings if settings is not None else LoadTableSettings()
        self.tables: dict[str, pd.DataFrame] = self.setup()

    def __getitem__(self, table_name: _OriginalTable) -> pd.DataFrame:
        """Get a table by name.

        Parameters
        ----------
        table_name : str
            Name of table to retrieve

        Returns
        -------
        table : DataFrame
            Table loaded for the given name

        """
        return self.tables[table_name]

    def get(
        self, names: _OriginalTable | Iterable[_OriginalTable]
    ) -> list[pd.DataFrame]:
        """Get multiple tables by name.

        Parameters
        ----------
        names : str or list of str
            Name(s) of tables to retrieve

        Returns
        -------
        tables : list of DataFrames
            Requested tables loaded

        """
        names = [names] if isinstance(names, str) else names
        return [self[name] for name in names]

    def setup(self) -> dict[str, pd.DataFrame]:
        """Set up the handler by loading all tables.

        Loads all of the configured tables in parallel using a
        ThreadPoolExecutor.

        Returns
        -------
        tables : dict of DataFrames
            Dictionary of the loaded tables by name.

        """
        with ThreadPoolExecutor(max_workers=6) as executer:
            tables = zip(
                self.table_list, executer.map(self.read_table, self.table_list)
            )
        return dict(tables)

    def read_table(self, table_name: _OriginalTable) -> pd.DataFrame:
        """Read a single table by name.

        Parameters
        ----------
        table_name : str
            Name of the table to load

        Returns
        -------
        table : DataFrame
            Loaded table data

        """
        file_name = f"{self.year}_{table_name}.parquet"
        local_file = defaults.processed_data.joinpath(file_name)

        if self.settings.recreate:
            table = self._create_table(table_name)
        elif self.settings.redownload:
            table = self._download_table(table_name)
        elif local_file.exists():
            table = pd.read_parquet(local_file)
        elif self.settings.on_missing == "create":
            table = self._create_table(table_name)
        elif self.settings.on_missing == "download":
            table = self._download_table(table_name)
        else:
            raise FileNotFoundError

        table.attrs["table_name"] = table_name
        table.attrs["year"] = self.year
        return table

    def _create_table(self, table_name: _OriginalTable) -> pd.DataFrame:
        file_name = f"{self.year}_{table_name}.parquet"
        local_path = defaults.processed_data.joinpath(file_name)
        table = open_and_clean_table(table_name, self.year)
        if self.settings.save_created:
            table.to_parquet(local_path)
        return table

    def _download_table(self, table_name: _OriginalTable) -> pd.DataFrame:
        file_name = f"{self.year}_{table_name}.parquet"
        local_path = defaults.processed_data.joinpath(file_name)
        file_url = f"{defaults.online_dir}/parquet_files/{file_name}"
        table = pd.read_parquet(file_url)
        if self.settings.save_downloaded:
            table.to_parquet(local_path)
        return table


class Pipeline:
    """Applies a sequence of transformation steps to a DataFrame.

    This class allows chaining together a set of predefined steps
    for cleaning, transforming, and processing a DataFrame representing
    a table of data. The steps are configured by passing a list of
    operations which are applied in sequence.

    Attributes
    ----------
    table : DataFrame
        The input DataFrame that steps are applied to
    steps : list
        The sequence of step functions to apply
    properties : dict
        Additional properties passed to steps

    """

    def __init__(
        self, table: pd.DataFrame, steps: list, table_name: str, year: int
    ) -> None:
        self.table = table
        self.table_name = table_name
        self.year = year
        self.steps = steps
        self.modules: dict[str, ModuleType] = {}

    def run(self) -> pd.DataFrame:
        """Run the pipeline on the table.

        Iterates through the step functions in the pipeline
        and applies them sequentially to transform the table.

        Returns
        -------
        table : DataFrame
            The transformed table after applying all steps.
        """
        for step in self.steps:
            if step is None:
                continue
            method_name, method_input = self._extract_method_name(step)
            if method_input is None:
                getattr(self, f"_{method_name}")()
            else:
                getattr(self, f"_{method_name}")(method_input)
        return self.table

    def _extract_method_name(self, instruction):
        if isinstance(instruction, str):
            method_name = instruction
            method_input = None
        elif isinstance(instruction, dict):
            method_name, method_input = list(instruction.items())[0]
        else:
            raise TypeError
        return method_name, method_input

    def _add_year(self) -> None:
        self.table["Year"] = self.year

    def _add_table_name(self) -> None:
        self.table["Table_Name"] = self.table_name

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
        other_table = create_table(table_name, years)
        self.table = self.table.merge(other_table, on=columns)


class TableFactory:
    """Builds DataFrames representing tables of data.

    This class handles loading or constructing DataFrames representing
    different tables of data. It builds tables either from original
    source parquet files, by querying cached results, or by dynamically
    constructing the table from other tables based on a schema.

    """

    def __init__(
        self,
        table_name: str,
        year: int,
        settings: LoadTableSettings | None = None,
    ):
        self.table_name = table_name
        self.year = year
        self.settings = settings if settings is not None else LoadTableSettings()
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
        self.table_handler = TableHandler(dependencies, year, settings)

    def load(self, table_name: str | None = None) -> pd.DataFrame:
        """Load the table.

        Builds the table according to the configured settings.
        Will attempt to read from cache first before building
        dynamically from a schema.

        Parameters
        ----------
        table_name : str, optional
            Table to load, will use instance table_name if not specified

        Returns
        -------
        table : DataFrame
            The loaded table data

        """
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
        """Read a cached table if dependencies are unchanged.

        Checks that the dependencies of the cached table match the
        current dependencies before reading from the cached parquet file.

        Raises FileNotFoundError if dependencies have changed.

        Parameters
        ----------
        table_name : str
            Name of table to read from cache

        Returns
        -------
        table : DataFrame
            The cached table data

        Raises
        ------
        FileNotFoundError
            If cached dependencies are out of date

        """
        if not self.check_table_dependencies(table_name):
            raise FileNotFoundError
        file_name = f"{table_name}_{self.year}.parquet"
        file_path = defaults.cached_data.joinpath(file_name)
        table = pd.read_parquet(file_path)
        return table

    def check_table_dependencies(self, table_name: str) -> bool:
        """Check if cached dependencies match current dependencies.

        Compares the dependencies recorded in the cache metadata file
        to the currently extracted dependencies for the table.

        Parameters
        ----------
        table_name : str
            Table name to check dependencies for

        Returns
        -------
        match : bool
            True if dependencies match, False otherwise

        """
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
        """Save table to cache along with metadata.

        Saves the table to a parquet file and saves metadata about
        dependencies to a yaml file.

        Parameters
        ----------
        table : DataFrame
            Table data to cache
        table_name : str
            Name of table being cached

        """
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

        steps = self.schema[table_name]["instructions"]
        assert isinstance(steps, list)
        table = Pipeline(
            table=table, steps=steps, table_name=table_name, year=self.year
        ).run()
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
        table_list = [self.load(name) for name in table_names]
        table_list = [table for table in table_list if not table.empty]
        return table_list


def create_table(
    table_name: str,
    years: _Years,
    settings: LoadTableSettings | None = None,
) -> pd.DataFrame:
    """Construct a table by loading it for multiple years.

    Loads the specified table for each year in the provided
    range of years. Concatenates the individual tables into
    one table indexed by year.

    Parameters
    ----------
    table_name : str
        Name of table to load
    years : int or list of int
        Years to load table for
    settings : LoadTable, optional
        Settings for how to load each table

    Returns
    -------
    table : DataFrame
        Table concatenated across specified years

    """
    table_list = []
    for year in utils.parse_years(years):
        table = TableFactory(table_name, year, settings).load()
        table_list.append(table)
    table = pd.concat(table_list)
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

        "default": Use "external" for years <= 1395, "household_info"
        for later years

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
            create_table("Number_of_Members", years=year)
            .set_index("ID")
            .loc[:, "Members"]
        )
        weights, members = weights.align(members, join="left")
        weights = weights.mul(members)

    weights = weights.rename(defaults.columns.weight)
    return weights


def _load_from_household_info(year) -> pd.Series:
    loader = TableFactory("household_information", year)
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
