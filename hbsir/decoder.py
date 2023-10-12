from itertools import product
from typing import Callable

import pandas as pd
from pydantic import BaseModel

from . import utils
from .metadata_reader import metadata, defaults, _Attribute


def read_classification_info(name: str, year: int) -> dict:
    """Read classification metadata for a classification scheme.

    Parameters
    ----------
    name: str
        Name of the classification scheme (e.g. 'original', 'coicop').
    year: int
        Year for which to read classification metadata.

    Returns
    -------
    dict: Classification metadata dictionary containing information
        for the given classification scheme and year.
    """
    versioned_info = metadata.commodities[name]
    category_resolver = utils.MetadataCategoryResolver(versioned_info, year)
    classification_info = category_resolver.categorize_metadata()
    return classification_info


def create_classification_table(name, years) -> pd.DataFrame:
    table_list = []
    for year in years:
        classification_info = read_classification_info(name, year)
        annual_table = _create_annual_classification_table(classification_info)
        # pylint: disable=unsupported-assignment-operation
        annual_table["Year"] = year
        table_list.append(annual_table)
    table = pd.concat(table_list, ignore_index=True)
    return table


def _create_annual_classification_table(classification_info) -> pd.DataFrame:
    # pylint: disable=unsubscriptable-object
    table = pd.DataFrame(classification_info["items"])
    table["code_range"] = table["code"].apply(
        utils.Argham,  # type: ignore
        default_start=defaults.first_year,
        default_end=defaults.last_year + 1,
        keywords=["code"],
    )
    table = table.drop(columns=["code"])
    return table


def extract_column(table: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name in table.columns:
        column = table.loc[:, column_name].copy()
    elif isinstance(table.index, pd.Index) and table.index.name == column_name:
        column = table.index.to_series()
    elif isinstance(table.index, pd.MultiIndex) and column_name in table.index.names:
        column = table.index.to_frame().loc[:, column_name].copy()
    else:
        raise KeyError
    return column


class CommodityDecoderSettings(BaseModel):
    name: str = "original"
    code_column_name: str = "Code"
    year_column_name: str = "Year"
    versioned_info: dict = {}
    defaults: dict = {}
    labels: tuple[str, ...] = ()
    levels: tuple[int, ...] = ()
    drop_value: bool = False
    output_column_names: tuple[str, ...] = ()
    required_columns: tuple[str, ...] | None = None
    missing_value_replacements: dict[str, str] | None = None

    def model_post_init(self, __contex=None) -> None:
        self.versioned_info = metadata.commodities[self.name]
        if "defaults" in self.versioned_info:
            self.defaults = self.versioned_info["defaults"]
        for key, value in self.defaults.items():
            if isinstance(value, list):
                value = tuple(value)
            if (getattr(self, key) is None) or (len(getattr(self, key)) == 0):
                setattr(self, key, value)
        if len(self.labels) == 0:
            self.labels = ("item_key",)
        if len(self.levels) == 0:
            self.levels = (1,)
        self._resolve_column_names()
        super().model_post_init(None)

    def _resolve_column_names(self) -> None:
        if len(self.output_column_names) == 0:
            names = [
                f"{label}_{level}" for label, level in product(self.labels, self.levels)
            ]
            self.output_column_names = tuple(names)
        elif len(self.output_column_names) == len(self.labels) * len(self.levels):
            pass
        elif len(self.output_column_names) == len(self.labels):
            names = [
                f"{label}_{level}"
                for label, level in product(self.output_column_names, self.levels)
            ]
            self.output_column_names = tuple(names)

    @property
    def rename_dict(self):
        return dict(zip(product(self.labels, self.levels), self.output_column_names))


class CommodityDecoder:
    def __init__(self, table: pd.DataFrame, settings: CommodityDecoderSettings) -> None:
        self.table = table
        self.settings = settings
        self.code_column = extract_column(table, settings.code_column_name)
        self.year_column = extract_column(table, settings.year_column_name)
        self.classification_table = create_classification_table(
            name=self.settings.name,
            years=self.year_column.drop_duplicates().to_list(),
        )
        self.year_code_pairs = self.create_year_code_pairs()

    def create_year_code_pairs(self) -> pd.DataFrame:
        years = self.year_column.drop_duplicates()
        yc_pair_list = []
        for year in years:
            filt = self.year_column == year
            codes = self.code_column.loc[filt].drop_duplicates()
            yc_pair = codes.to_frame()
            yc_pair["Year"] = year
            yc_pair_list.append(yc_pair)
        return pd.concat(yc_pair_list, ignore_index=True)

    @staticmethod
    def _build_year_code_table(
        year_code_pairs: pd.DataFrame, row: pd.Series
    ) -> pd.DataFrame:
        filt = year_code_pairs["Code"].apply(lambda x: x in row["code_range"])
        filt = filt & (year_code_pairs["Year"] == row["Year"])
        matched_codes = year_code_pairs.loc[filt].set_index(["Year", "Code"])
        columns = row.drop(["code_range", "Year"]).index
        code_table = pd.DataFrame(
            data=[row.loc[columns]] * len(matched_codes.index),
            index=matched_codes.index,
            columns=columns,
        )
        return code_table

    def create_mapping_table(self) -> pd.DataFrame:
        code_table_list = []
        for _, row in self.classification_table.iterrows():
            code_table = self._build_year_code_table(self.year_code_pairs, row)
            if not code_table.empty:
                code_table_list.append(code_table)
        mapping_table = pd.concat(code_table_list)
        mapping_table = mapping_table.set_index("level", append=True)
        self._validate_mapping_table(mapping_table)
        mapping_table = mapping_table.unstack(-1)
        mapping_table = mapping_table.loc[:, self.settings.rename_dict.keys()]  # type: ignore
        mapping_table.columns = self.settings.rename_dict.values()
        return mapping_table

    @staticmethod
    def _validate_mapping_table(mapping_table: pd.DataFrame):
        filt = mapping_table.index.duplicated(keep=False)
        invalid_case_sample = (
            mapping_table.loc[filt].sort_values(["Code", "level"]).head(10)
        )
        if filt.sum() > 0:
            raise ValueError(f"Classification is not valid \n{invalid_case_sample}")

    def _fill_missing_values(self):
        if "missing_value_replacements" not in self.settings.defaults:
            return
        for column, default in self.settings.defaults[
            "missing_value_replacements"
        ].items():
            if column not in self.table.columns:
                continue
            filt = self.table.loc[:, column].isna()
            self.table.loc[filt, column] = default  # type: ignore

    def add_classification(self):
        mapping = self.create_mapping_table()
        self.table = self.table.join(mapping, on=["Year", "Code"])
        self._fill_missing_values()
        return self.table


class IDDecoderSettings(BaseModel):
    name: _Attribute
    id_column_name: str = "ID"
    year_column_name: str = "Year"
    labels: tuple[str, ...] = ("names",)
    output_column_names: tuple[str, ...] = tuple()

    def model_post_init(self, __contex=None) -> None:
        self._resolve_output_column_names()
        super().model_post_init(None)

    def _resolve_output_column_names(self) -> None:
        if len(self.output_column_names) != len(self.labels):
            if len(self.labels) == 1:
                names = [self.name]
            else:
                names = [f"{self.name}_{label}" for label in self.labels]
            self.output_column_names = tuple(names)


class IDDecoder:
    def __init__(
        self,
        table: pd.DataFrame,
        settings: IDDecoderSettings,
    ) -> None:
        self.table = table
        self.settings = settings
        self.id_column = extract_column(table, settings.id_column_name)
        self.year_column = extract_column(table, settings.year_column_name)

    def construct_mapping_table(self):
        mapped_columns = [self.year_column, self.id_column]
        for label in self.settings.labels:
            mapped_column = self.map_id_to_label(label)
            mapped_columns.append(mapped_column)
        year_and_id = [self.settings.year_column_name, self.settings.id_column_name]
        columns = year_and_id + list(self.settings.output_column_names)
        mapping_table = pd.concat(mapped_columns, axis="columns", keys=columns)
        mapping_table = mapping_table.drop_duplicates().set_index(year_and_id)
        return mapping_table

    def _create_code_builder(
        self, household_metadata: dict
    ) -> Callable[[pd.Series], pd.Series]:
        ld_len = household_metadata["ID_Length"]
        attr_dict = household_metadata[self.settings.name]["code"]

        if ("position" in attr_dict) and attr_dict["position"] is not None:
            start, end = attr_dict["position"]["start"], attr_dict["position"]["end"]

            def builder(household_id_column: pd.Series) -> pd.Series:
                return (
                    household_id_column
                    % pow(10, (ld_len - start))
                    // pow(10, (ld_len - end))
                )

        elif "external_file" in attr_dict:
            file_name = f"{attr_dict['external_file']}.parquet"
            file_path = defaults.external_data.joinpath(file_name)
            if not file_path.exists():
                defaults.external_data.mkdir(parents=True, exist_ok=True)
                file_address = f"{defaults.online_dir}/external_data/{file_name}"
                utils.download(file_address, file_path)
            code_builer_file = pd.read_parquet(file_path)
            code_series = code_builer_file.loc[household_metadata["year"]].iloc[:, 0]
            assert isinstance(code_series, pd.Series)
            mapping_dict = code_series.to_dict()

            def builder(household_id_column: pd.Series) -> pd.Series:
                codes = household_id_column.map(mapping_dict)
                assert codes.isna().sum() == 0
                return codes

        else:
            raise ValueError("Code position is not available")

        return builder

    def _create_code_mapper(
        self, label: str, year: int
    ) -> Callable[[pd.Series], pd.Series]:
        household_metadata = utils.resolve_metadata(metadata.household, year)

        if label == "code":
            return self._create_code_builder(household_metadata)

        if not isinstance(household_metadata, dict):
            raise ValueError
        # pylint: disable=unsubscriptable-object
        mapping = household_metadata[self.settings.name][label]
        code_builder = self._create_code_builder(household_metadata)

        def mapper(household_id_column: pd.Series) -> pd.Series:
            mapped = code_builder(household_id_column).map(mapping).astype("category")
            mapped.name = label
            return mapped

        return mapper

    def map_id_to_label(self, label):
        years = self.year_column.drop_duplicates()
        attribute_column = pd.Series(index=self.table.index, dtype="object")
        for year in years:
            filt = self.year_column == year
            attribute_column.loc[filt] = self._create_code_mapper(label, year)(
                self.id_column.loc[filt]
            )
        return attribute_column

    def add_attribute(self):
        mapping_table = self.construct_mapping_table()
        year_and_id = [self.settings.year_column_name, self.settings.id_column_name]
        self.table = self.table.join(mapping_table, year_and_id)
        return self.table
