from itertools import product

import pandas as pd
from pydantic import BaseModel, Field

from . import utils
from .metadata_reader import metadata, defaults


def read_classification_info(name, year):
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
    classification_name: str = Field(default="original", alias="name")
    code_column_name: str = "Code"
    year_column_name: str = "Year"
    versioned_info: dict = {}
    defaults: dict = {}
    labels: tuple[str, ...] = tuple()
    levels: tuple[int, ...] = tuple()
    drop_value: bool = False
    column_names: tuple[str, ...] | None = None
    required_columns: tuple[str, ...] | None = None
    missing_value_replacements: dict[str, str] | None = None

    def model_post_init(self, __contex=None) -> None:
        self.versioned_info = metadata.commodities[self.classification_name]
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
        if self.column_names is None:
            names = [
                f"{label}_{level}" for label, level in product(self.labels, self.levels)
            ]
            self.column_names = tuple(names)

    @property
    def rename_dict(self):
        return dict(zip(product(self.labels, self.levels), self.column_names))  # type: ignore


class CommodityDecoder:
    def __init__(self, table: pd.DataFrame, settings: CommodityDecoderSettings) -> None:
        self.table = table
        self.settings = settings
        self.code_column = extract_column(table, settings.code_column_name)
        self.year_column = extract_column(table, settings.year_column_name)
        self.classification_table = create_classification_table(
            name=self.settings.classification_name,
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
        code_table = pd.DataFrame(index=matched_codes.index, columns=columns)
        code_table[columns] = row.loc[columns]
        return code_table

    def create_mapping_table(self) -> pd.DataFrame:
        code_table_list = []
        for _, row in self.classification_table.iterrows():
            code_table = self._build_year_code_table(self.year_code_pairs, row)
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
