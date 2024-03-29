from typing import Literal, Iterable

import pandas as pd
from pydantic import BaseModel, Field

from .. import api
from ..core.metadata_reader import _Attribute, _Table, defaults
from .. import utils


_QuantileBase = Literal[
    "Income",
    "Expenditure",
    "Gross_Expenditure",
    "Net_Expenditure",
]


_EquivalenceScale = Literal[
    "Household", "Per_Capita", "OECD", "OECD_Modified", "Square_Root"
]


class QuantileSettings(BaseModel):
    on_variable: _QuantileBase | None = Field(default=None, alias="on")
    on_column: str | None = None
    weighted: bool = True
    adjust_weight_for_household_size: bool = False
    weight_column: str = defaults.columns.weight
    equivalence_scale: _EquivalenceScale = "Household"
    for_all: bool = True
    annual: bool = True
    groupby: _Attribute | Iterable[_Attribute] | None = None
    years: int | Iterable[int] | str | None = None

    def model_post_init(self, __context=None) -> None:
        if self.groupby is None:
            self.groupby = []
        if isinstance(self.groupby, str):
            self.groupby = [self.groupby]
        else:
            self.groupby = list(self.groupby)

        super().model_post_init(__context)


class Quantiler:
    """
    Quantiler Class
    """

    variable_aliases = {
        "Expenditure": "Gross_Expenditure",
    }

    variable_tables: dict[str, _Table] = {
        "Income": "Total_Income",
        "Gross_Expenditure": "Total_Expenditure",
        "Net_Expenditure": "Total_Expenditure",
    }

    def __init__(
        self,
        table: pd.DataFrame | pd.Series | None = None,
        settings: QuantileSettings | None = None,
    ) -> None:
        if settings is None:
            self.settings = QuantileSettings()
        else:
            self.settings = settings.model_copy()

        self.table = table
        self.years = self._find_years()

        if (self.settings.on_column is not None) and (self.table is not None):
            self.value_table = self._extract_value_table(self.table)
        elif self.settings.on_variable is not None:
            self.value_table = self._get_external_value_table(self.settings.on_variable)
        else:
            raise ValueError

    def _find_years(self) -> list[int]:
        if self.settings.years is not None:
            return utils.parse_years(self.settings.years)
        if self.table is None:
            raise ValueError("Year is Not available")
        if "Year" in self.table.index.names:
            return self.table.index.get_level_values("Year").unique().to_list()
        if "Year" in self.table.columns:
            return list(self.table["Year"].unique())
        raise ValueError("Year must be provided")

    def _extract_value_table(self, table: pd.DataFrame | pd.Series) -> pd.DataFrame:
        if isinstance(table, pd.Series):
            table = table.to_frame(name="Values")
            assert ("Year" in table.index.names) and ("ID" in table.index.names)
        if isinstance(table, pd.DataFrame):
            table = table.reset_index().set_index(["Year", "ID"])
            if len(table.columns) > 1:
                if self.settings.on_column is None:
                    raise ValueError("Column name must be provided")
                assert self.settings.on_column in table.columns
                table = table[[self.settings.on_column]]
            table.columns = ["Values"]
        return table

    def _get_external_value_table(self, variable) -> pd.DataFrame:
        variable = self.variable_aliases.get(variable, variable)
        table_name = self.variable_tables[variable]
        value_table = (
            api.load_table(table_name, self.years)
            .set_index(["Year", "ID"])[[variable]]
            .rename(columns={variable: "Values"})
        )
        if (not self.settings.for_all) and (self.table is not None):
            value_table = value_table.loc[self.table.set_index(["Year", "ID"]).index]
        return value_table

    def calculate_simple_quantile(self) -> pd.Series:
        groupby_columns: list = self.settings.groupby.copy()  # type: ignore
        if self.settings.annual:
            groupby_columns.append("Year")
        quantile = (
            self.value_table.dropna()
            .sort_values("Values")
            .pipe(self._add_attributes)
            .pipe(self._add_weights)
            .groupby(groupby_columns)
            .apply(self._calculate_subgroup_quantile)
        )
        if isinstance(quantile, pd.Series):
            quantile.index = pd.MultiIndex.from_arrays(
                [
                    quantile.index.get_level_values(-2),
                    quantile.index.get_level_values(-1),
                ]
            )
        else:
            quantile = quantile.iloc[0].rename("Quantile")
        return quantile

    def _calculate_subgroup_quantile(self, subgroup: pd.DataFrame) -> pd.Series:
        return subgroup.assign(
            CumWeight=lambda df: df[self.settings.weight_column].cumsum(),
            Quantile=lambda df: df["CumWeight"] / df["CumWeight"].iloc[-1],
        ).loc[:, "Quantile"]

    def _add_weights(self, table: pd.DataFrame) -> pd.DataFrame:
        if self.settings.weighted:
            return api.add_weight(table, self.settings.adjust_weight_for_household_size)
        return table.assign(Weight=1)

    def _add_attributes(self, table: pd.DataFrame) -> pd.DataFrame:
        for attribute in self.settings.groupby:  # type: ignore
            table = api.add_attribute(table, attribute)  # type: ignore
        return table

    def calculate_quantile(self) -> pd.Series:
        equivalence_scale = (
            api.load_table("Equivalence_Scale", years=self.years)
            .set_index(["Year", "ID"])
            .loc[:, self.settings.equivalence_scale]
        )
        new_values = self.value_table["Values"].div(equivalence_scale)
        self.value_table.loc[:, "Values"] = new_values
        quantile = self.calculate_simple_quantile()
        if self.table is not None:
            _, quantile = self.value_table.align(quantile, join="left", axis="index")
        return quantile


# pylint: disable=too-many-arguments
# pylint: disable=unused-argument
def calculate_quantile(
    *,
    table: pd.DataFrame | pd.Series | None = None,
    quantile_column_name: str = "Quantile",
    bins: int = -1,
    on: _QuantileBase | None = "Gross_Expenditure",
    on_column: str | None = None,
    weighted: bool = True,
    adjust_weight_for_household_size: bool = False,
    weight_column: str = defaults.columns.weight,
    equivalence_scale: _EquivalenceScale = "Household",
    for_all: bool = True,
    annual: bool = True,
    groupby: _Attribute | Iterable[_Attribute] | None = None,
    years: int | Iterable[int] | str | None = None,
):
    settings_vars = {key: value for key, value in locals().items() if key != "table"}
    settings = QuantileSettings(**settings_vars)
    quantile = Quantiler(table=table, settings=settings).calculate_quantile()
    quantile = quantile.rename(quantile_column_name)
    if bins > 0:
        quantile = (
            quantile.multiply(bins)
            .floordiv(1)
            .add(1)
            .clip(1, bins)
            .astype(int)
            .rename(quantile_column_name)
        )
    return quantile


def add_quantile(
    table: pd.DataFrame,
    quantile_column_name: str = "Quantile",
    *,
    on: _QuantileBase | None = "Gross_Expenditure",
    on_column: str | None = None,
    weighted: bool = True,
    adjust_weight_for_household_size: bool = False,
    weight_column: str = defaults.columns.weight,
    equivalence_scale: _EquivalenceScale = "Household",
    for_all: bool = True,
    annual: bool = True,
    groupby: _Attribute | Iterable[_Attribute] | None = None,
    years: int | Iterable[int] | str | None = None,
) -> pd.DataFrame:
    quantile = calculate_quantile(**locals())
    quantile.index = table.index
    table[quantile_column_name] = quantile
    return table


def add_decile(
    table: pd.DataFrame,
    quantile_column_name: str = "Decile",
    *,
    on: _QuantileBase | None = "Gross_Expenditure",
    on_column: str | None = None,
    weighted: bool = True,
    adjust_weight_for_household_size: bool = False,
    weight_column: str = defaults.columns.weight,
    equivalence_scale: _EquivalenceScale = "Household",
    for_all: bool = True,
    annual: bool = True,
    groupby: _Attribute | Iterable[_Attribute] | None = None,
    years: int | Iterable[int] | str | None = None,
) -> pd.DataFrame:
    setting_vars = locals()
    setting_vars.update({"bins": 10})
    quantile = calculate_quantile(**setting_vars)
    quantile.index = table.index
    table[quantile_column_name] = quantile
    return table


def add_percentile(
    table: pd.DataFrame,
    quantile_column_name: str = "Percentile",
    *,
    on: _QuantileBase | None = "Gross_Expenditure",
    on_column: str | None = None,
    weighted: bool = True,
    adjust_weight_for_household_size: bool = False,
    weight_column: str = defaults.columns.weight,
    equivalence_scale: _EquivalenceScale = "Household",
    for_all: bool = True,
    annual: bool = True,
    groupby: _Attribute | Iterable[_Attribute] | None = None,
    years: int | Iterable[int] | str | None = None,
) -> pd.DataFrame:
    setting_vars = locals()
    setting_vars.update({"bins": 100})
    quantile = calculate_quantile(**setting_vars)
    quantile.index = table.index
    table[quantile_column_name] = quantile
    return table
