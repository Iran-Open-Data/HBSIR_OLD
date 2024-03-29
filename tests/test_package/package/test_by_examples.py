import pytest
import pandas as pd

import hbsir


class TestWithFormalNumbers:
    """Compare output of calculations with formal numbers"""

    @pytest.fixture()
    def commodity_table(self, expenditures_1400: pd.DataFrame) -> pd.DataFrame:
        table = expenditures_1400.copy()
        table = hbsir.add_classification(table, "Food_NonFood")
        return table

    @pytest.fixture()
    def household_table(self, commodity_table: pd.DataFrame) -> pd.DataFrame:
        table = commodity_table.copy()
        table = table.groupby(["Year", "ID", "Food_NonFood"])[
            ["Gross_Expenditure", "Net_Expenditure"]
        ].sum()
        table = table.reset_index()
        table = hbsir.add_weight(table)
        table["Weighted_Gross_Expenditure"] = table.eval("Gross_Expenditure * Weight")
        table["Weighted_Net_Expenditure"] = table.eval("Net_Expenditure * Weight")
        table = hbsir.add_attribute(table, "Urban_Rural")
        return table

    @pytest.fixture()
    def weights_sum(self, weight_1400: pd.DataFrame):
        table = weight_1400.copy()
        table = hbsir.add_attribute(table, "Urban_Rural")
        table = table.groupby(["Year", "Urban_Rural"])["Weight"].sum()
        return table

    @pytest.fixture()
    def summery_table(self, household_table: pd.DataFrame, weights_sum: pd.DataFrame):
        table = household_table.groupby(["Year", "Food_NonFood", "Urban_Rural"])[
            ["Weighted_Net_Expenditure", "Weighted_Gross_Expenditure"]
        ].sum()
        table.columns = ["Net", "Gross"]
        table.columns.name = "Net-Gross"
        table = table.stack().unstack([0, 2])  # type: ignore #
        table = table / weights_sum
        table = table.unstack(1)
        table.loc["Total"] = table.sum()
        table = table[1400]
        table = table.stack([0, 1])
        table = table / 1000
        return table

    def test_equality(self, summery_table):
        isc_summery_values = {
            ("Total", "Urban", "Net"): 92,
            ("Total", "Urban", "Gross"): 94,
            ("Food", "Urban", "Net"): 24,
            ("Food", "Urban", "Gross"): 24,
            ("NonFood", "Urban", "Net"): 67,
            ("NonFood", "Urban", "Gross"): 69,
            ("Total", "Rural", "Net"): 51,
            ("Total", "Rural", "Gross"): 52,
            ("Food", "Rural", "Net"): 20,
            ("Food", "Rural", "Gross"): 20,
            ("NonFood", "Rural", "Net"): 31,
            ("NonFood", "Rural", "Gross"): 32,
        }
        table = summery_table.copy()
        table = table / 10_000
        assert table.astype(int).to_dict() == isc_summery_values
