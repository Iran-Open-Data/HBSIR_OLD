"""docs"""

import pandas as pd
import pytest

import hbsir
from hbsir.data_engine import add_attribute, add_classification, read_table


class TestAddAttribute:
    """Tests for add_attribute Function"""
    def test_basics(self, food_1400):
        """Test if add_attribute adds correct number of columns"""
        for attr in ["Urban-Rural", "Province", "Region"]:
            assert (
                len(add_attribute(food_1400, attribute=attr).columns) # type: ignore
                == len(food_1400.columns) + 1
            )

        for attr in [["Urban-Rural"], ["Urban-Rural", "Province"],
                    ["Urban-Rural", "Province", "Region"]]:
            assert (
                len(add_attribute(food_1400, attribute=attr).columns) # type: ignore
                == len(food_1400.columns) + len(attr)
            )


class TestWithFormalNumbers:
    """Compare output of calculations with formal numbers"""

    @pytest.fixture()
    def prepared_expenditure(self, expenditures_1400):
        """Add necessary attribute and classification to expenditure table"""
        exp = expenditures_1400.copy()
        exp = add_attribute(exp, attribute="Urban-Rural")
        exp = add_classification(exp, "Food-NonFood")
        exp = exp.rename(columns={"Net_Expenditure": "Net", "Gross_Expenditure": "Gross"})
        return exp

    @pytest.fixture()
    def prepared_hh_info(self):
        """Get household info"""
        info = read_table("household_information", 1400)
        info = add_attribute(info, attribute="Urban-Rural")
        info = info.set_index("ID")
        return info

    def _calc_hh_agg(self, frame, urban_rural):
        frame = frame.copy()
        filt = frame["Urban-Rural"] == urban_rural
        frame = frame.loc[filt].groupby(["ID", "Food-NonFood"])[["Net", "Gross"]].sum()
        frame = frame.unstack([1])
        return frame

    def _calc_wghtd_avrg(self, frame, hh_info, urban_rural):
        filt = hh_info["Urban-Rural"] == urban_rural
        weights = hh_info.loc[filt, "Weight"]
        wghtd_avrg = frame.apply(lambda column: (column * weights).sum() / weights.sum()) / 1000
        return wghtd_avrg

    @pytest.fixture()
    def year_table(self, prepared_expenditure, prepared_hh_info):
        """Create summary expenditure table of 1400"""
        agg_tables = []
        for ur_ru in ["Urban", "Rural"]:
            frame = self._calc_hh_agg(prepared_expenditure, ur_ru)
            frame = self._calc_wghtd_avrg(frame, prepared_hh_info, ur_ru)
            frame.index.rename(["Net-Gross", "Food-NonFood"], inplace=True)
            agg_tables.append(frame)
        year_table = pd.concat(agg_tables, keys=["Urban", "Rural"], names=["Urban-Rural"])
        return year_table

    def test_equality(self, year_table):
        """
        # Compare output numbers with summary expenditure table of 1400

            Urban-Rural    Net-Gross    Food-NonFood     Value
            -------------  -----------  --------------  --------
            Urban          Net          Food             246,537
            Urban          Net          Non-Food         678,479
            Urban          Gross        Food             246,537
            Urban          Gross        Non-Food         694,814
            Rural          Net          Food             207,037
            Rural          Net          Non-Food         312,090
            Rural          Gross        Food             207,037
            Rural          Gross        Non-Food         322,389
        """
        index_value =[
                [('Urban',   'Net',      'Food'),       246_537],
                [('Urban',   'Net',      'Non-Food'),   678_480],
                [('Urban',   'Gross',    'Food'),       246_537],
                [('Urban',   'Gross',    'Non-Food'),   694_815],
                [('Rural',   'Net',      'Food'),       207_034],
                [('Rural',   'Net',      'Non-Food'),   312_085],
                [('Rural',   'Gross',    'Food'),       207_034],
                [('Rural',   'Gross',    'Non-Food'),   322_383],
            ]
        for index, value in index_value:
            assert abs(year_table.loc[index] - value) < 10


def test_incomes(incomes_1400):
    table = incomes_1400.copy()
    filt = hbsir.add_attribute(table, "Urban-Rural")["Urban-Rural"] == "Urban"
    table = table.loc[filt]
    table = table.groupby(["ID", "Income_Type"])["Income"].sum()
    table = table.unstack()

    info = read_table("household_information", 1400)
    info = add_attribute(info, attribute="Urban-Rural")
    filt = hbsir.get_attribute(info, "Urban-Rural", year=1400) == "Urban"
    info = info.loc[filt]
    info = info.set_index("ID")
    weights = info["Weight"]

    results = {}
    for column_name, column in table.items():
        value = (column * weights).sum() / weights.sum() / 1000
        results[column_name] = value

    for key, value in [
        ("Cash_Public",                 102_544),
        ("NonCash_Public",               19_806),
        ("Cash_Cooperative",                467),
        ("NonCash_Cooperative",              72),
        ("Cash_Private",                219_459),
        ("NonCash_Private",              23_686),
        ("Cash_Agricultural",            17_482),
        ("NonCash_Agricultural",            368),
        ("Cash_NonAgricultural",        166_680),
        ("NonCash_NonAgricultural",       1_423),
        # Cash_Others                   325_061
        # ("Cash_Other",                  272_590),
        ("Cash_Subsidy",                 12_591),
        # NonCash_Others                247_168
        ("NonCash_ImputedRent",         214_137),
        ("NonCash_Donation",             32_250),
        ("NonCash_HomeProduction",          781),
    ]:
        assert abs(results[key] - value) < 10
