import pandas as pd


def create_year_month_index(from_year: int, to_year: int) -> pd.MultiIndex:
    to_year += 1
    return pd.MultiIndex.from_frame(
        pd.concat(
            [
                pd.Series(range(from_year, to_year))
                .repeat(12)
                .reset_index(drop=True)
                .rename("Year"),
                pd.Series(list(range(1, 13)) * (to_year - from_year)).rename("Month"),
            ],
            axis="columns",
        )
    )


def sci_cpi_by1395_urban_singleindex_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[2:, [2]]
    table.index = create_year_month_index(1361, 1401)
    table.columns = ["CPI"]
    return table


def sci_cpi_by1395_urban_singleindex_annual(table: pd.DataFrame) -> pd.DataFrame:
    table.columns = ["Year", "CPI"]
    table = table.loc[2:]
    table = table.set_index("Year")
    return table


def sci_cpi_by1395_rural_maingroups_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 53:].T
    table.index = create_year_month_index(1374, 1401)
    table.columns = ["CPI"]
    return table


def sci_cpi_by1395_rural_maingroups_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[4], 1:].T
    table.columns = ["CPI"]
    table.index = pd.Index(range(1361, 1401), name="Year")
    return table


def sci_cpi_by1395_monthly_urban_rural(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(
        table_list, keys=["Urban", "Rural"], names=["Urban_Rural", "Year", "Month"]
    )


def sci_cpi_by1395_annual_urban_rural(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(table_list, keys=["Urban", "Rural"], names=["Urban_Rural", "Year"])


def sci_gini_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[2], 1:].T
    index = pd.Index(range(1363, 1402), name="Year")
    table = table.set_axis(index, axis="index").set_axis(["Gini"], axis="columns")
    return table
