import pandas as pd


def create_year_month_index(from_year: int, to_year: int) -> pd.MultiIndex:
    return pd.MultiIndex.from_product(
        [range(from_year, to_year + 1), range(1, 12 + 1)], names=["Year", "Month"]
    )


def sci_cpi_1395_urban_singleindex_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[2:, [2]]
    table.index = create_year_month_index(1361, 1401)
    table.columns = ["CPI"]
    return table


def sci_cpi_1395_urban_singleindex_annual(table: pd.DataFrame) -> pd.DataFrame:
    table.columns = ["Year", "CPI"]
    table = table.loc[2:]
    table = table.set_index("Year")
    return table


def sci_cpi_1395_rural_maingroups_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 53:].T
    table.index = create_year_month_index(1374, 1401)
    table.columns = ["CPI"]
    return table


def sci_cpi_1395_rural_maingroups_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[4], 1:].T
    table.columns = ["CPI"]
    table.index = pd.Index(range(1361, 1401), name="Year")
    return table


def sci_cpi_1395_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 1:].T
    table.columns = ["CPI"]
    table.index = create_year_month_index(1390, 1401)
    return table


def sci_cpi_1395_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[4], 1:].T.astype("float64")
    table.columns = ["CPI"]
    table.index = pd.Index(range(1390, 1401), name="Year")
    return table


def sci_cpi_1395_monthly_urban_rural(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(
        table_list, keys=["Urban", "Rural"], names=["Urban_Rural", "Year", "Month"]
    )


def sci_cpi_1395_annual_urban_rural(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(table_list, keys=["Urban", "Rural"], names=["Urban_Rural", "Year"])


def sci_cpi_1400_urban_singleindex_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[2:, [2]]
    table.index = create_year_month_index(1361, 1402)
    table.columns = ["CPI"]
    return table


def sci_cpi_1400_urban_singleindex_annual(
    table_list: list[pd.DataFrame],
) -> pd.DataFrame:
    table = table_list[0].groupby("Year")[["CPI"]].mean()
    return table


def sci_cpi_1400_rural_maingroups_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table.loc[1, :] = table.loc[1].ffill()
    table.loc[1, 0] = "Year"
    table.loc[2, 0] = "Month_Seasion"
    table = table.loc[1:]
    table = table.set_index(0)
    table.index.name = None
    table = table.T
    table["Year"] = table["Year"].astype(int)
    table = table.set_index(["Year", "Month_Seasion"])
    table = table.replace(r"[\s\-]", None, regex=True)
    return table


def sci_cpi_1400_rural_maingroups_annual(
    table_list: list[pd.DataFrame],
) -> pd.DataFrame:
    table = table_list[0].groupby("Year").mean()
    return table


def sci_cpi_1400_annual_urban_rural(
    table_list: list[pd.DataFrame],
) -> pd.DataFrame:
    urban_table, rural_table = table_list
    rural_table = rural_table.iloc[:, [0]]
    rural_table.columns = ["CPI"]

    table = pd.concat(
        [urban_table, rural_table],
        keys=["Urban", "Rural"],
        names=["Urban_Rural", "Year"],
    )
    return table


def sci_gini_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[2], 1:].T
    index = pd.Index(range(1363, 1402), name="Year")
    table = table.set_axis(index, axis="index").set_axis(["Gini"], axis="columns")
    return table
