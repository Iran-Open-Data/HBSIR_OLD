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


def cpi_by1395_urban_singleindex(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[2:, [2]]
    table.index = create_year_month_index(1361, 1401)
    table.columns = ["CPI"]
    return table


def cpi_by1395_rural_maingroups(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 53:].T
    table.index = create_year_month_index(1374, 1401)
    table.columns = ["CPI"]
    return table


def cpi_by1395(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(
        table_list, keys=["Urban", "Rural"], names=["Urban_Rural", "Year", "Month"]
    )
