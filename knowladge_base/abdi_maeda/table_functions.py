import pandas as pd


def head_age_range(
    table: pd.DataFrame,
    start_year: int = 1201,
    end_year: int = 1400,
    interval: int = 7,
    minimum_age: int = 0,
    maximum_age: int = 100,
) -> pd.DataFrame:
    bins = range(start_year, end_year, interval)
    labels = [f"{year}_{year+interval-1}" for year in bins][:-1]
    return (
        table.query(f"Relationship=='Head' and {minimum_age} < Age < {maximum_age}")
        .assign(
            Head_Birth_Year=lambda df: pd.cut(
                df.eval("Year - Age"), bins, labels=labels
            )
        )
        .set_index(["Year", "ID"])
        .loc[:, ["Head_Birth_Year"]]
    )
