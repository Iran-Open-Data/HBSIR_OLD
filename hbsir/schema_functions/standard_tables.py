import pandas as pd


def number_of_members(table: pd.DataFrame) -> pd.DataFrame:
    return (
        table.assign(Adult=lambda df: df["Age"] >= 14)
        .groupby(["Year", "ID"], as_index=False)
        .agg(
            Members=pd.NamedAgg("Member_Number", "count"),
            Adults=pd.NamedAgg("Adult", "sum"),
        )
        .assign(Childs=lambda df: df["Members"] - df["Adults"])
    )
