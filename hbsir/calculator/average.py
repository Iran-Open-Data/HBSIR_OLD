import pandas as pd

from ..core.metadata_reader import defaults, _Groupby
from .. import api
from .quantile import add_decile, add_percentile


def weighted_average(
    table: pd.DataFrame, columns: list[str] | None = None, weight_col: str | None = None
) -> pd.Series:
    """Calculate the weighted average of columns in a DataFrame.

    Parameters
    ----------
    table : DataFrame
        Input DataFrame containing the columns to calculate
        weighted average for and the weight column.
    weight_col : str, default 'Weight'
        The name of the column containing the weights.

    Returns
    -------
    pandas Series
        A Series containing the weighted average of each column.

    Raises
    ------
    ValueError
        If the weight column is not in the table.

    Examples
    --------
    >>> df = pd.DataFrame({
            'Col1': [1, 2, 3],
            'Col2': [25, 15, 10],
            'Weight': [0.25, 0.25, 0.5]})
    >>> weighted_average(df)
        Col1     2.25
        Col2    15.00
        dtype: float64

    """
    weight_col = defaults.columns.weight if weight_col is None else weight_col
    if weight_col not in table.columns:
        raise ValueError(f"Weight column {weight_col} not in table")
    if columns is None:
        columns = [
            col
            for col in table.select_dtypes("number").columns
            if col in table.columns
            if col not in defaults.columns.groupby
            if col not in [defaults.columns.household_id, weight_col, "index"]
        ]
    table[columns] = table[columns].multiply(table[weight_col], axis="index")
    columns_summation = table.sum()
    results = (
        columns_summation.loc[columns]
        .divide(columns_summation[weight_col])
        .loc[columns]
    )
    return results


def average_table(
    table: pd.DataFrame,
    columns: list[str] | None = None,
    groupby: list[_Groupby] | _Groupby | None = None,
    weight_col: str | None = None,
    weighted: bool = True,
) -> pd.DataFrame:
    if isinstance(table.columns, pd.MultiIndex):
        is_multi_index = True
        column_names = table.columns.names
        table.columns = table.columns.to_flat_index()
    else:
        is_multi_index = False
        column_names = None

    table = table.reset_index().copy()

    if groupby is None:
        groupby = [col for col in table.columns if col in defaults.columns.groupby]
    elif isinstance(groupby, str):
        groupby = [groupby]
    if groupby is None:
        groupby = []

    for groupby_column in ("Urban_Rural", "Province", "County"):
        if groupby_column not in table.columns:
            table = api.add_attribute(table, groupby_column)
    if "Percentile" in groupby:
        table = add_percentile(table)
    elif "Decile" in groupby:
        table = add_decile(table)

    weight_col = defaults.columns.weight if weight_col is None else weight_col

    if not weighted:
        table["weight_col"] = 1
    elif weight_col not in table.columns:
        table = api.add_weight(table)

    if len(groupby) == 0:
        row = weighted_average(table, columns=columns, weight_col=weight_col)
        result = pd.DataFrame([row])
    else:
        result = table.groupby(groupby).apply(weighted_average, columns, weight_col)

    if is_multi_index:
        result.columns = pd.MultiIndex.from_tuples(result.columns, names=column_names)

    return result
