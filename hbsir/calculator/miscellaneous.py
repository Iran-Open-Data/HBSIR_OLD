import pandas as pd

from ..core.metadata_reader import defaults


def weighted_average(
    table: pd.DataFrame, weight_col: str = defaults.columns.weight
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
    if weight_col not in table.columns:
        raise ValueError(f"Weight column {weight_col} not in table")
    columns = [col for col in table.columns if col != weight_col]
    table[columns] = table[columns].multiply(table[weight_col], axis="index")
    columns_summation = table.sum()
    results = (
        columns_summation[columns].divide(columns_summation[weight_col]).loc[columns]
    )
    return results
