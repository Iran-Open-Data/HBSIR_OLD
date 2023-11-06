from typing import Iterable, Literal

from ..core.metadata_reader import (
    defaults,
    metadata,
    _Years,
    _OriginalTable,
    original_tables,
)
from .argham import Argham


def parse_years(years: _Years) -> list[int]:
    """Convert different year representations to a list of integer years.

    This function handles converting various input types representing
    years into a standardized list of integer years.

    The input `years` can be specified as:

    - int: A single year
    - Iterable[int]: A collection of years like [94, 95, 96] or range(1390, 1400)
    - str: A comma-separated string of years or ranges like '86-90, 1396-1400'
    - "all": All available years
    - "last": Just the last year

    Years are validated before returning.

    Parameters
    ----------
    years : _Years
        The input years to parse

    Returns
    -------
    list[int]
        The converted years as a list of integer values

    Examples
    --------
    >>> parse_years(1399)
    [1399]

    >>> parse_years([98, 99, 1400])
    [1398, 1399, 1400]

    >>> parse_years(range(1380, 1390))
    [1380, 1381, 1382, 1383, 1384, 1385, 1386, 1387, 1388, 1389]

    >>> parse_years('1365, 80-83, 99')
    [1365, 1380, 1381, 1382, 1383, 1399]
    """
    if isinstance(years, int):
        year_list = [_check_year_validity(years)]
    elif isinstance(years, str):
        if years.lower() == "all":
            year_list = list(range(defaults.first_year, defaults.last_year + 1))
        elif years.lower() == "last":
            year_list = [defaults.last_year]
        else:
            year_list = _parse_year_str(years)
    elif isinstance(years, Iterable):
        year_list = [_check_year_validity(year) for year in years]
    else:
        raise TypeError

    return year_list


def _check_year_validity(year: str | int) -> int:
    if isinstance(year, str):
        year = int(year.strip())

    if year <= 60:
        year += 1400
    elif year < 100:
        year += 1300

    if year not in range(defaults.first_year, defaults.last_year + 1):
        raise ValueError(
            f"Year {year} not in range {defaults.first_year, defaults.last_year}"
        )

    return year


def _parse_year_str(year: str) -> list[int]:
    year_list = []
    year_parts = year.split(",")
    for part in year_parts:
        if part.find("-") >= 0:
            year_interval = part.split("-")
            if len(year_interval) != 2:
                raise ValueError(f"Interval Not Valid {part}")
            start_year, end_year = year_interval
            start_year = _check_year_validity(start_year)
            end_year = _check_year_validity(end_year)
            year_list.extend(list(range(start_year, end_year + 1)))
        else:
            year_list.append(_check_year_validity(part))
    return year_list


def construct_table_year_pairs(
    table_names: _OriginalTable | Iterable[_OriginalTable] | Literal["all"],
    years: _Years,
) -> list[tuple[_OriginalTable, int]]:
    """Constructs list of (table, year) tuples from inputs.

    Takes table names and years and returns a list of valid (table, year) pairs.
    Checks table availability for each provided year.

    Parameters
    ----------
    table_names : _OriginalTable or Iterable[_OriginalTable]
        Table name(s) to construct pairs for.

    years : _Years
        Year(s) to construct pairs for.

    Returns
    -------
    list[tuple[_OriginalTable, int]]
        List of (table, year) tuples.

    """
    years = parse_years(years)
    table_names = original_tables if table_names == "all" else table_names
    table_names = [table_names] if isinstance(table_names, str) else table_names
    table_year = []
    for table_name in table_names:
        if table_name in metadata.tables["yearly_table_availability"]:
            available_years = Argham(
                metadata.tables["yearly_table_availability"][table_name],
                default_start=defaults.first_year,
                default_end=defaults.last_year + 1,
            )
        else:
            available_years = range(defaults.first_year, defaults.last_year + 1)
        table_year.extend(
            [(table_name, year) for year in years if year in available_years]
        )
    return table_year
