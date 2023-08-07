from typing import Iterable

from ..metadata_reader import defaults, metadatas, Table as _Table
from .argham import Argham


def parse_years(years: int | Iterable[int] | str | None) -> list[int]:
    """Convert different year representations to a list of integer years.

    This function handles converting various different input types representing
    years into a standardized list of integer years.

    Args:
        year: The input year value. Can be one of the following types:
            - int: A single year
            - Iterable[int]: A collection of years
            - str: A comma-separated string of years or ranges

    Returns:
        list[int]: The converted years as a list of integer values.

    Raises:
        TypeError: If the input year is not one of the accepted types.

    Examples:
        >>> get_year_list(1399)
        [1399]

        >>> get_year_list([98, 99, 1400])
        [1398, 1399, 1400]

        >>> get_year_list('1365, 80-83, 99')
        [1365, 1380, 1381, 1382, 1383, 1399]
    """
    if years is None:
        year_list = [defaults.last_year + 1]
    elif isinstance(years, int):
        year_list = [_check_year_validity(years)]
    elif isinstance(years, str):
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
    table_names: str | Iterable[str], years: int | Iterable[int] | str | None
) -> list[tuple[str, int]]:
    years = parse_years(years)
    table_names = [table_names] if isinstance(table_names, str) else table_names
    table_year = []
    for table_name in table_names:
        if table_name in metadatas.tables["yearly_table_availability"]:
            available_years = Argham(
                metadatas.tables["yearly_table_availability"][table_name],
                default_start=defaults.first_year,
                default_end=defaults.last_year + 1,
            )
        else:
            available_years = range(defaults.first_year, defaults.last_year + 1)
        table_year.extend(
            [(table_name, year) for year in years if year in available_years]
        )
    return table_year
