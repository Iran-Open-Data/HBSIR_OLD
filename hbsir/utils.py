"""
Utility functions
"""

import subprocess
from pathlib import Path
import platform
from typing import Iterable
from zipfile import ZipFile

from tqdm import tqdm
import requests

from .metadata import (
    defaults,
    metadatas,
    original_tables,
    Table as _Table,
)


def download_file(
    url: str, path: str | Path | None = None, show_progress_bar: bool = False
) -> Path:
    """
    Download a file from a URL to a local path.

    :param url: The URL to download the file from.
    :param path: The local path to save the downloaded file to. If None, a
        temporary directory is used.
    :param show_progress_bar: Whether to display a progress bar during the
        download.
    :param timeout: The timeout for the download request, in seconds.
    :returns: The local path of the downloaded file.

    """
    if isinstance(path, str):
        path = Path(path)
        file_name = path.name
    elif path is None:
        temp_folder = defaults.pack_dir.joinpath("temp")
        temp_folder.mkdir(exist_ok=True)
        file_name = url.split("/")[-1]
        path = temp_folder.joinpath(file_name)
    else:
        file_name = path.name

    response = requests.get(url, timeout=10, stream=True)
    file_size = response.headers.get("content-length")
    if file_size is not None:
        file_size = int(file_size)
    else:
        raise FileNotFoundError
    download_bar = tqdm(
        desc=f"downloading {file_name}",
        total=file_size,
        unit="B",
        unit_scale=True,
        disable=not show_progress_bar,
    )
    Path(path.parent).mkdir(parents=True, exist_ok=True)
    with open(path, mode="wb") as file:
        for chunk in response.iter_content(chunk_size=4096):
            download_bar.update(len(chunk))
            file.write(chunk)
    download_bar.close()
    return path


def download_7zip():
    """
    Download the appropriate version of 7-Zip for the current operating system
    and architecture, and extract it to the root directory.

    """
    print(
        f"Downloading 7-Zip for {platform.system()} with {platform.architecture()[0]} architecture"
    )
    file_name = f"{platform.system()}-{platform.architecture()[0]}.zip"
    file_path = Path().joinpath("temp", file_name)
    file_path = download_file(
        f"{defaults.online_dir}/7-Zip/{file_name}",
        show_progress_bar=True,
    )
    with ZipFile(file_path) as zip_file:
        zip_file.extractall(defaults.pack_dir)
    file_path.unlink()

    if platform.system() == "Linux":
        defaults.pack_dir.joinpath("7-Zip", "7zz").chmod(0o771)


def extract_with_7zip(compressed_file_path: str, output_directory: str) -> None:
    """
    Extracts the contents of a compressed file using the 7-Zip tool.

    :param compressed_file_path: The path to the compressed file to be extracted.
    :type compressed_file_path: str
    :param output_directory: The path to the directory where the extracted files will be stored.
    :type output_directory: str
    :return: None, as this function does not return anything.

    """
    if platform.system() == "Windows":
        seven_zip_file_path = Path().joinpath("7-Zip", "7z.exe")
        if not seven_zip_file_path.exists():
            download_7zip()
        subprocess.run(
            [
                seven_zip_file_path,
                "e",
                compressed_file_path,
                f"-o{output_directory}",
                "-y",
            ],
            check=False,
            shell=True,
        )
    elif platform.system() == "Linux":
        seven_zip_file_path = defaults.pack_dir.joinpath("7-Zip", "7zz")
        if not seven_zip_file_path.exists():
            download_7zip()
        subprocess.run(
            [
                seven_zip_file_path,
                "e",
                compressed_file_path,
                f"-o{output_directory}",
                "-y",
            ],
            check=False,
        )

def _check_year_validity(year: str | int) -> int:
    if isinstance(year, str):
        year = int(year.strip())

    if year <= 60:
        year += 1400
    elif year < 100:
        year += 1300

    if year not in range(defaults.first_year, defaults.last_year+1):
        raise ValueError(f"Year {year} not in range {defaults.first_year, defaults.last_year}")

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
            year_list.extend(list(range(start_year, end_year+1)))
        else:
            year_list.append(_check_year_validity(part))
    return year_list


def parse_years(years: int | Iterable[int] | str | None,) -> list[int]:
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
        year_list = list(range(defaults.first_year, defaults.last_year+1))
    elif isinstance(years, int):
        year_list = [_check_year_validity(years)]
    elif isinstance(years, str):
        year_list = _parse_year_str (years)
    elif isinstance(years, Iterable):
        year_list = [_check_year_validity(year) for year in years]
    else:
        raise TypeError

    return year_list



def interpret_number_range(
    number_range_info: int | dict | list,
    default_end: int | None = None,
    keyword: str = "code",
) -> list[int]:
    """
    A tool for interpretation of number ranges
    """
    number_range: list[int] = []
    if isinstance(number_range_info, int):
        number_range.append(number_range_info)
    elif isinstance(number_range_info, dict):
        if ("start" in number_range_info) and ("end" in number_range_info):
            number_range.extend(
                list(range(number_range_info["start"], number_range_info["end"]))
            )
        elif ("start" in number_range_info) and (default_end is not None):
            number_range.extend(list(range(number_range_info["start"], default_end)))
        elif keyword in number_range_info:
            number_range.extend(
                interpret_number_range(number_range_info[keyword], default_end=default_end)
            )
        else:
            raise KeyError
    elif isinstance(number_range_info, list):
        for element in number_range_info:
            number_range.extend(interpret_number_range(element, default_end=default_end))
    else:
        raise KeyError

    return number_range


def build_year_interval(
    from_year: int | None,
    to_year: int | None,
    earliest_year: int | None = None,
    latest_year: int | None = None,
) -> tuple[int, int]:
    """
    Returns a tuple of two integers representing a range of years.

    :param from_year: An integer representing the starting year of the range.
        If `None`, the default starting year is used.
    :type from_year: int or None

    :param to_year: An integer representing the ending year of the range. If
        `None`, the default ending year + 1 is used.
    :type to_year: int or None

    :return: A tuple representing the range of years.
    :rtype: tuple of two integers

    :raises: ValueError if `from_year` is greater than `to_year`.

    Example:

    >>> build_year_interval()
    (1363, 1401)

    >>> build_year_interval(1390)
    (1390, 1401)

    >>> build_year_interval(1380, 1385)
    (1380, 1386)

    >>> build_year_interval(1383, 1375)
    ValueError: `from_year` must be less than `to_year`.
    """
    if earliest_year is None:
        earliest_year = defaults.first_year
    if latest_year is None:
        latest_year = defaults.last_year

    if (from_year is not None) and (to_year is not None):
        if to_year < from_year:
            raise ValueError("`from_year` is greater than `to_year`")
        _from_year, _to_year = from_year, to_year + 1
    elif from_year == 0:
        _from_year, _to_year = earliest_year, latest_year + 1
    elif from_year is not None:
        _from_year, _to_year = from_year, from_year + 1
    elif to_year is not None:
        raise KeyError
    else:
        _from_year, _to_year = latest_year, latest_year + 1

    return _from_year, _to_year


def build_year_interval_for_table(
    table_name: _Table, from_year: int | None = None, to_year: int | None = None
) -> list[int]:
    """_summary_

    Parameters
    ----------
    table_name : str
        _description_
    from_year : int | None, optional
        _description_, by default None
    to_year : int | None, optional
        _description_, by default None

    Returns
    -------
    _type_
        _description_
    """
    availability_info = metadatas.tables["yearly_table_availability"][table_name]
    available_years = interpret_number_range(availability_info, default_end=defaults.last_year+1)
    from_year, to_year = build_year_interval(from_year, to_year)
    selected_years = [year for year in range(from_year, to_year) if year in available_years]
    return selected_years


def create_table_year_product(
    table_name: _Table | list[_Table] | tuple[_Table] | None,
    from_year: int | None = None,
    to_year: int | None = None,
) -> list[tuple[_Table, int]]:
    """_summary_

    Parameters
    ----------
    table_name : str | List[str]
        _description_
    from_year : int | None
        _description_
    to_year : int | None, optional
        _description_, by default None

    Returns
    -------
    List[tuple]
        _description_
    """
    table_list: list[_Table] | tuple[_Table]
    if table_name is None:
        table_list = original_tables
    elif isinstance(table_name, (list, tuple)):
        table_list = table_name
    else:
        table_list = [table_name]

    product_list = []
    for _table_name in table_list:
        years = build_year_interval_for_table(_table_name, from_year, to_year)
        product_list.extend([(_table_name, year) for year in years])
    return product_list


def is_multi_year(
    table_name: _Table | list[_Table] | tuple[_Table],
    from_year: int | None = None,
    to_year: int | None = None,
) -> bool:
    if isinstance(table_name, str):
        table_list: list[_Table] = [table_name]
    else:
        table_list = [table for table in table_name]

    for _table_name in table_list:
        years = build_year_interval_for_table(_table_name, from_year, to_year)
        if len(years) > 1:
            return True
    return False


# def _parse_sentence(sentence: str):
#     if sentence.count("*") >= 1:
#         parts = sentence.split("*")
#         coeffs = [part for part in parts if (part.isnumeric())]
#     else:
#         numbers = [(char.isnumeric() or (char == ".")) for char in sentence]
#         first_letter = numbers.index(False)
#         coeff, var = sentence[:first_letter], sentence[first_letter:]
#     if coeff == "":
#         coeff = 1
#     elif coeff.find(".") > 0:
#         coeff = float(coeff)
#     else:
#         coeff = int(coeff)
#     parsed_sentence = (coeff, var)
#     return parsed_sentence


# def _parse_expression(expression: str):
#     expression = expression.replace(" ", "")
#     symbols = re.findall(r"[+\-]", expression)
#     sentences = re.split(r"[+\-]", expression)
#     parsed_sentences = [_parse_sentence(sent) for sent in sentences if sent != ""]
#     coeffs = [element[0] for element in parsed_sentences]
#     variables = [element[1] for element in parsed_sentences]
#     if len(symbols) + 1 == len(variables):
#         symbols = ["+"] + symbols
#     if len(symbols) != len(variables):
#         raise SyntaxError
#     parsed_expression = list(zip(symbols, coeffs, variables))
#     return list(parsed_expression)


# def build_pandas_expression(expression: str, table_name="table"):
#     parsed_expression = _parse_expression(expression)

#     first_sentence =True
#     pandas_expression = ""
#     for sign, coeff, var in parsed_expression:
#         if first_sentence:
#             first_sentence = False
#             sign = "" if sign == "+" else sign
#         sign += " "

#         if coeff == 0:
#             continue

#         if coeff == 1:
#             coeff = ""
#         else:
#             coeff = f" * {coeff}"

#         pandas_expression += (f"{sign}{table_name}['{var}'].fillna(0){coeff} ")
#     pandas_expression = pandas_expression.strip()
#     return pandas_expression
