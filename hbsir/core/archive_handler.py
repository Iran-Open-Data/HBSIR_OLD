"""
This module provides utility functions for downloading, unpacking, and extracting
household budget survey data from archive files. 

Key functions:

- setup() - Downloads, unpacks, and extracts data for specified years
- download() - Downloads archive files for given years 
- unpack() - Unpacks archive files into directories
- extract() - Extracts tables from Access DBs as CSVs

The key functions allow:

- Downloading survey data archive files for specified years from an online directory.

- Unpacking the downloaded archive files (which are in .rar format) into directories.
  Nested archives are extracted recursively.
  
- Connecting to the MS Access database file contained in each archive.

- Extracting all tables from the Access database as CSV files.

This enables access to the raw underlying survey data tables extracted directly 
from the archive Access database files, before any cleaning or processing is applied. 

The extracted CSV table data can then be loaded and cleaned as needed by the
data_cleaner module. 

Typical usage often only requires the cleaned processed data from data_engine.  
However, this module is useful for development and checking details in the original
raw data tables before cleaning.
"""


from contextlib import contextmanager
from typing import Generator
import shutil
import platform
from pathlib import Path

from tqdm import tqdm
import pandas as pd
import pyodbc

from .metadata_reader import defaults, metadata, _Years
from .. import utils


def setup(years: _Years = "all", replace: bool = False) -> None:
    """Download, unpack, and extract survey data for the specified years.

    This function executes the full workflow to download, unpack, and
    extract the raw survey data tables for the given years.

    It calls the download(), unpack(), and extract() functions internally.

    The years can be specified as:

    - int: A single year
    - Iterable[int]: A list or range of years
    - str: A string range like '1390-1400'
    - "all": All available years (default)
    - "last": Just the last year

    Years are parsed and validated by the `parse_years()` helper.

    Existing files are skipped unless `replace=True`.

    Parameters
    ----------
    years : _Years, optional
        Years to setup data for. Default is "all".

    replace : bool, optional
        Whether to re-download and overwrite existing files.

    Returns
    -------
    None

    Examples
    --------
    >>> setup(1393) # Setup only 1393 skip if files already exist

    >>> setup("1390-1400") # Setup 1390 to 1400

    >>> setup("last", replace=True) # Setup last year, replace if already exists

    Notes
    -----
    This function is intended for development use to access the raw data.

    For analysis you likely only need the cleaned dataset.

    Warnings
    --------
    Setting up the full range of years will download and extract
    approximately 12 GB of data.

    See Also
    --------
    download : Download archive files.
    unpack : Unpack archive files.
    extract : Extract tables from Access DBs.
    parse_years : Validate and parse year inputs.
    """
    download(years, replace)
    unpack(years, replace)
    extract(years, replace)


def download(years: _Years = "all", replace: bool = False) -> None:
    """Download archive files for the specified years.

    This downloads the archive files for the given years from the
    online directory to local storage.

    The input `years` can be specified as:

    - int: A single year
    - Iterable[int]: A list or range of years
    - str: A string range like '1390-1400'
    - "all": Download all available years
    - "last": Download just the last year

    Years are parsed and validated by the `parse_years()` helper.

    Parameters
    ----------
    years : _Years, optional
        Years to download archives for. Default is "all".

    replace : bool, optional
        Whether to re-download existing files.

    Returns
    -------
    None

    Raises
    ------
    HTTPError
        If the URL cannot be reached or the file not found.

    Notes
    -----
    The archive file is downloaded from the online directory as a RAR
    file named `{year}.rar` and saved locally under `defaults.archive_files`.

    See Also
    --------
    setup : Download, unpack, and extract data for given years.
    parse_years : Parse and validate different year representations.
    """
    years = utils.parse_years(years)
    Path(defaults.archive_files).mkdir(exist_ok=True, parents=True)
    for year in years:
        _download_year_file(year, replace=replace)


def _download_year_file(year: int, replace: bool = True) -> None:
    """Download the archive file for the given year.

    See Also
    --------
    download: Download archive files for the specified years.
    """
    file_name = f"{year}.rar"
    file_url = f"{defaults.online_dir}/original_files/{file_name}"
    defaults.archive_files.mkdir(parents=True, exist_ok=True)
    local_path = defaults.archive_files.joinpath(file_name)
    if (not Path(local_path).exists()) or replace:
        utils.download(url=file_url, path=local_path, show_progress_bar=True)


def unpack(years: _Years = "all", replace: bool = False) -> None:
    """Extract archive files for the specified years.

    This extracts the RAR archive for each given year from
    defaults.archive_files into a directory under defaults.unpacked_data.

    Nested ZIP/RAR archives found within the extracted files are also
    recursively unpacked.

    The years can be specified as:

    - int: A single year
    - Iterable[int]: A list or range of years
    - str: A string range like '1390-1400'
    - "all": Extract all available years
    - "last": Extract just the last year

    Years are parsed and validated by parse_years().

    Parameters
    ----------
    years : _Years, optional
        Years to extract archives for. Default is "all".

    replace : bool, optional
        Whether to re-extract if directories already exist.

    Returns
    -------
    None

    See Also
    --------
    setup : Download, unpack, and extract data for given years.
    parse_years : Parse and validate year inputs.
    """
    years = utils.parse_years(years)
    for year in tqdm(years, desc="Unziping raw data", unit="file"):
        _unpack_yearly_data_archive(year, replace=replace)


def _unpack_yearly_data_archive(year: int, replace: bool = True):
    """Extract the RAR archive for the given year.

    See Also
    --------
    unpack: Unpack archive files for the specified years.
    _unpack_archives_recursive : Recursively extracts nested archives.
    """
    file_path = defaults.archive_files.joinpath(f"{year}.rar")
    year_directory = defaults.unpacked_data.joinpath(str(year))
    if year_directory.exists():
        if not replace:
            return
        shutil.rmtree(year_directory)
    year_directory.mkdir(parents=True)
    utils.sevenzip(file_path, year_directory)
    _unpack_archives_recursive(year_directory)
    for path in year_directory.iterdir():
        if path.is_dir():
            shutil.rmtree(path)


def _unpack_archives_recursive(directory: Path):
    """Recursively extract nested archives under the given directory.

    This searches the given directory for any ZIP/RAR files, and extracts
    them using 7zip. It calls itself recursively on any nested archives found
    within the extracted directories.

    Stops recursing once no more archives are found.

    Parameters
    ----------
    directory : Path
        The directory under which to recursively extract archives.

    Returns
    -------
    None
    """
    while True:
        archive_files = [
            file for file in directory.iterdir() if file.suffix in (".zip", ".rar")
        ]
        if len(archive_files) == 0:
            break
        for file in archive_files:
            utils.sevenzip(file, directory)
            Path(file).unlink()


def extract(years: _Years = "all", replace: bool = False) -> None:
    """Extract tables from Access DBs into CSV files for the given years.

    This connects to the Access database file for each specified year,
    extracts all the tables, and saves them as CSV files under
    defaults.extracted_data.

    The years can be specified as:

    - int: A single year
    - Iterable[int]: A list or range of years
    - str: A string range like '1390-1400'
    - "all": Extract all available years
    - "last": Extract just the last year

    Tables that fail to extract are skipped.

    Parameters
    ----------
    years: _Years, optional
        Years to extract tables for. Default is "all".

    replace: bool, optional
        Whether to overwrite existing extracted CSV files.

    Returns
    -------
    None

    See Also
    --------
    setup : Download, unpack, and extract data for given years.
    parse_years : Parse and validate year inputs.
    """
    years = utils.parse_years(years)
    for year in years:
        _extract_tables_from_access_file(year, replace)


def _extract_tables_from_access_file(year: int, replace: bool = True) -> None:
    with _create_cursor(year) as cursor:
        table_list = _get_access_table_list(cursor)
        for table_name in tqdm(
            table_list, desc=f"Extracting data from {year}", unit="table"
        ):
            _extract_table(cursor, year, table_name, replace)


@contextmanager
def _create_cursor(year: int) -> Generator[pyodbc.Cursor, None, None]:
    connection_string = _make_connection_string(year)
    connection = pyodbc.connect(connection_string)
    try:
        yield connection.cursor()
    finally:
        connection.close()


def _make_connection_string(year: int):
    if platform.system() == "Windows":
        driver = "Microsoft Access Driver (*.mdb, *.accdb)"
    else:
        driver = "MDBTools"

    year_directory = defaults.unpacked_data.joinpath(str(year))
    access_file_path = _find_access_file_by_extension(year_directory)
    conn_str = f"DRIVER={{{driver}}};" f"DBQ={access_file_path};"
    return conn_str


def _get_access_table_list(cursor: pyodbc.Cursor) -> list:
    table_list = []
    access_tables = cursor.tables()
    for table in access_tables:
        table_list.append(table.table_name)
    table_list = [table for table in table_list if table.find("MSys") == -1]
    return table_list


def _extract_table(
    cursor: pyodbc.Cursor, year: int, table_name: str, replace: bool = True
):
    file_name = _change_1380_table_names(year, table_name)
    year_directory = defaults.extracted_data.joinpath(str(year))
    year_directory.mkdir(parents=True, exist_ok=True)
    file_path = year_directory.joinpath(f"{file_name}.csv")
    if (file_path.exists()) and (not replace):
        return
    try:
        table = _get_access_table(cursor, table_name)
    except (pyodbc.ProgrammingError, pyodbc.OperationalError):
        tqdm.write(f"table {table_name} from {year} failed to extract")
        return
    table.to_csv(file_path, index=False)


def _change_1380_table_names(year: int, table_name: str):
    if year == 1380:
        unusual_names = metadata.other["unusual_names_of_1380"]
        if table_name in unusual_names:
            table_name = unusual_names[table_name]
    return table_name


def _get_access_table(cursor: pyodbc.Cursor, table_name: str) -> pd.DataFrame:
    rows = cursor.execute(f"SELECT * FROM [{table_name}]").fetchall()
    headers = [c[0] for c in cursor.description]
    table = pd.DataFrame.from_records(rows, columns=headers)
    return table


def _find_access_file_by_extension(directory: Path) -> Path:
    files = list(directory.iterdir())
    for file in files:
        if file.suffix.lower() in [".mdb", ".accdb"]:
            return file
    raise FileNotFoundError
