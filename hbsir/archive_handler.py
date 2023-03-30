"""
The purpose of the archive handler module is to allow downloading and storing
raw data more easily.
"""

import shutil
import subprocess
import platform
from pathlib import Path

from tqdm import tqdm
import pandas as pd
import pyodbc

from . import metadata, utils


defaults = metadata.defaults


def setup(
        from_year: int | None = None, to_year: int | None = None,
        replace: bool = False) -> None:
    """
    Download census data archive files, unpack them, and extract tables from the MS Access files
    for each year specified in the range of years and save them as CSV.

    Parameters
    ----------
    from_year : int, optional
        The starting year of the range. If not provided, files will be set up for all available
        years.
    to_year : int, optional
        The ending year of the range. If provided, files will be set up for the years between
        from_year and to_year, inclusive. If not provided, files will be set up for the single
        year specified in from_year.
    replace : bool, optional
        If True, existing files will be overwritten. If False, existing files will be skipped.
        The default is False.

    Returns
    -------
    None

    See Also
    --------
    download : A function that downloads household census data archive files.
    unpack : A function that unpacks household census data archive files.
    extract_tables : A function that extracts tables from a census MS Access file and saves them
        as CSV files

    Notes
    -----

    .. note::

        For ordinary usage, it is not necessary to download these files. This data is intended
        for use during development and metadata writing.

    .. warning::
        Note that setting up files for the entire range of years will occupy approximately 12 GB
        of storage.
    

    Examples
    --------
    To set up all available files:    

    >>> setup()

    To set up files for the year 1393:    

    >>> setup(1393)

    To set up files for the years 1370 to 1380, inclusive:    

    >>> setup(1370, 1380)
    """
    download(from_year, to_year, replace)
    unpack(from_year, to_year, replace)
    extract_tables(from_year, to_year, replace)


def download(
        from_year: int | None = None, to_year: int | None = None,
        replace: bool = False) -> None:
    """
    Downloads household census data archive files.
    
    Parameters
    ----------
    from_year : int, optional
        The starting year of the range. If provided, download files for that specific year only.
        If not provided, download files for all available years.
    to_year : int, optional
        The ending year of the range. If provided, download files for the years between
        from_year and to_year, inclusive. If not provided, and from_year is provided,
        download files for the single year specified in from_year.
    replace : bool, optional
        If True, overwrite existing files. If False, skip existing files. Default is False.
        
    Returns
    -------
    None

    See Also
    --------
    setup : A function that downloads, unpacks and extracts all tables from household census data
        archive files and save them as CSV.

    Examples
    --------
    To download all available files:    

    >>> download()

    To download files for the year 1393:    

    >>> download(1393)

    To download files for the years 1370 to 1380, inclusively:    

    >>> download(1370, 1380)
    """
    from_year, to_year = utils.build_year_interval(from_year, to_year)
    Path(defaults.archive_files).mkdir(exist_ok=True, parents=True)
    for year in range(from_year, to_year):
        _download_year_file(year, replace=replace)


def _download_year_file(year: int, replace: bool = True) -> None:
    """
    Downloads the household census data archive file for a specified year
    from the online directory in its original form.

    :param year: The year for which to download the household census file.
    :type year: int
    :param replace: Whether to replace the file if it already exists.
    Defaults to True.
    :type replace: bool, optional
    :return: None, as this function does not return anything.

    """
    file_name = f"{year}.rar"
    file_url = f"{defaults.online_dir}/original_files/{file_name}"
    defaults.archive_files.mkdir(parents=True, exist_ok=True)
    local_path = defaults.archive_files.joinpath(file_name)
    if (not Path(local_path).exists()) or replace:
        utils.download_file(url=file_url, path=local_path,
                            show_progress_bar=True)


def _unpack_archive_with_7zip(
        compressed_file_path: Path | str, output_directory: Path | str) -> None:
    """
    Extracts the contents of a compressed file using the 7-Zip tool.

    :param compressed_file_path: The path to the compressed file to be
    extracted.
    :type compressed_file_path: str
    :param output_directory: The path to the directory where the
    extracted files will be stored.
    :type output_directory: str
    :return: None, as this function does not return anything.

    """
    if platform.system() == "Windows":
        seven_zip_file_path = Path().joinpath("7-Zip", "7z.exe")
        if not seven_zip_file_path.exists():
            utils.download_7zip()
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


def unpack(
        from_year: int | None = None, to_year: int | None = None,
        replace: bool = False) -> None:
    """
    Unpacks census data archive files, extracts tables from the MS Access files and saves them
    as CSV files for each year specified in the range of years.

    Parameters
    ----------
    from_year : int, optional
        The starting year of the range. If not provided, unpack files for all available years.
    to_year : int, optional
        The ending year of the range. If provided, unpack files for the years between from_year
        and to_year, inclusive. unpack files for the single year specified in from_year.
    replace : bool, optional
        If True, overwrite existing files. If False, skip existing files. Default is False.

    Returns
    -------
    None

    See Also
    --------
    setup : A function that downloads, unpacks and extracts all tables from household census data
        archive files and save them as CSV.

    Notes
    -----
    .. note::

        Any nested archive files within the extracted directories will also be extracted
        recursively.

    Examples
    --------
    To unpack all available archives:    

    >>> unpack()

    To unpack archives from 1393 to 1400:    

    >>> unpack(1393, 1400)
    """
    from_year, to_year = utils.build_year_interval(
        from_year=from_year, to_year=to_year)
    for year in tqdm(range(from_year, to_year), desc="Unziping raw data", unit="file"):
        _unpack_yearly_data_archive(year, replace=replace)


def _unpack_yearly_data_archive(year: int, replace: bool = True):
    """
    Extracts data archive for the given year.
    """
    file_path = defaults.archive_files.joinpath(f"{year}.rar")
    year_directory = defaults.unpacked_data.joinpath(str(year))
    if year_directory.exists():
        if not replace:
            return
        shutil.rmtree(year_directory)
    year_directory.mkdir(parents=True)
    _unpack_archive_with_7zip(file_path, year_directory)
    _unpack_archives_recursive(year_directory)
    _remove_created_directories(year_directory)


def _unpack_archives_recursive(directory: str | Path):
    """
    Extract all archives with the extensions ".zip" and ".rar" found
    recursively in the given directory and its subdirectories using
    7zip.

    :param directory: The directory path to search for archives.
    :type directory: str
    :return: None

    """
    while True:
        all_files = list(Path(directory).iterdir())
        archive_files = [
            file for file in all_files if file.suffix in (".zip", ".rar")]
        if len(archive_files) == 0:
            break
        for file in archive_files:
            _unpack_archive_with_7zip(file, directory)
            Path(file).unlink()


def _remove_created_directories(directory: Path):
    for path in directory.iterdir():
        if path.is_dir():
            shutil.rmtree(path)


def extract_tables(
        from_year: int | None = None, to_year: int | None = None,
        replace: bool = False) -> None:
    """
    Extracts tables from a census MS Access file and saves them as CSV files.

    Parameters
    ----------
    from_year : int, optional
        The starting year of the range. If not provided, extract tables for all available years.
    to_year : int, optional
        The ending year of the range. If provided, extract tables for the years between from_year
        and to_year, inclusive. extract tables for the single year specified in from_year.
    replace : bool, optional
        If True, overwrite existing files. If False, skip existing files. Default is False.

    Returns
    -------
    None

    See Also
    --------
    setup : A function that downloads, unpacks and extracts all tables from household census data
        archive files and save them as CSV.

    Examples
    --------
    To extract tables from all available census files:    

    >>> extract_tables()

    To extract tables from all available census files from 1393 to 1400:    

    >>> extract_tables(from_year=1393, to_year=1400)
    """
    from_year, to_year = utils.build_year_interval(from_year, to_year)
    for year in range(from_year, to_year):
        _extract_tables_from_access_file(year, replace)


def _extract_tables_from_access_file(year: int, replace: bool = True) -> None:
    table_list = _get_access_table_list(year)
    for table_name in tqdm(
        table_list, desc=f"Extracting data from {year}", unit="table"
    ):
        _extract_table(year, table_name, replace)


def _get_access_table_list(year: int) -> list:
    connection_string = _make_connection_string(year)
    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()
        access_tables = cursor.tables()
        table_list = []
        for table in access_tables:
            table_list.append(table.table_name)
    table_list = [table for table in table_list if table.find("MSys") == -1]
    return table_list


def _extract_table(year: int, table_name: str, replace: bool = True):
    file_name = _change_1380_table_names(year, table_name)
    year_directory = defaults.extracted_data.joinpath(str(year))
    year_directory.mkdir(parents=True, exist_ok=True)
    file_path = year_directory.joinpath(f"{file_name}.csv")
    if (file_path.exists()) and (not replace):
        return

    try:
        table = _get_access_table(year, table_name)
    except (pyodbc.ProgrammingError, pyodbc.OperationalError):
        tqdm.write(f"table {table_name} from {year} failed to extract")
        return
    table.to_csv(file_path, index=False)


def _change_1380_table_names(year: int, table_name: str):
    if year == 1380:
        if table_name in metadata.metadatas.other["unusual_names_of_1380"]:
            table_name = metadata.metadatas.other["unusual_names_of_1380"][table_name]
    return table_name


def _get_access_table(year: int, table_name: str) -> pd.DataFrame:
    connection_string = _make_connection_string(year)
    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()
        rows = cursor.execute(f"SELECT * FROM [{table_name}]").fetchall()
        headers = [c[0] for c in cursor.description]
    table = pd.DataFrame.from_records(rows, columns=headers)
    return table


def _make_connection_string(year: int):
    if platform.system() == "Windows":
        driver = "Microsoft Access Driver (*.mdb, *.accdb)"
    else:
        driver = "MDBTools"

    year_directory = defaults.unpacked_data.joinpath(str(year))
    access_file_path = _find_access_file_by_extension(year_directory)
    conn_str = f"DRIVER={{{driver}}};" f"DBQ={access_file_path};"
    return conn_str


def _find_access_file_by_extension(directory: Path) -> Path:
    files = list(directory.iterdir())
    for file in files:
        if file.suffix.lower() in [".mdb", ".accdb"]:
            return file
    raise FileNotFoundError
