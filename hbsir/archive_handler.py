"""
docs
"""

import shutil
import subprocess
import platform
from pathlib import Path

from tqdm import tqdm
import pandas as pd
import pyodbc

from . import metadata, utils


defaults = metadata.Defaults()

def download_year_files_in_range(
        from_year: int | None = None,
        to_year: int | None = None,
        replace: bool = False
) -> None:
    """
    Downloads compressed household census files for all years within a
    specified range from the online directory.

    :param from_year: The starting year of the range (inclusive). If not
    specified, defaults to the earliest year available.
    :type from_year: int, optional
    :param to_year: The ending year of the range (inclusive). If not specified,
    defaults to the latest year available.
    :type to_year: int, optional
    :param replace: Whether to replace existing files with the same names. Defaults to False.
    :type replace: bool, optional
    :return: None, as this function does not return anything.

    """
    from_year, to_year = utils.build_year_interval(from_year, to_year)
    Path(defaults.archive_files).mkdir(exist_ok=True, parents=True)
    for year in range(from_year, to_year):
        _download_year_file(year, replace=replace)


def _download_year_file(year: int, replace: bool = True) -> None:
    """
    Downloads a compressed household census file for a specified year from the
    online directory.

    :param year: The year for which to download the household census file.
    :type year: int
    :param replace: Whether to replace an existing file with the same name. Defaults to True.
    :type replace: bool, optional
    :return: None, as this function does not return anything.

    """
    file_name = f"{year}.rar"
    file_url = f"{defaults.online_dir}/original_files/{file_name}"
    defaults.archive_files.mkdir(parents=True, exist_ok=True)
    local_path = defaults.archive_files.joinpath(file_name)
    if (not Path(local_path).exists()) or replace:
        utils.download_file(url=file_url, path=local_path, show_progress_bar=True)


def unpack_archive_with_7zip(compressed_file_path: str, output_directory: str) -> None:
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
        seven_zip_file_path = defaults.root_dir.joinpath("7-Zip", "7zz")
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


def unpack_yearly_data_archives(from_year=None, to_year=None) -> None:
    """
    Extracts all the yearly data archives from a starting year to an ending
    year.

    :param from_year: The starting year (inclusive) to extract the data from.
        If None, the default is the first year available.
    :type from_year: int or None

    :param to_year: The ending year (inclusive) to extract the data up to. If
        None, the default is the last year.
    :type to_year: int or None

    :returns: None

    This function extracts all the yearly data archives from the specified
    starting year (inclusive) to the specified ending year (exclusive). If
    no starting year is specified, it starts from the first available year.
    If no ending year is specified, it extracts up to the last year. The yearly
    data archives are extracted into newly created directories under the
    'original_files' directory. Each archive file is named '`year`.rar' where
    `year` is the year for which the archive was created. If any of the
    directories already exist, the function will overwrite any existing files
    with the same name. Any nested archive files within the extracted
    directories will also be extracted recursively.

    """
    from_year, to_year = utils.build_year_interval(from_year=from_year, to_year=to_year)
    for year in tqdm(range(from_year, to_year), desc="Unziping raw data", unit="file"):
        _unpack_yearly_data_archive(year)


def _unpack_yearly_data_archive(year):
    """
    Extracts data archive for the given year.
    """
    file_path = defaults.archive_files.joinpath(f"{year}.rar")
    year_directory = defaults.unpacked_data.joinpath(str(year))
    if year_directory.exists():
        shutil.rmtree(year_directory)
    year_directory.mkdir(parents=True)
    unpack_archive_with_7zip(file_path, year_directory)
    _unpack_archives_recursive(year_directory)
    _remove_created_directories(year_directory)


def _unpack_archives_recursive(directory):
    """
    Extract all archives with the extensions ".zip" and ".rar" found
    recursively in the given directory and its subdirectories using 7zip.

    :param directory: The directory path to search for archives.
    :type directory: str
    :return: None

    """
    while True:
        all_files = list(Path(directory).iterdir())
        archive_files = [file for file in all_files if file.suffix in (".zip", ".rar")]
        if len(archive_files) == 0:
            break
        for file in archive_files:
            unpack_archive_with_7zip(file, directory)
            Path(file).unlink()


def _remove_created_directories(directory: Path):
    for path in directory.iterdir():
        if path.is_dir():
            shutil.rmtree(path)


def extract_data_from_access_files(from_year=None, to_year=None) -> None:
    """
    docs

    """
    from_year, to_year = utils.build_year_interval(from_year=from_year, to_year=to_year)
    for year in range(from_year, to_year):
        extract_tables_from_access_file(year)


def extract_tables_from_access_file(year: int) -> None:
    """
    docs

    """
    table_list = get_access_table_list(year)
    for table_name in tqdm(
        table_list, desc=f"Extracting data from {year}", unit="table"
    ):
        extract_table(year, table_name)


def get_access_table_list(year: int) -> list:
    """
    docs

    """
    connection_string = _make_connection_string(year)
    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()
        access_tables = cursor.tables()
        table_list = []
        for table in access_tables:
            table_list.append(table.table_name)
    table_list = [table for table in table_list if table.find("MSys") == -1]
    return table_list


def extract_table(year, table_name):
    """
    docs

    """
    try:
        table = get_access_table(year, table_name)
    except (pyodbc.ProgrammingError, pyodbc.OperationalError):
        tqdm.write(f"table {table_name} from {year} failed to extract")
        return
    table_name = _change_1380_table_names(year, table_name)
    year_directory = defaults.extracted_data.joinpath(str(year))
    Path(year_directory).mkdir(parents=True, exist_ok=True)
    table.to_csv(year_directory.joinpath(f"{table_name}.csv"), index=False)


def _change_1380_table_names(year, table_name):
    if year == 1380:
        if table_name in metadata.Metadata.other["unusual_names_of_1380"]:
            table_name = metadata.Metadata.other["unusual_names_of_1380"][table_name]
    return table_name


def get_access_table(year: int, table_name: str) -> pd.DataFrame:
    """
    docs

    """
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
