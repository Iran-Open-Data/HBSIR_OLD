"""
Utility functions
"""

import subprocess
from pathlib import Path
import platform
from zipfile import ZipFile

from tqdm import tqdm
import requests

from . import metadata


defaults = metadata.Defaults()
metadata_obj = metadata.Metadata()
_Tables = metadata.Tables


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
    elif (from_year is not None) and (to_year is None):
        _from_year, _to_year = from_year, from_year + 1
    elif (from_year is None) and (to_year is not None):
        raise KeyError
    else:
        _from_year, _to_year = earliest_year, latest_year + 1

    return _from_year, _to_year


def build_year_interval_for_table(
    table_name: _Tables, from_year: int | None = None, to_year: int | None = None
) -> tuple[int, int]:
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
    availability_interval = metadata_obj.tables["yearly_table_availability"][table_name]
    if isinstance(availability_interval, int):
        earliest_year = availability_interval
        latest_year = None
    elif isinstance(availability_interval, list):
        earliest_year, latest_year = availability_interval
    else:
        raise TypeError
    from_year, to_year = build_year_interval(
        from_year, to_year, earliest_year, latest_year
    )
    return from_year, to_year


def create_table_year_product(
    table_name: _Tables | list[_Tables] | tuple[_Tables],
    from_year: int | None = None,
    to_year: int | None = None,
) -> list[tuple[_Tables, int]]:
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
    if isinstance(table_name, str):
        table_list: list[_Tables] = [table_name]
    else:
        table_list = [table for table in table_name]

    product_list = []
    for _table_name in table_list:
        _from_year, _to_year = build_year_interval_for_table(
            _table_name, from_year, to_year
        )
        product_list.extend(
            [(_table_name, year) for year in range(_from_year, _to_year)]
        )
    return product_list


def is_multi_year(
    table_name: _Tables | list[_Tables] | tuple[_Tables],
    from_year: int | None = None,
    to_year: int | None = None,
) -> bool:
    if isinstance(table_name, str):
        table_list: list[_Tables] = [table_name]
    else:
        table_list = [table for table in table_name]

    for _table_name in table_list:
        _from_year, _to_year = build_year_interval_for_table(
            _table_name, from_year, to_year
        )
        if _to_year - _from_year > 1:
            return True
    return False
