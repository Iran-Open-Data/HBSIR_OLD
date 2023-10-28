from pathlib import Path
import platform
from zipfile import ZipFile

from tqdm import tqdm
import requests

from ..metadata_reader import defaults, metadata


def download(
    url: str, path: str | Path | None = None, show_progress_bar: bool = False
) -> Path:
    """Downloads a file from a given URL and saves it to a specified local path.

    This function uses the requests library to send a GET request to the provided URL,
    and then writes the response content to a file at the specified path. If the path
    is not provided, the file is saved in a temporary directory. The function also
    provides an option to display a progress bar during the download.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    path : str, Path, optional
        The local path where the downloaded file should be saved. If None, the file
        is saved in a temporary directory. Default is None.
    show_progress_bar : bool, optional
        If True, a progress bar is displayed during the download. Default is False.

    Returns
    -------
    Path
        The local path where the downloaded file was saved.

    Raises
    ------
    FileNotFoundError
        If the file cannot be found at the given URL.
    """
    file_name, path = _get_name_and_path(url=url, path=path)
    response = requests.get(url, timeout=1000, stream=True)
    content_iterator = response.iter_content(chunk_size=4096)
    file_size = response.headers.get("content-length")
    if file_size is not None:
        file_size = int(file_size)
    else:
        raise FileNotFoundError("File is not found on the server")
    download_bar = tqdm(
        desc=f"downloading {file_name}",
        total=file_size,
        unit="B",
        unit_scale=True,
        disable=not show_progress_bar,
    )
    Path(path.parent).mkdir(parents=True, exist_ok=True)
    with open(path, mode="wb") as file:
        while True:
            try:
                chunk = next(content_iterator)
            except StopIteration:
                break
            except requests.Timeout:
                continue
            download_bar.update(len(chunk))
            file.write(chunk)
    download_bar.close()
    return path


def _get_name_and_path(url: str, path: str | Path | None) -> tuple[str, Path]:
    if path is None:
        temp_folder = defaults.package_dir.joinpath("temp")
        temp_folder.mkdir(exist_ok=True)
        file_name = url.split("/")[-1]
        path = temp_folder.joinpath(file_name)
    elif isinstance(path, str):
        path = Path(path)
        file_name = path.name
    elif isinstance(path, Path):
        file_name = path.name
    else:
        raise TypeError
    return file_name, path


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
    file_path = download(
        f"{defaults.online_dir}/7-Zip/{file_name}",
        show_progress_bar=True,
    )
    with ZipFile(file_path) as zip_file:
        zip_file.extractall(defaults.package_dir)
    file_path.unlink()

    if platform.system() == "Linux":
        defaults.package_dir.joinpath("7-Zip", "7zz").chmod(0o771)


def download_map(map_name: str, source: str = "original") -> None:
    url = metadata.maps[map_name][f"{source}_link"]
    file_path = download(url, show_progress_bar=True)
    path = defaults.maps.joinpath(map_name)
    path.mkdir(exist_ok=True, parents=True)
    with ZipFile(file_path) as zip_file:
        zip_file.extractall(path)
    file_path.unlink()
