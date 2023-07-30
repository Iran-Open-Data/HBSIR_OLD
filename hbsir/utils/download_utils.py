
from pathlib import Path
import platform
from zipfile import ZipFile

from tqdm import tqdm
import requests

from ..metadata import defaults


def download(
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
    file_name, path = _get_name_and_path(url=url, path=path)
    response = requests.get(url, timeout=10, stream=True)
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
        for chunk in response.iter_content(chunk_size=4096):
            download_bar.update(len(chunk))
            file.write(chunk)
    download_bar.close()
    return path


def _get_name_and_path(url: str, path: str | Path | None) -> tuple[str, Path]:
    if path is None:
        temp_folder = defaults.pack_dir.joinpath("temp")
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
        zip_file.extractall(defaults.pack_dir)
    file_path.unlink()

    if platform.system() == "Linux":
        defaults.pack_dir.joinpath("7-Zip", "7zz").chmod(0o771)
