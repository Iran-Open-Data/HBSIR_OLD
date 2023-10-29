import subprocess
from pathlib import Path
import platform

from ..core.metadata_reader import defaults
from .download_utils import download_7zip


def extract(compressed_file_path: str | Path, output_directory: str | Path) -> None:
    """
    Extracts the contents of a compressed file using the 7-Zip tool.

    :param compressed_file_path: The path to the compressed file to be extracted.
    :type compressed_file_path: str
    :param output_directory: The path to the directory where the extracted files will be stored.
    :type output_directory: str
    :return: None, as this function does not return anything.

    """
    if platform.system() == "Windows":
        _windows_extract(compressed_file_path, output_directory)
    elif platform.system() == "Linux":
        _linux_extract(compressed_file_path, output_directory)
    else:
        print("Your OS is not supported. try buy a new one.")


def _windows_extract(compressed_file_path: str | Path, output_directory: str | Path):
    seven_zip_file_path = defaults.package_dir.joinpath("7-Zip", "7z.exe")
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


def _linux_extract(compressed_file_path: str | Path, output_directory: str | Path):
    seven_zip_file_path = defaults.package_dir.joinpath("7-Zip", "7zz")
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
