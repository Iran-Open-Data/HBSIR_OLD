"""For maintainer"""

from pathlib import Path
import tomllib
from typing import Iterable

import requests
from tqdm import tqdm
import boto3

from .metadata_reader import (
    defaults,
    _OriginalTable,
    _Years,
)
from . import utils


def update_online_files() -> None:
    for directories in [
        (defaults.processed_data, "parquet_files"),
        (defaults.external_data, "external_data"),
    ]:
        update_online_directory(*directories)


def update_online_directory(local_directory: Path, online_directory: str) -> None:
    file_stats = _scan_directory(local_directory, online_directory)
    to_up_files = [file for file, stat in file_stats.items() if stat != "UpToDate"]
    pbar = tqdm(total=len(to_up_files), desc="Preparing ...", unit="Table")
    for file in to_up_files:
        pbar.update()
        pbar.desc = f"Checking file: {file}"
        _publish_parquet_file(local_directory.joinpath(file), online_directory)
    pbar.close()


def _scan_directory(local_directory: Path, online_directory: str):
    file_stats = {}
    number_of_files = len(
        [file for file in local_directory.glob("*") if file.is_file()]
    )
    pbar = tqdm(total=number_of_files, desc="Preparing ...", unit="Table")
    for path in local_directory.iterdir():
        pbar.update()
        pbar.desc = f"Checking file: {path.name}"
        if path.is_dir():
            continue
        online_file_size = _get_file_size_online_directory(path.name, online_directory)
        local_size = path.stat().st_size

        if online_file_size is None:
            file_stats[path.name] = "Missing"
        elif abs(online_file_size - local_size) < 5:
            file_stats[path.name] = "UpToDate"
        else:
            file_stats[path.name] = "OutDated"
    pbar.close()
    return file_stats


def _get_file_size_online_directory(file_name: str, directory):
    url = f"{defaults.online_dir}/{directory}/{file_name}"
    response = requests.head(url, timeout=10)
    try:
        return int(response.headers["Content-Length"])
    except KeyError:
        return None


def publish_processed_table(
    table_name: _OriginalTable | Iterable[_OriginalTable] | None = None,
    years: _Years = "last",
    online_directory: str = "parquet_files",
) -> None:
    assert isinstance(table_name, str)
    table_year = utils.construct_table_year_pairs(table_name, years)
    pbar = tqdm(total=len(table_year), desc="Preparing ...", unit="Table")
    for _table_name, year in table_year:
        pbar.update()
        pbar.desc = f"Uploading table: {_table_name}, for year: {year}"
        file_name = f"{year}_{_table_name}.parquet"
        file_path = defaults.processed_data.joinpath(file_name)
        _publish_parquet_file(file_path, online_directory)
    pbar.close()


def _publish_parquet_file(file_path: Path, directory: str) -> None:
    url = f"HBSIR/{directory}/{file_path.name}"
    _upload_file_to_online_directory(file_path, url)


def _upload_file_to_online_directory(file_path, file_name):
    bucket = _get_bucket()

    with open(file_path, "rb") as file:
        bucket.put_object(ACL="public-read", Body=file, Key=file_name)


def _get_bucket(bucket_name="sdac"):
    with open("tokens.toml", "rb") as file:
        token = tomllib.load(file)["arvan"]
    s3_resource = boto3.resource(
        "s3",
        endpoint_url="https://s3.ir-tbz-sh1.arvanstorage.ir",
        aws_access_key_id=token["access_key"],
        aws_secret_access_key=token["secret_key"],
    )
    bucket = s3_resource.Bucket(bucket_name)  # type: ignore
    return bucket
