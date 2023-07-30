"""For maintainer"""

import tomllib

import requests
from tqdm import tqdm
from typing import Iterable
import boto3

from .metadata import (
    defaults,
    OriginalTable as _OriginalTable,
)
from . import utils


def update_online_directory() -> None:
    file_stats = _scan_local_directory()
    to_up_files = [file for file, stat in file_stats.items() if stat != "UpToDate"]
    pbar = tqdm(total=len(to_up_files), desc="Preparing ...", unit="Table")
    for file in to_up_files:
        pbar.update()
        pbar.desc = f"Checking file: {file}"
        _publish_parquet_file(file)
    pbar.close()


def _scan_local_directory():
    file_stats = {}
    number_of_files = len(list(defaults.processed_data.glob("*")))
    pbar = tqdm(total=number_of_files, desc="Preparing ...", unit="Table")
    for path in defaults.processed_data.iterdir():
        pbar.update()
        pbar.desc = f"Checking file: {path.name}"
        online_file_size = _get_file_size_online_directory(path.name)
        local_size = path.stat().st_size

        if online_file_size is None:
            file_stats[path.name] = "Missing"
        elif abs(online_file_size - local_size) < 5:
            file_stats[path.name] = "UpToDate"
        else:
            file_stats[path.name] = "OutDated"
    pbar.close()
    return file_stats


def _get_file_size_online_directory(file_name):
    url = f"{defaults.online_dir}/parquet_files/{file_name}"
    response = requests.head(url, timeout=10)
    try:
        return int(response.headers["Content-Length"])
    except KeyError:
        return None


def publish_data(
    table_name: _OriginalTable | list[_OriginalTable] | None = None,
    years: int | Iterable[int] | str | None = None,
) -> None:
    table_year = utils.create_table_year_product(table_name, years)
    pbar = tqdm(total=len(table_year), desc="Preparing ...", unit="Table")
    for _table_name, year in table_year:
        pbar.update()
        pbar.desc = f"Uploading table: {_table_name}, for year: {year}"
        file_name = f"{year}_{_table_name}.parquet"
        _publish_parquet_file(file_name)
    pbar.close()


def _publish_parquet_file(file_name: str) -> None:
    path = defaults.processed_data.joinpath(file_name)
    url = f"HBSIR/parquet_files/{file_name}"
    _upload_file_to_online_directory(path, url)


def _upload_file_to_online_directory(file_path, file_name):
    bucket = _get_bucket()

    with open(file_path, "rb") as file:
        bucket.put_object(ACL="public-read", Body=file, Key=file_name)


def _get_bucket(bucket_name="sdac"):
    with open("tokens.toml", "rb") as f:
        token = tomllib.load(f)["arvan"]
    s3_resource = boto3.resource(
        "s3",
        endpoint_url="https://s3.ir-tbz-sh1.arvanstorage.ir",
        aws_access_key_id=token["access_key"],
        aws_secret_access_key=token["secret_key"],
    )
    bucket = s3_resource.Bucket(bucket_name)  # type: ignore
    return bucket
