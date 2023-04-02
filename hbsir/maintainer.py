"""For maintainer"""

import tomllib

from tqdm import tqdm
import boto3

from .metadata import (
    defaults,
    OriginalTable as _OriginalTable,
)
from . import utils


def upload_file_to_online_directory(file_path, file_name):
    bucket = _get_bucket()

    with open(file_path, "rb") as file:
        bucket.put_object(
            ACL='public-read',
            Body=file,
            Key=file_name
        )

def _get_bucket(bucket_name='sdac'):
    with open("tokens.toml", "rb") as f:
        token = tomllib.load(f)["arvan"]
    s3_resource = boto3.resource(
        's3',
        endpoint_url='https://s3.ir-tbz-sh1.arvanstorage.ir',
        aws_access_key_id=token["access_key"],
        aws_secret_access_key=token["secret_key"],
    )
    bucket = s3_resource.Bucket(bucket_name) # type: ignore
    return bucket


def publish_parquet(
    table_name: _OriginalTable | list[_OriginalTable] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
) -> None:
    table_year = utils.create_table_year_product(table_name, from_year, to_year)
    pbar = tqdm(total=len(table_year), desc="Preparing ...", unit="Table")
    for _table_name, year in table_year:
        pbar.update()
        pbar.desc = f"Uploading table: {_table_name}, for year: {year}"
        file_name = f"{year}_{_table_name}.parquet"
        path = defaults.processed_data.joinpath(file_name)
        url = f"parquet_files/{file_name}"
        upload_file_to_online_directory(path, url)
    pbar.close()
