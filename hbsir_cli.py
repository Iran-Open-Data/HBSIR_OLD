"""
docstring
"""

import click

from hbsir.archive_handler import (
    download_year_files_in_range,
    unpack_yearly_data_archives,
    extract_data_from_access_files,
)


@click.group()
def cli():
    """
    Group Function of Command Line Interface
    """


@click.command()
@click.option(
    "--fromyear",
    type=int,
    help="An integer representing the starting year of the range.",
)
@click.option(
    "--toyear",
    type=int,
)
@click.option("--replace", type=bool)
def download(fromyear=None, toyear=None, replace=False):
    """
    Download Data Archive Files
    """
    download_year_files_in_range(fromyear, toyear, replace)


cli.add_command(download)


@click.command()
@click.option(
    "--fromyear",
    type=int,
    help="An integer representing the starting year of the range.",
)
@click.option(
    "--toyear",
    type=int,
)
def unpack(fromyear=None, toyear=None):
    """
    Unpack Data Archive Files
    """
    unpack_yearly_data_archives(fromyear, toyear)


cli.add_command(unpack)


@click.command()
@click.option(
    "--fromyear",
    type=int,
    help="An integer representing the starting year of the range.",
)
@click.option(
    "--toyear",
    type=int,
)
def extract(fromyear=None, toyear=None):
    """
    Extract Data from Archive Files
    """
    extract_data_from_access_files(fromyear, toyear)


cli.add_command(extract)


if __name__ == "__main__":
    cli()
