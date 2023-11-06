"""HBSIR library utility functions"""
from typing import Any

from .seven_zip_utils import extract as sevenzip
from .download_utils import download, download_map, download_processed_data
from .parsing_utils import parse_years, construct_table_year_pairs
from .metadata_utils import (
    MetadataVersionResolver,
    MetadataCategoryResolver,
    MetadataResolverSettings,
)
from .argham import Argham


__all__ = [
    "sevenzip",
    "download",
    "download_map",
    "download_processed_data",
    "parse_years",
    "construct_table_year_pairs",
    "Argham",
    "resolve_metadata",
]


_Default = Any


# pylint: disable=too-many-arguments
# pylint: disable=unused-argument
def resolve_metadata(
    versioned_metadata: dict,
    year: int,
    categorize: bool = False,
    year_range: tuple[int, int] = _Default,
    year_keyword: str = _Default,
    version_keyword: str = _Default,
    items_keyword: str = _Default,
    category_keyword: str = _Default,
    item_key_name: str = _Default,
) -> ...:
    """Resolves metadata for the given year.

    Resolves the versioned metadata to the specified year. Optionally
    categorizes the resolved metadata if has_category is True.

    Allows overriding default settings via keyword arguments.

    Parameters
    ----------
    versioned_metadata : dict
        Raw metadata dictionary with embedded version info.

    year : int
        Year to resolve metadata for.

    categorize : bool, optional
        Whether to categorize resolved metadata.
        Default is False.

    Additional keywords:
        Used to override default versioning settings.

    Returns
    -------
    dict
        Resolved metadata dictionary for the specified year.
        Categorized if categorize is True.
    """
    setting_parameters = {
        key: value
        for key, value in locals().items()
        if (value is not _Default)
        and (value not in ["versioned_metadata", "year", "has_category"])
    }
    settings = MetadataResolverSettings(**setting_parameters)

    if categorize:
        resolver = MetadataCategoryResolver(versioned_metadata, year, settings)
        resolved_metadata = resolver.categorize_metadata()
    else:
        resolver = MetadataVersionResolver(versioned_metadata, year, settings)
        resolved_metadata = resolver.get_version()

    if isinstance(resolved_metadata, dict):
        resolved_metadata.update({"year": year})

    return resolved_metadata
