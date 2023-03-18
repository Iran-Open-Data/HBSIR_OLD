"""
docs
"""

from dataclasses import dataclass
import pathlib

import yaml


ROOT_DIRECTORY = pathlib.Path(__file__).parents[1]


def open_yaml(path):
    """
    Read the contents of a YAML file relative to the root directory and return it as a dictionary.

    :param path: The path to the YAML file, relative to the root directory.
    :type path: str

    :return: The contents of the YAML file as a dictionary.
    :rtype: dict

    :raises yaml.YAMLError: If there is an error parsing the YAML file.

    """
    path = ROOT_DIRECTORY.joinpath(path)
    with open(path, mode="r", encoding="utf8") as yaml_file:
        yaml_content = yaml.load(yaml_file, Loader=yaml.CLoader)
    return yaml_content


@dataclass
class Metadata:
    """
    A dataclass for accessing metadata used in other parts of the project.

    """

    tables = open_yaml("metadata/tables.yaml")
    maps = open_yaml("metadata/maps.yaml")
    household = open_yaml("metadata/household.yaml")
    commodities = open_yaml("metadata/commodities.yaml")
    schema = open_yaml("metadata/schema.yaml")
    other = open_yaml("metadata/other.yaml")


@dataclass
class Defaults:
    """
    This dataclass provides access to default values that are used in other
    parts of the project. It first attempts to load the settings from a local
    `settings.yaml` file. If the file is not found, it loads the settings from
    the sample file named `settings-sample.yaml`.

    """

    try:
        settings = open_yaml("settings.yaml")
    except FileNotFoundError:
        settings = open_yaml("settings-sample.yaml")

    # online directory
    online_dir = settings["online_directory"]

    # local directories
    root_dir = ROOT_DIRECTORY
    local_dir = root_dir.joinpath(settings["local_directory"])
    archive_files = local_dir.joinpath(settings["archive_files"])
    unpacked_data = local_dir.joinpath(settings["unpacked_data"])
    extracted_data = local_dir.joinpath(settings["extracted_data"])
    processed_data = local_dir.joinpath(settings["processed_data"])

    first_year = settings["first_year"]
    last_year = settings["last_year"]


def get_latest_version_year(metadata_dict: dict, year: int) -> int | None:
    """
    Retrieve the most recent available version of metadata that matches or
    precedes the given year, provided that the metadata is versioned.

    :param metadata_dict: A dictionary representing the metadata.
    :type metadata_dict: dict

    :param year: The year to which the version of the metadata should match or
        precede.
    :type year: int

    :return: The version number of the most recent metadata version that
        matches or precedes the given year, or None if the metadata is not
        versioned.
    :rtype: int or None

    """
    if not isinstance(metadata_dict, dict):
        return None
    version_list = list(metadata_dict.keys())
    for element in version_list:
        if not isinstance(element, int):
            return None
        if (element < 1300) or (element > 1500):
            return None

    selected_version = 0
    for version in version_list:
        if version <= year:
            selected_version = max(selected_version, version)
    return selected_version


def get_metadata_version(metadata_dict: dict, year: int) -> dict:
    """
    Retrieve the metadata version that matches or precedes the given year,
    returning the complete metadata for that version.

    :param metadata_dict: A dictionary representing the metadata.
    :type metadata_dict: dict

    :param year: The year to which the version of the metadata should match or
        precede.
    :type year: int

    :return: A dictionary containing the complete metadata for the version that
        matches or precedes the given year. If the metadata is not versioned, the
        function returns the original metadata dictionary.
    :rtype: dict

    """
    selected_version = get_latest_version_year(metadata_dict, year)

    if selected_version is None:
        return metadata_dict
    return metadata_dict[selected_version]
