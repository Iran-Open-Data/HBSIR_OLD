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
    house_hold_id = open_yaml("metadata/house_hold_id.yaml")
    commodity_codes = open_yaml("metadata/commodity_codes.yaml")
    standard_tables = open_yaml("metadata/standard_tables.yaml")
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
