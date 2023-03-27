"""
Metadata module
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml


PACKAGE_DIRECTORY = Path(__file__).parent
ROOT_DIRECTORT = Path().absolute()


Attributes = Literal["Urban-Rural", "Province", "Region"]

GeneralTables = Literal[
    "household_information",
    "members_properties",
    "house_specifications",
]

ExpenditureTables = Literal[
    "food",
    "tobacco",
    "cloth",
    "home",
    "furniture",
    "medical",
    "transportation",
    "communication",
    "entertainment",
    "education",
    "hotel",
    "other",
    "durable",
    "investment",
]

IncomeTables = Literal[
    "employment_income",
    "self_employed_income",
    "other_income",
    "subsidy",
    "public_employment_income",
    "private_employment_income",
]

Tables = Literal[GeneralTables, ExpenditureTables, IncomeTables]


def open_yaml(path):
    """
    Read the contents of a YAML file relative to the root directory and return it as a dictionary.

    :param path: The path to the YAML file, relative to the root directory.
    :type path: str

    :return: The contents of the YAML file as a dictionary.
    :rtype: dict

    :raises yaml.YAMLError: If there is an error parsing the YAML file.

    """
    path = PACKAGE_DIRECTORY.joinpath(path)
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
    `hbsir-settings.yaml` file in the root directory. If the file is not found,
    it loads the settings from the sample file named `settings-sample.yaml`
    located in config folder in library directory.

    """

    try:
        settings = open_yaml(ROOT_DIRECTORT.joinpath("hbsir-settings.yaml"))
    except FileNotFoundError:
        settings = open_yaml(PACKAGE_DIRECTORY.joinpath("config", "settings-sample.yaml"))

    # online directory
    online_dir = settings["online_directory"]

    # local directory
    pack_dir: Path = PACKAGE_DIRECTORY
    root_dir: Path = ROOT_DIRECTORT
    if Path(settings["local_directory"]).is_absolute():
        local_dir = Path(settings["local_directory"])
    elif settings["in_root"]:
        local_dir: Path = root_dir.joinpath(settings["local_directory"])
    else:
        local_dir: Path = pack_dir.joinpath(settings["local_directory"])
    archive_files: Path = local_dir.joinpath(settings["archive_files"])
    unpacked_data: Path = local_dir.joinpath(settings["unpacked_data"])
    extracted_data: Path = local_dir.joinpath(settings["extracted_data"])
    processed_data: Path = local_dir.joinpath(settings["processed_data"])

    first_year: int = settings["first_year"]
    last_year: int = settings["last_year"]


def get_latest_version_year(metadata_dict: dict, year: int) -> int | bool:
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
        return False
    if "versions" in metadata_dict:
        return True
    version_list = list(metadata_dict.keys())
    for element in version_list:
        if not isinstance(element, int):
            return False
        if (element < 1300) or (element > 1500):
            return False

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
    if selected_version is True:
        selected_version = get_latest_version_year(metadata_dict["versions"], year)
        metadata_version = {key: value for key, value in metadata_dict.items() if key != "versions"}
        for key, value in metadata_dict["versions"][selected_version].items():
            metadata_version[key] = value

    elif selected_version is False:
        metadata_version =  metadata_dict

    else:
        metadata_version = metadata_dict[selected_version]

    return metadata_version


def get_categories(metadata_dict: dict) -> list:
    """_summary_

    Parameters
    ----------
    metadata_dict : dict
        _description_

    Returns
    -------
    _type_
        _description_
    """
    if "categories" not in metadata_dict:
        categories_list = [metadata_dict]
    else:
        categories_number = list(metadata_dict["categories"].keys())
        categories_number.sort()
        categories_list = [metadata_dict["categories"][number] for number in categories_number]
        shared_infos = [key for key in metadata_dict.keys() if key != "categories"]
        for category in categories_list:
            for info in shared_infos:
                if info not in category.keys():
                    category[info] = metadata_dict[info]
    return categories_list
