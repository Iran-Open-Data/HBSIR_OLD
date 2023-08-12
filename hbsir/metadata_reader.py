"""
Metadata module
"""
import re

from pathlib import Path
from typing import Any, Literal, Callable, get_args

from pydantic import BaseModel
import yaml


PACKAGE_DIRECTORY = Path(__file__).parent
ROOT_DIRECTORT = Path().absolute()


Attribute = Literal["Urban-Rural", "Province", "Region"]

GeneralTable = Literal[
    "household_information",
    "members_properties",
    "house_specifications",
]

ExpenditureTable = Literal[
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

IncomeTable = Literal[
    "employment_income",
    "self_employed_income",
    "other_income",
    "subsidy",
    "public_employment_income",
    "private_employment_income",
]

OriginalTable = Literal[GeneralTable, ExpenditureTable, IncomeTable]

StandardTable = Literal[
    "Original_Expenditures",
    "Expenditures",
    "Imputed_Rent",
    "Incomes",
]

Table = Literal[OriginalTable, StandardTable]


general_tables: tuple[GeneralTable] = get_args(GeneralTable)
expenditure_tables: tuple[ExpenditureTable] = get_args(ExpenditureTable)
original_tables: tuple[OriginalTable] = get_args(OriginalTable)
standard_tables: tuple[StandardTable] = get_args(StandardTable)


def open_yaml(
    path: Path | str,
    location: Literal["package", "root"] = "package",
    interpreter: Callable[[str], str] | None = None,
):
    """
    Read the contents of a YAML file relative to the root directory and return it as a dictionary.

    :param path: The path to the YAML file, relative to the root directory.
    :type path: str

    :return: The contents of the YAML file as a dictionary.
    :rtype: dict

    :raises yaml.YAMLError: If there is an error parsing the YAML file.

    """
    path = Path(path) if isinstance(path, str) else path
    if path.is_absolute():
        pass
    elif location == "root":
        path = ROOT_DIRECTORT.joinpath(path)
    else:
        path = PACKAGE_DIRECTORY.joinpath(path)

    with open(path, mode="r", encoding="utf8") as yaml_file:
        yaml_text = yaml_file.read()
    if interpreter is not None:
        yaml_text = interpreter(yaml_text)
    yaml_content = yaml.safe_load(yaml_text)
    return yaml_content


def flatten_dict(dictionary: dict) -> dict[tuple[Any, ...], Any]:
    flattened_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, dict):
            flattend_value = flatten_dict(value)
            for sub_key, sub_value in flattend_value.items():
                flattened_dict[(key,) + sub_key] = sub_value
        else:
            flattened_dict[(key,)] = value
    return flattened_dict


def collect_settings() -> dict[tuple[Any, ...], Any]:
    sample_settings_path = PACKAGE_DIRECTORY.joinpath("config", "settings-sample.yaml")
    _settings = flatten_dict(open_yaml(sample_settings_path))

    package_settings_path = PACKAGE_DIRECTORY.joinpath(_settings[("package_settings",)])
    if package_settings_path.exists():
        package_settings = flatten_dict(open_yaml(package_settings_path))
        _update_settings(_settings, package_settings)

    root_setting_path = ROOT_DIRECTORT.joinpath(_settings[("local_settings",)])
    if root_setting_path.exists():
        root_settings = flatten_dict(open_yaml(root_setting_path))
        _update_settings(_settings, root_settings)

    return _settings


def _update_settings(_settings, new_settings):
    for key, value in new_settings.items():
        if key in _settings:
            _settings[key] = value


settings = collect_settings()


class Defaults(BaseModel):
    # online directory
    online_dir: str = settings[("online_directory",)]

    # local directory
    package_dir: Path = PACKAGE_DIRECTORY
    root_dir: Path = ROOT_DIRECTORT

    if Path(settings[("local_directory",)]).is_absolute():
        local_dir: Path = Path(settings[("local_directory",)])
    elif settings[("in_root",)]:
        local_dir: Path = root_dir.joinpath(settings[("local_directory",)])
    else:
        local_dir: Path = package_dir.joinpath(settings[("local_directory",)])  # type: ignore

    archive_files: Path = local_dir.joinpath(settings[("archive_files",)])
    unpacked_data: Path = local_dir.joinpath(settings[("unpacked_data",)])
    extracted_data: Path = local_dir.joinpath(settings[("extracted_data",)])
    processed_data: Path = local_dir.joinpath(settings[("processed_data",)])
    external_data: Path = local_dir.joinpath(settings[("external_data",)])

    first_year: int = settings[("first_year",)]
    last_year: int = settings[("last_year",)]


class Metadata:
    """
    A dataclass for accessing metadata used in other parts of the project.

    """

    metadata_files = [
        "instruction",
        "tables",
        "maps",
        "household",
        "commodities",
        "schema",
        "other",
    ]
    instruction: dict[str, Any]
    tables: dict[str, Any]
    maps: dict[str, Any]
    household: dict[str, Any]
    commodities: dict[str, Any]
    schema: dict[str, Any]
    other: dict[str, Any]

    def __init__(self) -> None:
        self.reload()

    def reload(self):
        for file_name in self.metadata_files:
            self.reload_file(file_name)

    def reload_file(self, file_name):
        package_metadata_path = settings[("package_metadata", file_name)]
        local_metadata_path = ROOT_DIRECTORT.joinpath(
            settings[("local_metadata", file_name)]
        )
        if f"{file_name}_interpreter" in dir(self):
            interpreter = getattr(self, f"{file_name}_interpreter")
        else:
            interpreter = None
        _metadata = open_yaml(package_metadata_path, interpreter=interpreter)
        if local_metadata_path.exists():
            local_metadata = open_yaml(local_metadata_path)
            _metadata.update(local_metadata)
        setattr(self, file_name, _metadata)

    @staticmethod
    def commodities_interpreter(yaml_text: str):
        yaml_body = yaml.safe_load(re.sub("{{.*}}", "", yaml_text))
        placeholders_list: list[str] = re.findall(r"{{\s*(.*)\s*}}", yaml_text)
        placeholders_mapping = {}
        for placeholder in placeholders_list:
            parts = placeholder.split(".")
            if len(parts) == 1:
                placeholders_mapping[placeholder] = yaml_body[parts[0]]
            elif len(parts) == 2:
                placeholders_mapping[placeholder] = yaml_body[parts[0]]["items"][parts[1]]
        for placeholder, value in placeholders_mapping.items():
            yaml_text = yaml_text.replace("{{" + placeholder + "}}", str(value))
        return yaml_text

class LoadTable(BaseModel):
    data_type: Literal["processed", "cleaned", "original"] = settings[
        ("functions_defaults", "load_table", "data_type")
    ]
    on_missing: Literal["error", "download", "create"] = settings[
        ("functions_defaults", "load_table", "on_missing")
    ]
    save_downloaded: bool = settings[
        ("functions_defaults", "load_table", "save_downloaded")
    ]
    redownload: bool = settings[("functions_defaults", "load_table", "recreate")]
    save_created: bool = settings[("functions_defaults", "load_table", "save_created")]
    recreate: bool = settings[("functions_defaults", "load_table", "recreate")]


metadata = Metadata()
defaults = Defaults()
