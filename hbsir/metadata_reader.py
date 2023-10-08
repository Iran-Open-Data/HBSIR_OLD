"""
Metadata module
"""
import functools
import re

from pathlib import Path
from typing import Any, Literal, Callable, get_args

from pydantic import BaseModel
import yaml


PACKAGE_DIRECTORY = Path(__file__).parent
ROOT_DIRECTORT = Path().absolute()


Attribute = Literal["Urban_Rural", "Province", "Region"]

GeneralTable = Literal[
    "household_information",
    "census_month",
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


general_tables: tuple[GeneralTable] = get_args(GeneralTable)  # type: ignore
expenditure_tables: tuple[ExpenditureTable] = get_args(ExpenditureTable)  # type: ignore
original_tables: tuple[OriginalTable] = get_args(OriginalTable)  # type: ignore
standard_tables: tuple[StandardTable] = get_args(StandardTable)  # type: ignore


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
    maps: Path = local_dir.joinpath(settings[("maps",)])
    cached_data: Path = local_dir.joinpath(settings[("cached_data",)])

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
        "external_data",
    ]
    instruction: dict[str, Any]
    tables: dict[str, Any]
    maps: dict[str, Any]
    household: dict[str, Any]
    commodities: dict[str, Any]
    schema: dict[str, Any]
    other: dict[str, Any]
    external_data: dict[str, Any]

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
        interpreter = self.get_interpreter(file_name)
        _metadata = open_yaml(package_metadata_path, interpreter=interpreter)
        interpreter = self.get_interpreter(file_name, _metadata)
        if local_metadata_path.exists():
            local_metadata = open_yaml(local_metadata_path, interpreter=interpreter)
            _metadata.update(local_metadata)
        setattr(self, file_name, _metadata)

    def get_interpreter(
        self, file_name: str, context: dict | None = None
    ) -> Callable[[str], str] | None:
        context = context or {}
        if f"{file_name}_interpreter" in dir(self):
            interpreter = getattr(self, f"{file_name}_interpreter")
            interpreter = functools.partial(interpreter, context=context)
        else:
            interpreter = None
        return interpreter

    @staticmethod
    def commodities_interpreter(yaml_text: str, context: dict) -> str:
        context.update(yaml.safe_load(re.sub("{{.*}}", "", yaml_text)))
        placeholders_list: list[str] = re.findall(r"{{\s*(.*)\s*}}", yaml_text)
        mapping = {}
        for placeholder in placeholders_list:
            parts = placeholder.split(".")
            if len(parts) == 1:
                mapping[placeholder] = context[parts[0]]["items"]
            elif len(parts) == 2:
                mapping[placeholder] = context[parts[0]]["items"][parts[1]]
            else:
                raise ValueError
        for placeholder, value in mapping.items():
            yaml_text = yaml_text.replace("{{" + placeholder + "}}", str(value))
        return yaml_text


class LoadTable(BaseModel):
    dataset: Literal["processed", "cleaned", "original"] = settings[
        ("functions_defaults", "load_table", "dataset")
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
