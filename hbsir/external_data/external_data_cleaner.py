from typing import Callable, Literal
import importlib
from pathlib import Path

import pandas as pd

from .. import utils
from ..core.metadata_reader import defaults, metadata


class ExternalDataCleaner:
    def __init__(
        self, name: str, download_cleaned: bool = False, save_cleaned: bool = False
    ) -> None:
        self.name = name
        self.download_cleaned = download_cleaned
        self.metadata = self._get_metadata()
        self.metadata_type = self._extract_type()
        self.save_cleaned = save_cleaned

    def load_data(self):
        external_data = [file.stem for file in defaults.external_data.iterdir()]
        if self.name in external_data:
            table = self.open_cleaned_data()
        elif self.metadata_type == "alias":
            name = self.metadata["alias"]
            if name.count(".") == 0:
                name = f"{self.name}.{name}"
            table = ExternalDataCleaner(
                name=name,
                download_cleaned=self.download_cleaned,
                save_cleaned=self.save_cleaned,
            ).load_data()
        elif self.download_cleaned or (self.metadata_type == "manual"):
            table = self.read_from_online_directory()
        elif self.metadata_type == "url":
            table = self.clean_raw_file()
        elif self.metadata_type == "from":
            table = self.collect_and_clean()
        else:
            raise ValueError
        if (self.save_cleaned) and (self.metadata_type != "alias"):
            self.save_data(table)
        return table

    def _get_metadata(self) -> dict:
        meta = metadata.external_data.copy()
        name_parts = self.name.split(".")
        while len(name_parts) > 0:
            part = name_parts.pop(0)
            meta = meta[part]
            if "goto" in meta:
                self.name = meta["goto"] + ".".join(name_parts)
                meta = self._get_metadata()
                break
        return meta

    def _extract_type(self) -> Literal["manual", "url", "from", "alias"]:
        for metadata_type in ("manual", "url", "from", "alias"):
            if (metadata_type in self.metadata) or (self.metadata == metadata_type):
                return metadata_type
        raise ValueError("Metadata type is missing")

    def _find_extension(self) -> str:
        available_extentions = ["xlsx"]
        extension = self.metadata.get("extension", None)
        url = self.metadata.get("url", None)

        if (extension is None) and (url is not None):
            extension = url.rsplit(".", maxsplit=1)[1]

        assert extension in available_extentions
        return extension

    def open_cleaned_data(self) -> pd.DataFrame:
        return pd.read_parquet(defaults.external_data.joinpath(f"{self.name}.parquet"))

    @property
    def raw_file_path(self) -> Path:
        raw_folder_path = defaults.external_data.joinpath("_raw")
        raw_folder_path.mkdir(exist_ok=True, parents=True)
        extension = self._find_extension()
        return raw_folder_path.joinpath(f"{self.name}.{extension}")

    def download_raw_file(self) -> None:
        url = self.metadata["url"]
        utils.download(url, self.raw_file_path)

    def load_raw_file(self) -> pd.DataFrame:
        assert self.raw_file_path is not None
        if not self.raw_file_path.exists():
            self.download_raw_file()
        if self.raw_file_path.suffix in [".xlsx"]:
            table = pd.read_excel(self.raw_file_path, header=None)
        else:
            raise ValueError("Format not supported yet")
        return table

    def clean_raw_file(self, table: pd.DataFrame | None = None) -> pd.DataFrame:
        if table is None:
            table = self.load_raw_file()
        try:
            table = self.cleaning_function(table)
        except AttributeError:
            pass
        return table

    def collect_and_clean(self) -> pd.DataFrame:
        table_list = [
            ExternalDataCleaner(table).load_data() for table in self.metadata["from"]
        ]
        table = self.cleaning_function(table_list)
        return table

    @property
    def cleaning_function(
        self,
    ) -> Callable[[pd.DataFrame | list[pd.DataFrame]], pd.DataFrame]:
        cleaning_module = importlib.import_module(
            "hbsir.external_data.cleaning_scripts"
        )
        return getattr(cleaning_module, self.name.replace(".", "_"))

    def save_data(self, table: pd.DataFrame) -> None:
        table.to_parquet(defaults.external_data.joinpath(f"{self.name}.parquet"))

    def read_from_online_directory(self) -> pd.DataFrame:
        url = f"{defaults.online_dir}/external_data/{self.name}.parquet"
        return pd.read_parquet(url)
