from typing import Callable
import importlib

import pandas as pd

from .. import utils
from ..metadata_reader import defaults, metadata


class ExternalDataCleaner:
    def __init__(self, name: str) -> None:
        self.name = name
        raw_folder_path = defaults.external_data.joinpath("_raw")
        raw_folder_path.mkdir(exist_ok=True, parents=True)

        self.metadata = metadata.external_data.get(self.name, None)
        extension = self._find_extension()
        if extension is not None:
            self.raw_file_path = raw_folder_path.joinpath(f"{self.name}.{extension}")
        else:
            self.raw_file_path = None

    def load_data(self):
        external_data = [file.stem for file in defaults.external_data.iterdir()]
        if self.name in external_data:
            return pd.read_parquet(
                defaults.external_data.joinpath(f"{self.name}.parquet")
            )
        if self.metadata is None:
            raise ValueError("Metadata is not available for this table")
        if "url" in self.metadata:
            table = self.clean_raw_file()
        elif "from" in self.metadata:
            table = self.collect_and_clean()
        else:
            raise ValueError
        self.save_data(table)
        return table

    def _find_extension(self) -> str | None:
        available_extentions = ["xlsx"]
        extension = self.metadata.get("extension", None)
        url = self.metadata.get("url", None)

        if (extension is None) and (url is not None):
            extension = url.rsplit(".", maxsplit=1)[1]

        if extension is not None:
            assert extension in available_extentions
        return extension

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
        table = self.cleaning_function(table)
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
        return getattr(cleaning_module, self.metadata["cleaning_function"])

    def save_data(self, table: pd.DataFrame) -> None:
        table.to_parquet(defaults.external_data.joinpath(f"{self.name}.parquet"))
