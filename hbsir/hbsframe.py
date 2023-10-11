"""
DataFrame extention
"""
from importlib import import_module

import pandas as pd

from . import decoder


@pd.api.extensions.register_dataframe_accessor("view")
class ViewAccessor:
    def __init__(self, pandas_obj: pd.DataFrame):
        self._validate(pandas_obj)
        self._obj = pandas_obj
        self._views = None

    @staticmethod
    def _validate(obj):
        pass

    @property
    def views(self) -> list[str] | None:
        return self._views

    @views.setter
    def views(self, value: list[str]):
        self._views = value

    def __getitem__(self, value: str) -> pd.DataFrame:
        if self._views is not None and value in self._views:
            settings = decoder.CommodityDecoderSettings(name=value)
            return decoder.CommodityDecoder(self._obj, settings).add_classification()
        raise KeyError(f"{value} is not a view of the current table")

