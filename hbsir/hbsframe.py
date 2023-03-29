"""
DataFrame extention
"""
import pandas as pd

from .data_engine import add_attribute, add_classification
from .metadata import (
    Attributes as _Attributes,
)

@pd.api.extensions.register_dataframe_accessor("hbs")
class HBSDF:
    """Household Budget Survay pandas API"""

    def __init__(self, data_table):
        self.table: pd.DataFrame = data_table
        self.year: int | None = None
        self.id_column_name: str = "ID"
        self.year_column_name: str = "Year"
        self.code_column_name: str = "Code"

    def add_attribute(self, attribute: _Attributes | list[_Attributes]) -> pd.DataFrame:
        """
        Add Household attributes to the DataFrame
        """
        return add_attribute(
            self.table,
            attribute=attribute,
            attribute_text="names",
            year=self.year,
            id_column_name=self.id_column_name,
            year_column_name=self.year_column_name,
        )

    def add_classification(
        self,
        classification: str = "original",
        level: int | list[int] | None = None,
        new_column_name: str | list[str] | None = None,
        ) -> pd.DataFrame:
        """
        Add Commodities Classification to the DataFrame
        """
        return add_classification(
            self.table,
            classification=classification,
            level=level,
            new_column_name=new_column_name,
            code_column_name=self.code_column_name,
            year_column_name=self.year_column_name,
        )
