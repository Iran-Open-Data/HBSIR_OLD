"""
DataFrame extention
"""
import pandas as pd

from .data_engine import add_attribute, add_classification


@pd.api.extensions.register_dataframe_accessor("hbs")
class HBSDF:
    """Household Budget Survay pandas API"""

    def __init__(self, data_table):
        self.table = data_table
        self.id_column_name = "ID"
        self.year_column_name = "Year"

    def add_attribute(self, attribute, year):
        """
        Add Household attributes to the DataFrame
        """
        return add_attribute(
            self.table,
            attribute=attribute,
            attribute_text="name",
            year=year,
            id_column_name=self.id_column_name,
            year_column_name=self.year_column_name,
        )

    def add_classification(self):
        """
        Add Commodities Classification to the DataFrame
        """
        return add_classification(self.table)